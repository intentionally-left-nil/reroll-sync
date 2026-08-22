# 05 — Fused convert worker, with pre-release retry

**Depends on:** 01 (for `WheelState` and the outcome shapes it feeds).

This is the highest-value early task. The worker is a **pure function**: no
database, no network, no filesystem. It is also where the new pre-release
behaviour lives.

## Goal

Create `src/reroll_sync/convert.py` holding one function that turns raw
METADATA bytes plus a wheel filename into an outcome. Delete
`metadata_parse.py` and `reroll_convert.py` and fold both into it.

## Why fuse parse and convert

Today they are two stages with a database round trip and a stored
`wheel_metadata` column between them. That column costs ~36 GB for 12M
wheels and buys only the ability to re-run conversion without re-parsing —
but parsing is ~5 ms and the bytes are local in the segment store, so a
"re-convert" campaign can just re-parse too. Fusing removes a stage, a
derived queue, a write per wheel, and 36 GB.

`reroll.parse_metadata(text)` takes no `allow_pre` argument; only
`get_wheel_records(...)` does. So the pre-release retry re-runs conversion
only, never the parse.

## The pre-release requirement

Wheels whose version is a pre-release, or whose requirements pin
pre-releases, are currently rejected outright because `sync_reroll` passes
`allow_pre=False` unconditionally. New behaviour:

1. Attempt conversion with `allow_pre=False`.
2. If it fails with an error indicating the refusal was **because** of a
   pre-release, retry with `allow_pre=True`.
3. If the retry succeeds, keep the result **and** set
   `requires_prerelease = True` on the outcome.
4. If the retry fails, report the *retry's* error — but see the note below
   about which error to record.

### Which exceptions trigger the retry

From `reroll/errors.py`, two leaves mention `allow_pre`:

- `UnsupportedPrereleaseError` (a `RerollScopeError`) — "A pre-release wheel
  version, rejected because the caller has not opted in via `allow_pre`."
  **Unambiguous.** Always retry.
- `UnconvertableRequirementError` (a `RerollUnconvertableError`) — its
  docstring lists "a pre-release version without `allow_pre`" among six
  other unrelated causes (direct URL reference, local version label,
  over-long extra name, marker referring to `extra`, MatchSpec validation
  failure). **Ambiguous.** Retrying is still correct: for the six unrelated
  causes the retry fails identically, costing one extra ~5 ms conversion on
  a wheel that already failed.

Define the trigger set as a module-level constant:

```
PRERELEASE_RETRY_ERRORS = (UnsupportedPrereleaseError, UnconvertableRequirementError)
```

**Do not infer the trigger set from exception message text.** Match on type.

**This set must be pinned by fixture tests, not assumed.** Build fixtures
from real wheel filenames + METADATA that exercise both leaves, assert which
exception type reroll actually raises, and assert the retry outcome. If a
future reroll version changes the taxonomy, those tests are what catches it.
Per `AGENTS.md`, discovering reroll's actual behaviour belongs in a test, not
in a throwaway `python -c`.

### Which error to record when the retry also fails

Record the **first** attempt's error (the `allow_pre=False` one) as the
authoritative skip reason, and include the retry's exception type in the
error `details`. Rationale: the first error describes the wheel under the
policy the system actually wants. Recording only the retry's error would
make "why was this skipped" answer a question nobody asked.

## Requirements

### Signature

```
convert(
    metadata_bytes: bytes,
    filename: str,
    *,
    mappers: NameMappers,
    reroll_version: str,
    parse: Parse = reroll.parse_metadata,
    get_wheel_records: GetWheelRecords = reroll.stages.get_wheel_records,
) -> ConvertOutcome
```

- `mappers` is **required**, not defaulted. `default_mappers()` reloads
  config and network-backed lookup tables from scratch on every call; the
  existing `reroll_convert.py` docstring already flags this. Building it is
  the caller's job, once per worker process (see below).
- `parse` and `get_wheel_records` are injectable purely as test seams.
- The function **never raises** for a wheel-attributable failure. It returns
  an outcome. It may propagate only genuinely exceptional programming errors.

### Outcome type

```
ConvertOutcome  =  ConvertOk | ConvertSkip | ConvertRetry

ConvertOk(
    records:             tuple[WheelRecord, ...],
    resolutions:         tuple[NameResolution, ...],
    conda_name:          str,
    requires_prerelease: bool,
)

ConvertSkip(
    reason:              str,     # stable, structured; e.g. "invalid_metadata"
    subcategory:         str,     # exception type name
    details:             str,
    permanent:           bool,    # False for reroll-version-attributable
    reroll_version:      str | None,
)

ConvertRetry(reason: str, details: str)
```

Mapping from reroll's four error categories — this mirrors the existing
(correct) logic in `metadata_parse.py` and `reroll_convert.py`, so preserve
it:

| Input | Outcome |
|---|---|
| `UnicodeDecodeError` on the bytes | `ConvertSkip(permanent=True, reroll_version=None)` |
| `RerollRuntimeError` (any leaf) | `ConvertRetry` — says nothing about the wheel |
| `RerollScopeError`, `RerollInvalidWheelError`, `RerollUnconvertableError` | `ConvertSkip(permanent=False, reroll_version=...)` |
| Success | `ConvertOk` |

Undecodable bytes are `permanent=True` because no reroll upgrade can make
invalid UTF-8 decode. Every other skip is attributed to a reroll version so
an upgrade can clear it (`DELETE FROM skips WHERE permanent = 0 AND
reroll_version < ?`).

`RerollRuntimeError` mapping to `ConvertRetry` is important and easy to get
wrong: its docstring says it means reroll's *host environment* is unstable,
so the wheel must be left alone for a later attempt.

### `conda_name` on the outcome

`ConvertOk.conda_name` is the wheel's own conda package name — the shard key
Phase 2 needs, and the reason `wheels.conda_name` exists. Derive it from the
records rather than from the resolutions: the resolutions are the *dependency*
names that were mapped, which is a different thing. If the records disagree
on the package name, that is a bug — return `ConvertRetry` and alarm, do not
pick one arbitrarily. This needs a test.

### `resolutions` dedup

Port `_deduped_resolutions` from `reroll_convert.py` verbatim in behaviour:
one `NameResolution` per unique PyPI name across all records, sorted, because
the same dependency is resolved once per platform record. Its existing test
coverage is good.

### Serialization is not this module's job

`convert()` returns reroll objects. The dispatcher (spec 07) serializes to
JSON and zstd-compresses for `wheel_repodata.repodata_zst` /
`name_conv_zst`. Keeping serialization out of the worker keeps it pure and
keeps the compression level configurable in one place.

### Process-pool usage

The worker runs in a `ProcessPoolExecutor`. Provide:

```
worker_init(reroll_version: str) -> None
```

as the pool's `initializer`, which builds `default_mappers()` **once per
process** and stashes it in a module global, plus a thin
`convert_in_worker(metadata_bytes, filename) -> ConvertOutcome` that reads
that global. Rebuilding mappers per wheel would dominate runtime — the
existing code already avoids this per-run and the same reasoning applies
per-process.

`ConvertOutcome` must be picklable, so it must not carry an exception
instance. Carry type names and strings only. Test that every outcome variant
survives a `pickle.dumps`/`loads` round trip — this is the kind of thing that
only fails in production otherwise.

## Tests to write first

**Happy path**

- Valid METADATA + filename produces `ConvertOk` with non-empty records.
- `requires_prerelease` is `False` when the first attempt succeeds.
- `conda_name` matches the expected mapped name.
- `resolutions` are deduped and sorted; a dependency resolved by two
  platform records appears once.
- A wheel producing multiple records (one per subdir) returns all of them.

**Pre-release retry — the core of this task**

- First attempt raises `UnsupportedPrereleaseError`, retry succeeds →
  `ConvertOk` with `requires_prerelease=True`.
- First attempt raises `UnconvertableRequirementError`, retry succeeds →
  `ConvertOk` with `requires_prerelease=True`.
- First attempt raises `UnsupportedPrereleaseError`, retry **also** fails →
  `ConvertSkip` whose `subcategory` is the *first* error's type and whose
  `details` mention the retry's type.
- First attempt raises an error **not** in `PRERELEASE_RETRY_ERRORS` (e.g.
  `InvalidFilenameError`) → `ConvertSkip` with **exactly one** call to
  `get_wheel_records`. Assert the call count; a stray retry here would
  double CPU on every bad wheel.
- The first attempt is called with `allow_pre=False` and the retry with
  `allow_pre=True`. Assert the kwarg on both calls.
- `RerollRuntimeError` on the first attempt does **not** trigger a retry —
  it returns `ConvertRetry` immediately. Assert one call only.
- Fixture test against real reroll: a genuine pre-release wheel filename
  (e.g. `...-1.0.0rc1-...whl`) with real METADATA. Assert which exception
  type reroll raises with `allow_pre=False`, and that it is a member of
  `PRERELEASE_RETRY_ERRORS`. **This is the test that pins the trigger set.**
- Fixture test: a wheel whose *dependency* pins a pre-release, same
  assertions.

**Error mapping**

- Invalid UTF-8 bytes → `ConvertSkip(permanent=True, reroll_version=None)`.
- Each of `RerollScopeError`, `RerollInvalidWheelError`,
  `RerollUnconvertableError` → `ConvertSkip(permanent=False)` with
  `reroll_version` set and `subcategory` equal to the exception's type name.
- Each `RerollRuntimeError` leaf (`NetworkFetchError`, `DatabaseError`,
  `ConfigLoadError`, `UnexpectedError`) → `ConvertRetry`.
- A parse failure and a conversion failure of the same category produce
  distinguishable `reason` values, so `errors.error_category` remains useful.
- `convert()` raises nothing for any of the above.

**Edge cases**

- Empty `metadata_bytes`.
- METADATA with a UTF-8 BOM.
- METADATA with CRLF line endings.
- Records disagreeing on package name → `ConvertRetry`, not a silent pick.
- Zero records returned by `get_wheel_records` without an exception — decide
  and test: this should be `ConvertRetry` (reroll contract violation), not
  `ConvertOk` with nothing.

**Pickling**

- `ConvertOk`, `ConvertSkip`, and `ConvertRetry` each round trip through
  `pickle`.
- No outcome holds a reference to an exception instance or a traceback.

**Process pool**

- `worker_init` builds mappers exactly once for N `convert_in_worker` calls
  in the same process. Assert with an injected counter.

## Acceptance criteria

- `metadata_parse.py` and `reroll_convert.py` are deleted; nothing imports
  them. Their good behaviours (error categorization, resolution dedup,
  build-mappers-once) are preserved and still tested.
- `convert()` performs no I/O of any kind — no sqlite, no network, no
  filesystem. Verifiable by inspection and by the absence of any such import
  in the module.
- `PRERELEASE_RETRY_ERRORS` is pinned by at least two fixture tests against
  real reroll behaviour, not by assumption.
- Every `ConvertOutcome` variant is picklable.
- `make ci` green, coverage 100%.

## Deferred

- Persisting `WheelMetadata`. Deliberately dropped — see rationale above.
- Exposing `allow_pre` as a configuration knob. The two-attempt policy is
  the product behaviour; there is no reason to let an operator force
  `allow_pre=True` globally, and doing so would make
  `requires_prerelease` meaningless.

## Open question

`reroll.parse_metadata` may itself raise on a pre-release-related condition
in some future version. If a fixture test shows the pre-release rejection
happening at parse time rather than conversion time, the retry has to wrap
both calls. Write the fixture tests first and let them tell you; do not
pre-emptively wrap the parse.
