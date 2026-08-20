# 07 — Dispatcher, derived queues, backoff, quarantine

**Depends on:** 06 (writer), 01 (schema).

## Goal

`src/reroll_sync/dispatcher.py`: selects work from the derived queues, hands
it to a stage, interprets the outcome, and turns it into `WriteOp`s. This is
the only place retry policy, quarantine, and skip attribution live.

## Why this exists as its own module

The current code repeats the same broken pattern in three places
(`metadata_sync.py`, `metadata_parse.py`, `reroll_convert.py`): each has its
own `_mark_skipped`, `_record_error`, its own `conn.commit()`, and its own
`except OSError: continue`. Consequences:

- **No backoff.** A failing row stays pending and `ORDER BY rowid` puts it
  back at the head of the queue on the next run. A handful of permanently
  500-ing URLs pins the front of a 12M-row queue forever.
- **No attempt limit.** Nothing ever gives up, so nothing is ever visibly
  broken.
- **Silent failure.** `except OSError: continue` means a stage can make zero
  progress indefinitely while `stats` reports success.
- **No queue bound.** `_pending_wheels` materializes the entire result set
  into a Python list before doing any work — an OOM at 12M rows.

Centralizing fixes all four at once and means a new stage gets correct retry
semantics for free.

## Requirements

### Queue selection

```
claim(stage: Stage, limit: int) -> list[QueueItem]
```

- One indexed query per stage, using the partial/composite indexes from
  spec 01. `EXPLAIN QUERY PLAN` must show no `SCAN` — spec 01 already
  requires that test; keep it passing.
- **Keyset pagination.** Each stage keeps an in-memory cursor
  (`last_id`) and queries `... AND id > ? ORDER BY ... LIMIT ?`. Wrap to the
  start when a query returns fewer than `limit` rows. Never `OFFSET`.
- Runs inside `read_txn` from spec 06 with a label.
- Excludes items with a `work` row whose `next_attempt_at` is in the future
  or whose `quarantined_at` is set. The deferred set is small in steady
  state; load ready-to-retry ids from `ix_work_ready` and merge, or filter
  with a `NOT EXISTS` subquery — measure and pick, but the plan must stay
  index-driven.
- **In-process claim tracking.** Since one process owns all writes, leases
  do not need to be in the database: keep a `set[int]` of in-flight ids per
  stage and exclude them. Simpler and faster than DB leases. It must be
  cleared on restart, which it is by construction.

Queue definitions for Phase 1:

| Stage | Predicate | Order |
|---|---|---|
| `fetch` | `state = NEED_METADATA` | `lane, project, id` |
| `convert` | `state = NEED_CONVERT` | `lane, project, id` |

Ordering by `project` before `id` is deliberate — see spec 01. Do not
"simplify" it to `id`.

### Lanes

`lane = 0` is incremental, `lane = 1` is backfill. Within a stage, the
dispatcher pulls from lane 0 until it is empty, then lane 1. Because the rate
limiter already reserves separate budget per domain (spec 03), lanes only
need to order work *within* a stage, so strict priority is fine and simpler
than a ratio.

Newly ingested wheels get `lane = 0`. Reprocess campaigns set `lane = 1`.

### Outcome application

Every stage returns a member of a common outcome union. `ConvertOutcome`
from spec 05 is one instance of the pattern; the fetch stage (spec 09)
defines its own with the same three shapes:

| Outcome | Actions |
|---|---|
| `Ok` | Advance `state`, write stage payload, bump `change_seq`, **delete** any `work` row |
| `Skip` | `state = SKIPPED`, upsert `skips`, insert `errors`, delete `work` row |
| `Retry` | Upsert `work` with `attempts + 1` and a computed `next_attempt_at`; leave `state` untouched |
| `Retry` at max attempts | `state = QUARANTINED`, set `work.quarantined_at`, insert `errors` |

Two rules that must be tested explicitly:

- **`Ok` and `Skip` always delete the `work` row.** Otherwise a wheel that
  failed twice then succeeded leaves a stale attempt count that skews
  metrics and could later quarantine it spuriously.
- **`Retry` never changes `state`.** The derived queue is what makes the item
  eligible again; moving state would remove it from the queue permanently.

Each transition must be validated against `ALLOWED_TRANSITIONS` from spec 01
before being applied, raising on an illegal edge. This catches stage bugs at
the point of writing rather than in `fsck` hours later.

### Backoff

```
next_attempt_at = now + min(base * 2 ** (attempts - 1), cap) * jitter
```

- `base = 30s`, `cap = 6h`, jitter uniform in `[0.5, 1.5)`.
- `max_attempts = 8` by default → roughly 30s, 1m, 2m, 4m, 8m, 16m, 32m,
  1h4m before quarantine, ~2 hours total. Configurable.
- Jitter is required, not decorative: without it, a batch of items that
  failed together retries together forever, producing a thundering herd
  against whatever was already struggling.
- `RateLimited` outcomes are **not** counted toward `attempts`. Being
  throttled says nothing about the item. Instead call
  `limiter.penalize(...)` and put the item back for immediate retry. Getting
  this wrong would quarantine healthy wheels during a PyPI incident, so it
  needs its own test.

### Serialization of stage payloads

The dispatcher owns turning stage output into stored bytes, so compression
settings live in one place:

- `ConvertOk.records` → JSON (`model_dump(mode="json")` per record, matching
  the existing `reroll_convert.py` behaviour) → zstd → `repodata_zst`.
- `ConvertOk.resolutions` → JSON → zstd → `name_conv_zst`.
- `ConvertOk.requires_prerelease` → `wheel_repodata.requires_prerelease`.
- `ConvertOk.conda_name` → `wheels.conda_name`.

Use one shared zstd compression level constant (level 10 is a reasonable
default for write-once/read-often). A helper pair
`compress_json(obj) -> bytes` / `decompress_json(blob) -> Any` belongs here
and needs round-trip tests including for empty collections.

### Reprocess campaigns

```
reprocess(selector: Selector) -> int    # returns affected row count
```

A single `WriteOp` performing one transaction. Selectors needed in Phase 1:

- `--reroll-version-below X` → rows whose `wheel_repodata.reroll_version < X`
- `--state S`
- `--project P`
- `--skipped-only`

The op must, atomically:

1. `DELETE FROM wheel_repodata WHERE ...` (reclaims pages via incremental
   vacuum, unlike setting columns to NULL)
2. `DELETE FROM skips WHERE permanent = 0 AND reroll_version < X` for the
   version selector — **this is the step that makes a reroll upgrade
   actually retry the wheels the old version rejected.** Omitting it is the
   most likely implementation mistake; it needs a dedicated test.
3. `DELETE FROM work` for affected wheels, clearing quarantine.
4. `UPDATE wheels SET state = NEED_CONVERT, lane = 1, change_seq = ...`

`permanent = 1` skips are **never** cleared by a version selector. A wheel
with no PEP 658 sidecar must not be dragged back into the queue by a reroll
upgrade.

Bound the transaction: for a 12M-row campaign, chunk by id range across
several `WriteOp`s rather than one giant transaction that would balloon the
WAL. Report progress as `target_count` minus current queue depth.

### Metrics surface

Per stage, expose for spec 11: queue depth, in-flight count, oldest-pending
age, throughput EMA, outcome counts by kind, retry count, quarantine count.
Queue depth must come from an indexed `COUNT(*)` on `(state, lane)`, never a
scan.

## Tests to write first

**Selection**

- `claim` returns at most `limit` items.
- Items are ordered by `(lane, project, id)`.
- Lane 0 items are returned before lane 1 items even when lane 1 has lower
  ids.
- An item with `work.next_attempt_at` in the future is excluded.
- The same item becomes eligible once the injected clock passes
  `next_attempt_at`.
- A quarantined item is never returned.
- An in-flight item is not returned by a second `claim`.
- The keyset cursor advances and wraps: two successive `claim`s on a
  3-item queue with `limit=2` return items 1,2 then 3, then wrap to 1,2.
- `claim` on an empty queue returns `[]` and does not raise.
- `claim` never materializes more than `limit` rows — assert via a
  `Connection` row-counting wrapper on a table with far more matching rows.
- The query plan for each stage contains no `SCAN`.

**Outcome application**

- `Ok` advances state, writes the payload, bumps `change_seq`.
- `Ok` deletes a pre-existing `work` row with `attempts = 3`.
- `Skip` sets `SKIPPED`, writes a `skips` row with the right `permanent` and
  `reroll_version`, writes an `errors` row, deletes the `work` row.
- `Skip` with `permanent=True` writes `reroll_version = NULL`.
- `Retry` creates a `work` row with `attempts = 1` and leaves `state`
  unchanged.
- Successive `Retry`s increment `attempts` and push `next_attempt_at` out
  monotonically.
- `Retry` at `max_attempts` sets `QUARANTINED` and `work.quarantined_at`,
  and writes an `errors` row.
- An illegal transition (e.g. `READY` → `NEED_METADATA` via `Ok`) raises.
- `RateLimited` does **not** increment `attempts`, does call
  `limiter.penalize`, and leaves the item immediately eligible.

**Backoff**

- Delays follow `30 * 2**(n-1)` within the jitter band for n = 1..8.
- The delay is capped at 6 h.
- Jitter produces different delays for two items with equal `attempts`
  (seed the RNG for determinism).
- Jitter never produces a delay ≤ 0.

**Serialization**

- `compress_json` / `decompress_json` round trip a record list, an empty
  list, an empty dict, and non-ASCII strings.
- A stored `repodata_zst` decompresses to JSON matching the records the
  worker produced.
- Compression actually reduces size for a realistic record list (guards
  against accidentally storing raw).

**Reprocess**

- `--reroll-version-below` deletes matching `wheel_repodata` rows, sets
  `NEED_CONVERT` and `lane = 1`.
- It deletes `skips` rows with `permanent = 0` and an older
  `reroll_version`.
- It **preserves** `skips` rows with `permanent = 1`. Dedicated test.
- It **preserves** `skips` rows with a newer `reroll_version`.
- It clears `work` rows, including quarantined ones.
- It is chunked: a campaign over N rows with chunk size C produces
  `ceil(N/C)` write ops.
- The returned count matches rows affected.
- A selector matching nothing returns 0 and writes nothing.

**Metrics**

- Queue depth matches a hand-counted expectation.
- Oldest-pending age uses `updated_at` and is `None` on an empty queue.
- Outcome counters increment per kind.

## Acceptance criteria

- No `except OSError: continue`, or any equivalent silent-swallow, anywhere
  in `src/`.
- No stage module writes to sqlite or calls `commit()`.
- Every failure path either increments `attempts` with backoff or terminates
  with an attributed `skips` row — nothing is dropped on the floor.
- Every queue query is provably index-driven and bounded by `limit`.
- `make ci` green, coverage 100%.

## Deferred

- DB-backed leases. Unnecessary while one process owns writes; revisit only
  if a second writer process is ever introduced.
- Priority beyond two lanes.
- Automatic un-quarantine. Deliberate: quarantine means a human should look.
