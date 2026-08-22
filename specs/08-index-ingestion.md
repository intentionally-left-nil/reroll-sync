# 08 — Index ingestion: polling, diffing, deletion, yanks

**Depends on:** 04 (client), 07 (dispatcher/writer ops), 01 (schema).

## Goal

Replace `sync.py` with `src/reroll_sync/ingest.py`: poll the PyPI simple
index, and for each stale project reconcile the local `wheels` rows against
the project page — inserting new files, updating changed ones, and
tombstoning ones that have disappeared.

## Why the current implementation must change

`sync.py` implements the algorithm in the old `docs/index_ingestion.md`
faithfully, and that algorithm is insert-only:

```sql
INSERT INTO wheels (...) VALUES (...) ON CONFLICT(filename) DO NOTHING
```

Three consequences, all now correctness bugs rather than optimizations:

1. **Yanks never propagate.** A file yanked after first ingestion keeps its
   original row forever. Yanked wheels must be excluded from published
   repodata, so a stale `yanked = 0` publishes a wheel that should not exist.
2. **Deleted files persist forever.** PyPI file and project deletion is real.
   A deleted wheel stays `READY` and stays in repodata indefinitely.
3. **Nothing else in the entry ever refreshes** — including
   `core-metadata` appearing on a file that previously had none, which is
   the only path out of `NO_METADATA`.

It also refetches the entire 25 MB index on every poll, and processes stale
projects strictly serially.

## Requirements

### Poll loop

```
poll_index(client, writer, *, etag: str | None) -> PollResult
```

1. Conditional GET of `/simple/` with the stored etag (spec 04).
2. On `304`, return immediately with `not_modified=True`. No further work.
3. On `200`, compare `meta._last-serial` against the stored global serial;
   if unchanged, still diff (a project can change without the global serial
   moving in edge cases) but log that the fast path was missed.
4. Read the local `name -> serial` map and compute stale projects: absent
   locally, or remote serial greater than local.
5. Persist the new etag and the global serial.

**The local serial map is ~650k rows and must not be a single unbounded
read.** Read it in keyset-paginated chunks inside `read_txn` (spec 06),
building the dict incrementally. At ~30 bytes/entry the dict is ~40 MB, which
is acceptable; a single 650k-row transaction is not.

Consider an alternative that avoids the full map: stream the remote project
list in chunks and probe locally per chunk with
`WHERE name IN (...)`. Either is acceptable; the chunked-read rule is not
negotiable.

### Project reconciliation

```
sync_project(client, name, *, now) -> ProjectSyncOutcome
```

For one project, produce the `WriteOp`s to make local state match the page.
Uses the same `Ok`/`Skip`/`Retry` outcome shape as other stages so the
dispatcher's retry and backoff apply (spec 07), keyed by project name rather
than wheel id — so the `work` table needs to accommodate a project-scoped
row, or ingestion keeps its own small retry table. **Decide and state it in
the module docstring**; reusing `work` with a nullable `wheel_id` plus a
`project` column is the smaller change, but it complicates the FK. Prefer a
separate `project_work` table if `work`'s shape has to bend.

The reconciliation itself:

1. Fetch the project page.
2. Filter to `*.whl` files only. Non-wheel files (sdists, eggs) are ignored
   entirely — preserve the existing behaviour.
3. Read all existing `wheels` rows for this project — one indexed query on
   `ix_wheels_project`, bounded (a project with 50k files exists; chunk if
   above a threshold).
4. Compute three sets by filename:
   - **New**: in the page, not local → `INSERT` with `state` per the rule
     below, `lane = 0`.
   - **Changed**: in both, but some stored column differs → `UPDATE` the
     changed columns, bump `change_seq`, and apply the state rule below.
   - **Vanished**: local (and not already tombstoned), not in the page →
     set `deleted_at = now`, `state = DELETED`, bump `change_seq`.
5. Upsert `pypi_index` with the serial **from the project page**, not from
   the index listing. The existing code gets this right; keep it.

**Steps 4 and 5 must be one transaction per project.** The existing
invariant — a `pypi_index` row is written only after every file for that
project is stored — is what makes partial failure safe: if the project fails
midway, the serial is not advanced and the next poll retries it. Preserve it,
and test it.

### Initial state for a new wheel

| Simple-index entry | Initial `state` |
|---|---|
| `has_metadata = True` | `NEED_METADATA` |
| `has_metadata = False` | `NO_METADATA` |

**Except**: before settling on that state, check `unlinked_blobs` for the
filename (a primary-key hit, so free). If a blob is already archived for it
— which happens when the bulk import (spec 13) ran against an index that
had not yet caught up — insert with `blob_sha256` set and
`state = NEED_CONVERT`, and delete the `unlinked_blobs` row. This turns
corpus/index drift from permanent loss into a self-healing case. See spec 13
for the full rationale and its tests.

### State transitions on a changed entry

This is the subtle part. Compute the update from what actually changed:

| Change | Action |
|---|---|
| `yanked` flipped (either direction) | Update `yanked` / `yanked_reason`, bump `change_seq`. **Do not touch `state`.** |
| `has_metadata` went `False` → `True` | Update `metadata_sha256`; if `state = NO_METADATA`, move to `NEED_METADATA`. Also delete any `permanent` skip whose reason was "no sidecar". |
| `metadata_sha256` changed while a blob is already archived | Log at warning, set `state = NEED_METADATA`, clear `blob_sha256`. PyPI republishing different metadata for the same filename should not happen; if it does, re-fetch rather than trust the stale blob. |
| `url` changed | Update it. Does not invalidate an archived blob. |
| `size`, `upload_time`, `requires_python` changed | Update them; no state change. |
| A tombstoned file reappears | Clear `deleted_at`, restore `state` to `NEED_METADATA` (or `NO_METADATA`), bump `change_seq`. |

A yank flip must **not** reset `state`. The repodata is still valid; it is the
*publishing* filter that changes, and Phase 2 reads `yanked` at publish time.
Resetting state would pointlessly re-fetch and re-convert millions of wheels
the first time a maintainer yanks a release.

The "metadata hash changed under us" case is genuinely surprising and worth
alarming on rather than silently handling — it may indicate a mirror problem
rather than a PyPI one.

### Tombstones, not deletions

Never `DELETE FROM wheels`. A tombstone is required because:

- Phase 2 needs to know the package changed so it can rebuild the shard.
- The row records which blob is now orphaned.
- PyPI's index can be briefly inconsistent, and a tombstone that later
  reverts is cheap while a re-insert loses history.

`fsck` (spec 11) should report tombstoned rows that still have a
`wheel_repodata` row — Phase 2 will need to decide whether to delete those or
filter at publish time; for Phase 1, leave them and report.

### Concurrency

Project pages are fetched by a bounded `ThreadPoolExecutor` (~32 workers)
against the `pypi.org` limiter child, which reserves 200 req/min. At that
rate 650k pages take ~5.5 hours and the reserve guarantees metadata backfill
cannot starve it.

Each worker fetches and computes; only the writer applies. A worker must
never touch the connection.

## Tests to write first

**Poll**

- A `304` response produces `not_modified=True` and zero write ops.
- Stale detection: a project absent locally is stale; one with a higher
  remote serial is stale; one with an equal serial is not; one with a
  *lower* remote serial is not (and logs a warning — serials should not go
  backwards).
- The etag from a `200` is persisted and sent on the next poll.
- The local serial map is read in chunks: with 250 rows and a chunk size of
  100, exactly 3 read transactions occur.
- No single read transaction exceeds the watchdog budget.

**New files**

- A `.whl` with `has_metadata = True` is inserted as `NEED_METADATA`.
- A `.whl` with `has_metadata = False` is inserted as `NO_METADATA`.
- A `.tar.gz` / `.egg` / `.zip` is not inserted at all.
- Every normalized column is populated from the entry.
- `lane = 0` on insert.
- A duplicate filename across two projects (should be impossible, but PyPI
  is the authority) does not crash — surface it as an error row.
- A new wheel whose filename is in `unlinked_blobs` is inserted as
  `NEED_CONVERT` with `blob_sha256` set, and the `unlinked_blobs` row is
  deleted.
- A new wheel not in `unlinked_blobs` is unaffected, and the lookup is a
  primary-key hit (query-plan assertion).

**Changed files**

- A yank flip updates `yanked` and `yanked_reason` and bumps `change_seq`,
  and leaves `state` at `READY`. **This is the most important test in the
  file.**
- An unyank flips it back.
- `yanked: "reason"` stores the reason; `yanked: true` stores `NULL`;
  `yanked: ""` stores yanked with `NULL` reason.
- `has_metadata` `False` → `True` moves `NO_METADATA` → `NEED_METADATA` and
  deletes the corresponding permanent skip.
- `has_metadata` `False` → `True` on a wheel already past `NO_METADATA`
  changes nothing but the hash.
- A changed `metadata_sha256` with a blob present clears `blob_sha256`, sets
  `NEED_METADATA`, and logs a warning.
- A changed `url` alone does not change `state` and does not clear the blob.
- An entry identical to what is stored produces **zero** write ops. Assert
  this — without it, every poll rewrites every row of every touched project
  and the WAL floods.

**Vanished files**

- A local row absent from the page is tombstoned with `deleted_at` and
  `state = DELETED`.
- An already-tombstoned row is not rewritten (no-op, no `change_seq` bump).
- A tombstoned row reappearing in the page is restored, with `deleted_at`
  cleared.
- Tombstoning never issues a `DELETE`.
- A project page that returns **zero** files tombstones everything for that
  project (and is distinguishable from a fetch failure, which must not).

**Transactionality**

- `pypi_index` is not updated when the project page fetch raises.
- `pypi_index` is not updated when a wheel write fails mid-project.
- A project that fails is retried on the next poll (its serial is unchanged).
- All of one project's changes plus its `pypi_index` upsert land in a single
  transaction.

**Error handling**

- `PyPINotFound` on a project page → the project is deleted upstream. Decide
  and test: tombstone every wheel for it and remove its `pypi_index` row, so
  it is not polled forever.
- `PyPITransientError` → `Retry` outcome with backoff, serial unchanged.
- `PyPIRateLimited` → penalize, no attempt increment.
- `PyPIProtocolError` → `Retry` plus a loud log.

## Acceptance criteria

- `sync.py` is deleted; `ingest.py` replaces it.
- Yank flips, file deletions, project deletions, and late-appearing
  `core-metadata` all propagate, each with a test.
- An unchanged project produces zero writes.
- No unbounded read anywhere in the module.
- The "serial advances only after all files are stored" invariant holds and
  is tested.
- `make ci` green, coverage 100%.

## Deferred

- Per-project-page etags. 650k conditional GETs would be cheaper than 650k
  full pages, but the serial check already avoids most fetches. Revisit if
  the `pypi.org` reserve turns out to be the constraint.
- Reacting to PyPI's changelog/event feeds instead of polling.

## Doc conflict note

The deleted `docs/index_ingestion.md` specified insert-only ingestion. The
new `docs/index_ingestion.md` should describe the diff-and-tombstone
algorithm above. Do **not** edit it yourself (`AGENTS.md`); if the doc in the
tree still describes insert-only when you start, stop and surface the
conflict.
