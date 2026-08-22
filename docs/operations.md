# Operations

Cold start, the one-off import of existing metadata, commands, and health
metrics for running reroll-sync as a long-lived service.

## Cold start sequencing

The 12M-wheel METADATA corpus already exists locally as BLOBs in another
sqlite database, so the import is local and CPU-bound rather than
network-bound. Sequencing this correctly is most of what turns a
multi-day cold start into an hour-scale one:

| Step | Who | Wall clock |
|---|---|---|
| 1. `sync-index` -- ~650k project pages | daemon | ~5.5 h (throttle-bound at 200 req/min on the `pypi.org` reserve) |
| 2. Import existing METADATA blobs -> segments + `blobs` + `wheels.state` | one-off script, not reroll-sync | hours, zstd-bound, parallelizable |
| 3. Convert all imported wheels -> `wheel_repodata` | daemon, process pool | roughly 1 h on 16 cores |
| 4. Fetch only the gap the import didn't cover | daemon | small |
| 5. Publish shards | daemon, continuously through steps 3-4 | -- |

Without the local import, the same cold start is bound entirely by the
metadata-fetch throttle: ~10M `.metadata` sidecar requests at 1800/min is
roughly 3.5 days by itself. The import exists specifically to skip that.

### Import script

Not part of reroll-sync; a one-off script run against both databases
directly. It must use `reroll_sync.archive`'s public `SegmentWriter` API
(`archive.md`) rather than inventing its own format, so the result is a
store the daemon can read normally and `verify-archive` can validate.

Requirements:

* **Run with the daemon stopped.** The architecture's rule is one writer
  at a time to the sqlite database, not one writer forever -- a one-off
  bulk import is a legitimate reason to be the sole writer temporarily,
  the same as any admin remediation command.
* **Read the source ordered by project name** before handing records to
  the segment writer, to get the compression benefit described in
  `archive.md` (adjacent versions of one project compress well together).
* **Dedup by sha256** of the METADATA bytes as records are written, same
  as the steady-state archive does.
* **Parallelize by segment**, not by row: allocate segment IDs from a
  shared counter and let each worker own and seal its own segments
  independently, since segments are independent files with no
  cross-segment dependency.
* **Tolerate drift** between the source corpus and current PyPI state:
  link an imported blob to a `wheels` row when the filename matches: leave
  the rest recorded in a side table rather than failing the whole import.
  The source corpus is a snapshot from some prior point in time and will
  not exactly match what `sync-index` populates.
* **Watch peak disk during the import itself**, separately from the
  steady-state ~19 GB budget in `db.md`: the source database, the new
  segments, and the new sqlite database may all be resident on disk
  simultaneously during the transfer. If the source can be streamed or
  progressively dropped rather than kept whole, do that.

## Commands

**Read-only** (open the database directly, read-only, relying on WAL
reader isolation -- never block on or are blocked by the writer):

* `status` -- per-stage queue depth, oldest-pending age, retry/quarantine
  counts, bucket utilization per domain, circuit-breaker states, dirty
  package count, last publish time, and `lag` (see below).
* `stats` -- summary counts (successor to the current `stats` command).
* `errors` -- browse `errors`, filterable by category/time.
* `fsck` -- verify state-machine invariants; see below.
* `verify-archive` -- reconstruct `blobs` from segment footers and diff
  against the live table; see `archive.md`.

**Mutating** (thin clients over a unix domain socket to the running
daemon; see `pipeline.md`, "Single writer, thin CLI"):

* `pause <stage>` / `resume` -- stop or restart one stage without
  affecting others.
* `drain` -- stop accepting new work and finish in-flight tasks, for a
  clean shutdown or handoff.
* `publish-now` -- force an out-of-cadence publish pass.
* `reprocess <selector>` -- bulk state transition plus lane assignment;
  see "Reprocess campaigns" below.
* `unquarantine <selector>` -- clear `work.quarantined_at` for matching
  rows, returning them to their prior state for retry.

## Reprocess campaigns

Made explicit and observable, rather than an ad hoc `UPDATE` an admin
types by hand each time:

```sql
CREATE TABLE campaigns (
  id            INTEGER PRIMARY KEY,
  kind          TEXT NOT NULL,
  selector      TEXT NOT NULL,
  target_count  INTEGER NOT NULL,
  started_at    TEXT NOT NULL,
  finished_at   TEXT
);
```

A campaign like `reroll-sync reprocess --stage convert --reroll-version-below
0.5.0` is one transaction: clear the relevant `wheel_repodata` rows,
delete the `permanent = 0` skips at or below that version (`db.md`), set
`lane = 1` on the affected wheels, and record a `campaigns` row. Because
the queue is derived from `state`/`lane` rather than a separately
materialized task list, there is nothing to enqueue -- and nothing to
un-enqueue if the campaign is cancelled partway through. Progress is
simply `target_count` minus the current matching queue depth.

## Health metrics

**`lag`** is the headline number: the max PyPI serial seen by `sync-index`
minus the max serial fully reflected in a published shard. Every other
metric below exists to diagnose why `lag` is nonzero or growing.

| Metric | Alarm condition |
|---|---|
| `wal_bytes` | > 2 GB |
| `seconds_since_truncate_checkpoint` | rising trend -- indicates a leaked reader (see `pipeline.md`) |
| `longest_read_txn_ms` | > 250 ms |
| `open_segment_age_seconds` / `unsealed_bytes` | informational: data not yet backup-eligible |
| `disk_free_bytes` | hard floor pauses the archive-write stage before the volume fills |
| queue depth, per stage | -- |
| oldest-pending age, per stage | -- |
| retry count / quarantine count, per stage | -- |
| token-bucket utilization, per domain reserve | -- |
| circuit-breaker state, per dependency (PyPI, R2) | -- |
| `dirty_packages` count | -- |
| time since last successful publish pass | -- |

Circuit breakers are per-dependency, not global (`pipeline.md`): an R2
outage pauses publishing only, while index sync, fetching, and converting
continue unaffected.

### `fsck`

Verifies that `state` and the columns it implies have not drifted apart,
and that cross-store references resolve:

* `READY` implies a matching row exists in `wheel_repodata`.
* `NEED_CONVERT` implies `blob_sha256` is set and resolves via `blobs` to
  a byte range inside an existing segment.
* Every row in `shard_index` has a corresponding row in `objects` that is
  not `deleted_at`.
* Every row in `blobs` resolves to a byte range within its segment's
  recorded `bytes` length.

Run on a slow periodic timer and on demand; failures are reported, not
auto-repaired -- fixing a detected drift is a remediation decision for an
admin, not something `fsck` should do silently.

## Error retention

`errors` is append-only and otherwise unbounded. The periodic GC pass
(alongside deferred shard GC in `publishing.md`) also deletes `errors`
rows past a configurable retention age.
