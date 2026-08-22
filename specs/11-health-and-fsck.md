# 11 — Health, metrics, and fsck

**Depends on:** 01, 02, 06, 07, 10.

## Goal

`src/reroll_sync/health.py` and `src/reroll_sync/fsck.py`. Replace
`stats.py`. Make the answer to "is this system healthy, and if not why" a
single command, and make invariant violations detectable rather than
hypothetical.

## Why `stats.py` is not enough

`compute_stats` returns two numbers — projects indexed and wheels synced.
That reports throughput but says nothing about whether the system is
*working*. In particular it would happily report success while:

- every fetch fails silently (`except OSError: continue`),
- the WAL grows toward filling the disk,
- thousands of wheels sit quarantined,
- the service is a week behind PyPI.

Every one of those is a real failure mode of the design, and each needs a
number.

## The headline metric

**`lag`** — the highest PyPI serial observed minus the highest serial fully
processed locally. One scalar that answers "are we behind, and by how much".

Define "fully processed" precisely and document it: the maximum
`pypi_index.serial` for which every non-tombstoned wheel of that project is in
a terminal state (`READY`, `SKIPPED`, `NO_METADATA`). Computing that exactly
over 650k projects is expensive, so compute an approximation cheaply and say
so:

- `index_lag` = remote `_last-serial` − max local `pypi_index.serial`
  (cheap, exact, measures ingestion freshness)
- `pipeline_backlog` = count of wheels in non-terminal states
  (cheap via the queue index, measures processing freshness)

Report both rather than one fuzzy number. Do **not** implement an expensive
exact `lag` — an approximation you can compute in milliseconds and trust is
worth more than a precise one that needs a table scan.

## Requirements

### `health.snapshot(reader, writer, limiter, breakers, stages) -> Health`

Every field must be obtainable **without** exceeding the read-transaction
budget. Any count over `wheels` must use an index.

**Freshness**

- `index_lag`, `remote_last_serial`, `local_max_serial`
- `last_index_poll_at`, `last_index_change_at`
- `projects_indexed`, `projects_stale`

**Queues** — per stage:

- `depth` (indexed `COUNT(*)` on `(state, lane)`), split by lane
- `in_flight`
- `oldest_pending_age_seconds`
- `throughput_ema` (items/sec)
- outcome counters: `ok`, `skip`, `retry`, `rate_limited`

**Wheel state census** — `COUNT(*) GROUP BY state`, index-driven. Includes
`quarantined_count` and `skipped_count`, and `requires_prerelease_count` from
the partial index on `wheel_repodata`.

**Storage / sqlite**

- `wal_bytes`
- `seconds_since_truncate_checkpoint`
- `consecutive_checkpoint_failures`
- `longest_read_txn_ms`
- `read_txn_budget_violations`
- `db_bytes`, `freelist_count`
- `writer_queue_depth`, `writer_failed_ops`

**Archive**

- `segments_sealed`, `segments_open`
- `open_segment_age_seconds`, `open_segment_bytes`
- `unsealed_records` — how many records are not yet backup-eligible
- `archive_bytes`, `disk_free_bytes`

**Rate limiting** — from `limiter.snapshot()`: per-domain available tokens,
grants, denials, current penalty deadline.

**Stages and dependencies**

- Per stage: paused, last-run, last-success, consecutive failures
- Per dependency: breaker state, consecutive failures, next trial time

**Errors**

- Counts by `error_category` over the last hour and last 24 hours (uses
  `ix_errors_cat`)

### Alarm evaluation

`health.alarms(snapshot) -> tuple[Alarm, ...]` — turn the numbers into
findings so an operator does not have to know the thresholds:

| Condition | Severity |
|---|---|
| `wal_bytes > 2 GB` | critical |
| `consecutive_checkpoint_failures >= 5` | critical — almost certainly a leaked reader |
| `disk_free_bytes < floor` | critical |
| any breaker open | warning |
| `read_txn_budget_violations` increasing | warning |
| `quarantined_count > 0` | warning |
| `index_lag` unchanged for > 1 h while stale projects remain | warning |
| `open_segment_age_seconds > 2 × seal_seconds` | warning |
| a stage with `consecutive_failures > 0` | warning |
| `writer_failed_ops > 0` | warning |

Each threshold needs a test at, just below, and just above the boundary.

### `/metrics` endpoint

Prometheus text format on localhost only, served by the daemon. Same numbers
as `snapshot()`, so there is exactly one source. Names prefixed
`reroll_sync_`. Counters as counters, gauges as gauges — do not report a
monotonic counter as a gauge.

### `fsck`

Read-only invariant checker. Reports; never repairs. Must stream and chunk
every scan — it walks 12M rows and must not hold a long read transaction or
materialize a large result.

Invariants:

**State consistency**

1. `state = READY` ⟺ a `wheel_repodata` row exists.
2. `state = NEED_CONVERT` ⟹ `blob_sha256` is set and resolves in `blobs`.
3. `state = NEED_METADATA` ⟹ `blob_sha256` is NULL.
4. `state = NO_METADATA` ⟹ `metadata_sha256` is NULL.
5. `state = SKIPPED` ⟹ at least one `skips` row.
6. `state = QUARANTINED` ⟹ a `work` row with `quarantined_at` set.
7. `state = DELETED` ⟹ `deleted_at` is set, and vice versa.
8. No wheel has a `state` value outside `WheelState`.

**Skip attribution**

9. `skips.reroll_version IS NULL` ⟺ `permanent = 1`.
10. No `skips` row for a wheel not in `SKIPPED` (a stale skip would silently
    block a wheel that was requeued).

**Work table**

11. No `work` row for a wheel in a terminal state — the dispatcher is
    supposed to delete it, and a leftover row skews attempts.
12. `work.attempts <= max_attempts`, and `attempts = max_attempts` ⟹
    quarantined.

**Archive**

13. Every `wheels.blob_sha256` resolves to a `blobs` row.
14. Every `blobs.segment_id` resolves to a `segments` row.
15. Every sealed segment exists on disk with a valid trailer (delegate to
    spec 02's `verify-archive` for byte-level checks; `fsck` checks
    existence and the row/file correspondence).
16. Report orphaned blobs — referenced by no wheel. Expected and harmless
    (blobs are never GC'd), so report as informational, not a warning.

**Sequences**

17. `change_seq` has no duplicate values among rows with differing
    `updated_at` — catches an op that forgot to take a fresh seq.
18. `MAX(change_seq)` matches the writer's counter at startup.

**Cross-cutting**

19. `conda_name` is NULL for every wheel not in `READY`.
20. Report tombstoned wheels that still have a `wheel_repodata` row. Phase 2
    decides the policy; for now this is informational.

Output: a structured report grouped by invariant, with counts and up to N
example ids per violation. Exit non-zero if any non-informational violation
is found, so it is usable in a cron check.

## Tests to write first

**Snapshot**

- Each field is populated from a hand-built database with known contents.
- Every `COUNT` used by `snapshot()` has an `EXPLAIN QUERY PLAN` test showing
  no `SCAN` over `wheels`. This is what keeps `status` from becoming a
  minute-long operation at 12M rows.
- No read in `snapshot()` exceeds the watchdog budget, asserted with strict
  mode enabled.
- `oldest_pending_age_seconds` is `None` on an empty queue.
- `index_lag` is 0 when caught up and correct when behind.
- `snapshot()` on an empty database succeeds and returns zeros, not `None`s
  or exceptions.

**Alarms**

- Each threshold at boundary − 1, exactly boundary, boundary + 1.
- No alarms on a healthy snapshot.
- Multiple simultaneous alarms are all reported.
- A critical alarm sorts before a warning.

**Metrics**

- Output parses as valid Prometheus text.
- Counter and gauge types are declared correctly.
- Every `snapshot()` field appears; no field is silently dropped. (A test
  that iterates the dataclass fields catches drift when someone adds one.)

**fsck** — one test per invariant, each constructing exactly the violation
and asserting it is reported with the right ids, plus:

- A clean database reports nothing and exits zero.
- Informational-only findings (orphaned blobs) exit zero.
- A single violation among 10,000 clean rows is found.
- Scans are chunked: a table with 1,000 rows and a chunk size of 100 performs
  10 read transactions, none exceeding budget.
- `fsck` performs no writes (connection raises on `execute` of anything
  mutating).
- Example ids are capped at N per violation, so a database with a million
  violations does not produce a million-line report.

## Acceptance criteria

- `stats.py` is deleted; `compute_stats`'s two numbers survive as fields of
  the richer snapshot.
- Every count in `snapshot()` is index-driven and proven so by a query-plan
  test.
- `fsck` covers all 20 invariants, each with a dedicated test.
- `snapshot()` and `/metrics` share one implementation.
- `fsck` and `snapshot()` are safe to run against a live daemon (read-only
  connection, chunked reads).
- `make ci` green, coverage 100%.

## Deferred

- Alerting integration (PagerDuty, Alertmanager). `/metrics` plus exit codes
  are enough for a single box.
- `fsck --repair`. Reporting first; every repair is a separate decision, and
  several (e.g. re-fetching a blob lost from a sealed segment) need network
  access that a checker should not have.
- Historical metric retention. Prometheus scraping covers it.
