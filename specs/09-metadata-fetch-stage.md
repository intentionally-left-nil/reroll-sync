# 09 — Metadata fetch stage and the fused handoff

**Depends on:** 02 (archive), 04 (client), 05 (convert), 07 (dispatcher).

## Goal

`src/reroll_sync/fetch.py`: fetch each wheel's `.metadata` sidecar once,
archive the bytes into a segment, and hand the **same in-memory bytes** to
the convert stage. Delete `metadata_sync.py`.

## Why this replaces the old two-stage flow

The old flow was:

```
sync-metadata  : GET .metadata  →  PUT r2/<rowid>       →  set timestamp
parse-metadata : GET r2/<rowid> →  parse                →  store metadata
```

The bytes made a round trip through object storage between two stages that
run seconds apart. That is a wasted upload, a wasted download, and a
dependency on R2 being healthy for parsing to make progress. It also keyed
storage by `rowid`, which `VACUUM` can renumber (spec 01).

New flow — one fetch, two consumers of the same bytes:

```
                    ┌─→ SegmentWriter.add(bytes)  → blobs row
GET .metadata ──────┤
   (throttled)      └─→ convert(bytes, filename)  → wheel_repodata
```

Neither consumer blocks the other's correctness, and R2 is not involved at
all in Phase 1.

## Requirements

### Stage function

```
fetch_one(client, wheel: QueueItem, *, now) -> FetchOutcome
```

Returns the same `Ok`/`Skip`/`Retry`/`RateLimited` shape as every other stage
(spec 07). Performs **no** database writes.

Steps:

1. If `metadata_sha256 is None` **and** `has_metadata` is false for this row,
   this should not have been queued — the ingestion stage sets
   `NO_METADATA`. Treat as a programming error and return `Retry` with a loud
   log, so `fsck` catches the inconsistency.
2. `GET {url}.metadata` via `client.fetch_metadata(url, expected_sha256)`.
   The `.metadata` suffix is constructed here, where the row is in hand —
   the client takes a full URL (spec 04).
3. Map exceptions:

   | Exception | Outcome |
   |---|---|
   | `PyPINotFound` | `Skip(reason="metadata_missing", permanent=True)` — the index claimed a sidecar that does not exist |
   | `MetadataHashMismatch` | `Skip(reason="metadata_hash_mismatch", permanent=False, reroll_version=None)` plus an `errors` row carrying both digests |
   | `PyPITransientError`, `PyPIProtocolError` | `Retry` |
   | `PyPIRateLimited` | `RateLimited(retry_after=...)` |

4. On success, return `Ok` carrying the raw bytes and their sha256.

**`MetadataHashMismatch` is a `Skip`, not a `Retry`.** PyPI served content
inconsistent with its own published hash; retrying will almost certainly
return the same bytes. But it is not `permanent` either — a later index
update may fix the published hash, and the ingestion stage's
"metadata_sha256 changed" path will re-queue it. Keep the existing
`_record_hash_mismatch` behaviour from `metadata_sync.py`, including both
digests in the details; its `reroll_version` should record
`"reroll-sync"` as the existing code does, since the comparison is fixed and
does not vary with reroll.

### The archive/convert handoff

The daemon (spec 10) owns the wiring; this spec defines the contract.

- Fetch workers run in a bounded `ThreadPoolExecutor` (~64) against the
  `files.pythonhosted.org` limiter child.
- On `Ok`, the worker pushes `(wheel_id, filename, sha256, bytes)` onto a
  **bounded** `queue.Queue`.
- **The bound is load-bearing.** Metadata averages ~5 KB but the client's
  size cap is 32 MB, so an unbounded queue could hold gigabytes. Size it in
  bytes, not items — a `maxsize` of 2,000 items could be 64 GB in the worst
  case. Implement a byte-budgeted queue (accumulate `len(bytes)`; block once
  over ~256 MB) or cap items at a level safe against the 32 MB ceiling.
  This is the single most likely OOM in the design.
- A single **archive thread** drains the queue, calls `SegmentWriter.add`,
  and forwards `(wheel_id, filename, bytes)` to the convert pool. One thread,
  because a segment writer is a sequential append and must not be shared.
- The archive thread calls `should_seal()` after each add and rotates
  segments when true, submitting the seal's `segments`-row update as a
  `WriteOp`.
- **Order matters**: the `blobs` row and `wheels.blob_sha256` must not be
  written before the bytes are durably in a segment. Since a segment's footer
  is written only at seal time, a crash loses the open segment (spec 02's
  recovery truncates it). So the `blob_sha256` write must be **deferred until
  the segment is sealed**, or accept that a crash leaves `blob_sha256`
  pointing into a truncated segment.

  **Resolve it this way:** write `blob_sha256` and the `blobs` row
  immediately (so convert can proceed), and on startup, after truncating an
  unsealed segment, clear `blob_sha256` and reset `state` to `NEED_METADATA`
  for every wheel whose blob lived in that segment. That is a bounded,
  indexed cleanup (`ix_blobs_segment`) and it costs at most 6 hours of
  re-fetching. It needs a dedicated test.

- Convert results come back and are applied by the dispatcher as
  `ConvertOutcome` (spec 05, spec 07).

### Bulk convert from the archive

The same convert stage must be drivable from the archive rather than from a
fresh fetch — this is the path a reprocess campaign takes, and the reason
the segment store exists.

```
BulkConvertSource(store: ArchiveStore, dispatcher)
```

- Claims `NEED_CONVERT` items whose `blob_sha256` is set.
- **Groups claimed items by `(segment_id, block_no)`** before reading, so
  each block is decompressed once for all the records it holds. Without
  grouping, a random-ordered claim would decompress a 4 MB block per record.
  This is the difference between ~50 minutes and many hours for a 12M-wheel
  campaign, so it must be tested by counting decompressions.
- Feeds the same convert pool.

Fetching and bulk-converting are separate stage loops that may run
concurrently: fetch is throttle-bound (~33/s) and convert is CPU-bound, so a
campaign runs at full core speed while incremental fetching continues.

### Concurrency summary for this stage

| Component | Executor | Size |
|---|---|---|
| `.metadata` fetch | `ThreadPoolExecutor` | ~64 |
| archive append | dedicated thread | 1 |
| convert | `ProcessPoolExecutor` | `ncores − 2` |
| bulk archive read | thread(s) feeding convert | 2–4 |

## Tests to write first

**`fetch_one`**

- Success returns `Ok` with the exact bytes and correct sha256.
- The requested URL is `{wheel.url}.metadata`.
- `expected_sha256` is passed through from `wheels.metadata_sha256`.
- A row with `metadata_sha256 = None` but `has_metadata` true fetches with no
  verification and succeeds.
- `PyPINotFound` → `Skip(permanent=True)`.
- `MetadataHashMismatch` → `Skip(permanent=False)` and the details contain
  both the expected and actual digests.
- `PyPITransientError` → `Retry`.
- `PyPIRateLimited` → `RateLimited` carrying `retry_after`.
- A row that should have been `NO_METADATA` returns `Retry` and logs.
- `fetch_one` performs no database write (inject a connection that raises on
  any `execute`).

**Handoff and backpressure**

- The byte-budgeted queue blocks a producer once the budget is exceeded, and
  unblocks when the archive thread drains.
- A single 32 MB response does not exceed the budget on its own (i.e. the
  budget is larger than one maximum response) — otherwise the pipeline
  deadlocks. Assert the invariant explicitly.
- The same bytes object reaches both the archive and the convert pool; the
  archive is not re-read to feed convert. Assert with a counting
  `SegmentReader`.
- Fetch stops when the convert pool is saturated (backpressure propagates
  end to end).

**Segment rotation**

- `should_seal()` true triggers a seal and a new segment, and the
  `segments` row is updated via a `WriteOp`.
- Records written after rotation land in the new segment and are readable.
- A rotation mid-stream loses no records.

**Crash recovery**

- Startup with an unsealed segment containing 5 blobs: those 5 wheels are
  reset to `NEED_METADATA` with `blob_sha256` cleared, and their `blobs` rows
  are removed.
- Wheels whose blobs live in *sealed* segments are untouched by that cleanup.
- The cleanup uses `ix_blobs_segment` (query-plan assertion) and is chunked.

**Bulk convert grouping**

- 10 items spread across 3 blocks decompress exactly 3 blocks. Assert the
  count — this is the performance regression test.
- Items are claimed and grouped even when the queue returns them in id order
  that does not match block order.
- A `blobs` row whose segment is missing on disk yields `Retry`, not a crash.
- A blob whose stored bytes fail the sha256 check yields
  `Skip(reason="corrupt_blob")` and an `errors` row, and does not poison the
  batch.

**End-to-end (in-process, no network)**

- A `wheels` row in `NEED_METADATA` + a `MockTransport` serving valid
  METADATA + a real segment directory + a fake convert results in: bytes in a
  segment, a `blobs` row, `blob_sha256` set, `state = READY`, a
  `wheel_repodata` row with the right `reroll_version`, and no `work` row.
- The same with METADATA that reroll rejects results in `state = SKIPPED`, a
  `skips` row, an `errors` row, and no `wheel_repodata` row.
- The same with a pre-release wheel results in
  `wheel_repodata.requires_prerelease = 1`.

## Acceptance criteria

- `metadata_sync.py` is deleted. Its two good behaviours — sha256
  verification against the index, and hash-mismatch error recording — are
  preserved and still tested.
- Bytes are fetched from PyPI exactly once per wheel and never round-tripped
  through storage to reach the parser.
- The handoff queue is bounded **in bytes**, and the bound provably exceeds
  one maximum response.
- Bulk convert decompresses each block once per batch, proven by test.
- Crash recovery for an unsealed segment is implemented and tested.
- `make ci` green, coverage 100%.

## Deferred

- Uploading segments anywhere. The existing server backup handles it; R2
  returns in Phase 2 for shards only.
- Re-fetching a wheel whose blob is lost from a *sealed* segment. `fsck`
  reports it (spec 11); automatic repair is future work.
- Parallel segment writers. One append stream is enough at 33 records/s, and
  the bulk import (spec 13) parallelizes differently — one segment per
  worker, offline.
