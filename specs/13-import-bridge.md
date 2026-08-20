# 13 — Import bridge for the existing 12M METADATA blobs

**Depends on:** 01 (schema), 02 (archive).

## Goal

Make it possible for an external, one-off script to load ~12M existing
METADATA blobs — currently stored as BLOBs in a separate sqlite database —
into the segment store and link them to `wheels` rows, without inventing its
own on-disk format and without going through the daemon.

**The import script itself is out of scope for this repository.** The owner
runs it separately. What is in scope is the supported API surface it needs,
the linking logic, and the reconciliation of drift.

## Why this matters so much

Fetching 12M `.metadata` files from PyPI at 2,000 req/min takes ~3.5 days.
The blobs already exist locally. Using them turns the cold start into:

| Step | Runs where | Wall clock |
|---|---|---|
| 1. `sync-index` — 650k project pages | daemon | ~5.5 h |
| 2. Import blobs → segments + `blobs` + link | external script, daemon stopped | hours, CPU-bound |
| 3. Convert 12M → `wheel_repodata` | daemon, process pool | ~1 h on 16 cores |
| 4. Fetch only what the import missed | daemon | small |

Roughly 8 hours instead of 4 days. Step 1 must precede step 2 so the `wheels`
rows exist to link against.

## The source schema

Confirmed, not assumed. The corpus lives in a separate sqlite database with
two relevant tables:

```sql
CREATE TABLE metadata_blob (
    id          INTEGER PRIMARY KEY,
    sha256      TEXT NOT NULL UNIQUE,  -- hex, of the UNCOMPRESSED body
    n_bytes     INTEGER NOT NULL,      -- uncompressed length
    z_body      BLOB NOT NULL,         -- zlib level 6, ~2.81x on real bodies
    stored_at   INTEGER,
    parsed_json TEXT                   -- see "Ignore parsed_json" below
);

CREATE TABLE wheel (
    project         TEXT NOT NULL,
    filename        TEXT NOT NULL,
    url             TEXT,
    size            INTEGER,
    upload_time     TEXT,
    requires_python TEXT,
    sha256          TEXT,              -- the wheel's own hash
    hashes_json     TEXT,
    yanked          INTEGER NOT NULL DEFAULT 0,
    yanked_reason   TEXT,
    has_metadata    INTEGER,
    metadata_sha256 TEXT,              -- JOIN KEY -> metadata_blob.sha256
    provenance_url  TEXT,
    extra_json      TEXT,
    first_seen      INTEGER,
    last_seen       INTEGER,
    PRIMARY KEY (project, filename)
) WITHOUT ROWID;
```

Facts that follow, each of which shapes the import:

- **The join is `wheel.metadata_sha256 = metadata_blob.sha256`**, not
  `metadata_blob.id`. That value is the same sha256 our `blobs.sha256` uses,
  so the key aligns exactly with the target schema and no translation is
  needed.
- **Bodies are zlib-compressed, not zstd.** The import decompresses with
  `zlib` and hands raw bytes to `SegmentWriter`, which recompresses with
  zstd at block level. There is no way to avoid the recompression: block
  compression across records is the entire point (spec 02), and zlib frames
  cannot be concatenated into one.
- **`metadata_blob` is already deduped** by its `UNIQUE(sha256)`. Exact
  duplicates — one body, same sha256, referenced by more than one wheel
  filename — account for ~1% of files, so expect ~11.9M distinct bodies for
  ~12M wheels. The import therefore iterates `metadata_blob` (bodies), not
  `wheel` (references), and the ~1% shows up as two `wheels` rows sharing one
  `blob_sha256`. Our schema already expresses that natively via
  `wheels.blob_sha256 -> blobs.sha256`; no extra machinery is needed, and none
  should be added to chase 1%.
- **`n_bytes` gives the exact raw corpus size.** Before starting, run
  `SELECT COUNT(*), SUM(n_bytes), SUM(LENGTH(z_body)) FROM metadata_blob` and
  report it. If `SUM(n_bytes)` differs materially from the ~60 GB the design
  assumes, segment sizing (64 MB seal threshold) and the disk budget need
  revisiting before the import runs, not after.
- **Bodies are `BLOB`, and deliberately so** — the source's own comment notes
  METADATA is not reliably valid UTF-8 and may contain embedded NULs. Keep
  them as `bytes` end to end. Never decode on the way through. Our convert
  worker already handles undecodable bytes as a permanent skip (spec 05), and
  that is the correct place for it to be handled.
- **The source `wheel` PK is `(project, filename)`**, but our `wheels.filename`
  is globally `UNIQUE`. A filename appearing under two projects in the source
  is therefore a conflict that must be detected and reported, not silently
  collapsed.

### Ignore `parsed_json`

The source already stores reroll's `parse_metadata` output per distinct body.
It is tempting to import it and skip parsing entirely.

**Do not.** That column records no reroll version, so its provenance is
unknown, and the entire design attributes every derived artifact to the
version that produced it (`docs/db.md`, `skips.reroll_version`,
`wheel_repodata.reroll_version`). Importing unattributed derived data would
make reprocess campaigns unable to reason about what needs redoing.

The cost of ignoring it is one parse per wheel — ~5 ms, so ~1 hour on 16
cores for 12M wheels, which step 3 already budgets. Note this decision in the
`reroll_sync.bulk` docstring so a later agent does not "optimize" by trusting
it.

### Wheels with metadata but no published hash

`has_metadata = 1 AND metadata_sha256 IS NULL` is a real case: PyPI may
publish `core-metadata: true` with no hash (spec 04's `has_metadata=True,
metadata_sha256=None`). Such a `wheel` row has **no join path** to
`metadata_blob`.

Handle it explicitly rather than letting it fall through:

- If the source populated `metadata_sha256` itself by hashing the body it
  fetched, the join works and there is nothing to do. Verify which is the
  case with a `SELECT COUNT(*) FROM wheel WHERE has_metadata = 1 AND
  metadata_sha256 IS NULL` before importing.
- If that count is non-zero, those wheels cannot be imported and must be left
  in `NEED_METADATA` for the daemon to fetch normally. Report the count so
  the operator knows how much step 4 has left to do.

This must not be discovered mid-import. Make it a precondition check the
script runs and prints up front.

## Requirements

### Public API surface

`reroll_sync.archive.__init__` exports (per spec 02) `SegmentWriter`,
`SegmentReader`, `ArchiveStore`, `BlobLocation`, `CorruptSegmentError`. This
is a **supported interface** — the import script is an external consumer, so
it may not reach into private helpers, and its signature may not change
casually. Note that contract in the package docstring.

Additionally expose from a new `reroll_sync.bulk` module:

```
allocate_segment_ids(conn, count: int) -> list[int]
```

Reserves `count` segment ids and inserts unsealed `segments` rows, so N
worker processes can each own a segment and write in parallel. Ids must be
allocated centrally even though writing is parallel.

```
link_blobs(conn, links: Iterable[BlobLink]) -> LinkStats

BlobLink(filename: str, location: BlobLocation)
```

One batched transaction (chunked) that, per link:

1. Inserts the `blobs` row (`ON CONFLICT DO NOTHING` — content-addressed, so
   a duplicate sha256 from two workers is expected and benign).
2. Looks up `wheels` by `filename`.
3. If found and `state` is `NEED_METADATA`: set `blob_sha256`,
   `state = NEED_CONVERT`, bump `change_seq`.
4. If found and `state` is `NO_METADATA`: this is informative — the local
   corpus has metadata PyPI's index says does not exist. Set `blob_sha256`
   and `state = NEED_CONVERT` anyway, and count it separately. Having the
   bytes is better than trusting the index's `core-metadata: false`.
5. If found and already `READY` / `SKIPPED`: leave state alone, but still set
   `blob_sha256` if NULL so the archive is complete.
6. If **not found**: record in `unlinked_blobs` (below). Do not fail.

`LinkStats` counts each of those six outcomes. The script prints them; a
large `not found` count is the signal that the corpus and the index have
drifted.

### `unlinked_blobs` table

The corpus will not match current PyPI exactly — it was collected at some
past time, so it contains wheels since deleted, and lacks wheels since
published. Add to the Phase 1 schema (spec 01 owns the DDL; this spec owns
the requirement):

```sql
CREATE TABLE unlinked_blobs (
  filename   TEXT PRIMARY KEY NOT NULL,
  sha256     TEXT NOT NULL,
  noted_at   TEXT NOT NULL
);
```

Purpose: a blob whose filename has no `wheels` row today may get one later
(the index lags, or a project was temporarily unavailable during step 1). So
the ingestion stage (spec 08) gains one small addition: **when inserting a
new `wheels` row, check `unlinked_blobs` for its filename**, and if present,
link it immediately as `NEED_CONVERT` and delete the `unlinked_blobs` row.
That converts drift from a permanent loss into a self-healing case, and it
costs one indexed lookup per newly ingested wheel.

Because it is keyed by `filename` (a primary key), that lookup is free. Report
the remaining count in `status`.

### Verification hooks

- `verify-archive` (spec 02) must be runnable immediately after the import
  and validate every segment the script produced. This is the acceptance gate
  for the import: if it does not pass, the import is not done.
- `fsck` (spec 11) must pass afterwards too — in particular invariant 2
  (`NEED_CONVERT` ⟹ blob resolvable) and 13 (`blob_sha256` resolves).

### Rules the external script must follow

Document these in `reroll_sync.bulk`'s docstring, since the script author
reads that rather than this spec:

1. **The daemon must be stopped.** The rule is one writer at a time, not one
   writer forever. `init` already refuses while a socket is live (spec 12);
   the import should check the same way.
2. **Sort by project before adding.** Compression depends on consecutive
   versions of one project landing in one block. `SegmentWriter` cannot
   reorder because it streams, so this is the caller's responsibility. The
   natural query is `metadata_blob` joined to `wheel` on
   `metadata_sha256 = sha256`, ordered by `wheel.project, wheel.filename`.
   Getting the ordering wrong does not break correctness — it just wastes a
   large fraction of the compression.
3. **A blob shared across projects picks one project for ordering.** With ~1%
   sharing this is a rounding error; take the first project seen and move on.
   Do not write the body twice.
4. **Decompress with zlib, hand raw `bytes` to `SegmentWriter`.** Never
   decode to `str` at any point — bodies may be invalid UTF-8 with embedded
   NULs.
5. **Partition work by project, not round-robin.** Each worker owns whole
   projects and its own segment, so a project's cross-version redundancy
   stays inside one worker's blocks.
6. **Seal every segment before exiting.** An unsealed `.open` segment will be
   truncated by the daemon's recovery on next start (spec 09) and its wheels
   reset to `NEED_METADATA`.
7. **Do not set `state = READY` or write `wheel_repodata`.** The import's job
   ends at `NEED_CONVERT`; the daemon converts. Converting inside the import
   would duplicate spec 05's pre-release logic.
8. **Ignore `parsed_json`** — see above.
9. **Peak disk**: the source database and the new segments coexist. The
   source stores bodies at ~2.81x, so ~60 GB raw is ~21 GB stored, plus the
   new ~6 GB of segments, plus the target database. That fits the 160 GB
   shared volume with room, but confirm with the `SUM(LENGTH(z_body))` query
   rather than trusting the estimate, and check free space before starting.

### What this spec deliberately does not provide

No `reroll-sync import` subcommand. The import is a one-time operation
against a schema the source database happens to have; baking a bespoke reader
for it into the shipped CLI would be permanent code for a single event. The
supported surface is the archive API plus `link_blobs`, which is the reusable
part.

## Tests to write first

**`allocate_segment_ids`**

- Returns `count` distinct ids and inserts that many unsealed `segments`
  rows.
- Two successive calls never overlap.
- Resumes above the existing maximum on a database that already has
  segments.
- `count = 0` returns an empty list and writes nothing.

**`link_blobs`**

One test per outcome branch:

- A `NEED_METADATA` wheel becomes `NEED_CONVERT` with `blob_sha256` set and
  `change_seq` bumped.
- A `NO_METADATA` wheel becomes `NEED_CONVERT` and is counted separately.
- A `READY` wheel keeps its state but gains `blob_sha256` if it was NULL.
- A `READY` wheel with a *different* `blob_sha256` already set is left alone
  and reported as a conflict rather than overwritten.
- A `SKIPPED` wheel keeps its state.
- An unknown filename lands in `unlinked_blobs` and does not raise.
- Duplicate sha256 across two links inserts one `blobs` row.
- Duplicate filename in the input is idempotent.
- Linking is chunked: 250 links with chunk size 100 produce 3 transactions.
- `LinkStats` counts match a hand-built mixed input.

**Self-healing via ingestion**

- Ingesting a new wheel whose filename is in `unlinked_blobs` links it
  immediately as `NEED_CONVERT` and deletes the `unlinked_blobs` row.
- Ingesting a wheel not in `unlinked_blobs` performs the lookup and proceeds
  normally (one extra indexed lookup, no behaviour change).
- The lookup is a primary-key hit — query-plan assertion, no scan.

**Integration**

- Write 1,000 synthetic blobs across 3 segments via `SegmentWriter`, seal
  them, `link_blobs` them against a matching set of `wheels` rows, then run
  `verify-archive` and `fsck`: both clean.
- The same with 50 unmatched filenames: `verify-archive` clean, `fsck` clean,
  50 rows in `unlinked_blobs`.
- Every blob is readable via `ArchiveStore.get` after linking.
- Compression of a project-sorted synthetic corpus is ≥ 4x and strictly
  better than the same corpus compressed one frame per record. Asserting both
  directions documents why the ordering rule exists.

**Source-shaped fixtures**

Build a miniature source database with the real `metadata_blob` / `wheel`
schema and drive the linking against it. These are the tests that catch a
mis-read of the source, which is the failure mode that would waste a
multi-hour import:

- A zlib level-6 `z_body` decompresses to bytes whose sha256 equals the
  stored `sha256` and whose length equals `n_bytes`.
- A body that is **not valid UTF-8** and a body containing an **embedded
  NUL** both survive the round trip through `SegmentWriter` and
  `ArchiveStore.get` byte-for-byte. Neither may be decoded anywhere in the
  path.
- A `z_body` whose decompressed sha256 does **not** match its `sha256` column
  is reported and skipped, not imported. Source corruption must not become
  archive corruption.
- A `wheel` row with `has_metadata = 1 AND metadata_sha256 IS NULL` produces
  no link and is counted in the precondition report.
- A `wheel` row whose `metadata_sha256` has no `metadata_blob` is counted, not
  fatal.
- One `metadata_blob` referenced by `wheel` rows in two different projects
  writes the body once and links both wheels.
- The same `filename` under two different source projects is reported as a
  conflict (our `wheels.filename` is globally unique) and does not raise.
- `n_bytes` disagreeing with the actual decompressed length is reported.
- The precondition query (`COUNT(*)`, `SUM(n_bytes)`,
  `SUM(LENGTH(z_body))`) runs and its results are surfaced.
- `parsed_json` is present in the fixture and **never read** — assert via a
  source connection wrapper that fails if the column is selected. This is the
  test that stops a future agent from trusting unattributed derived data.

## Acceptance criteria

- `reroll_sync.bulk` exposes `allocate_segment_ids` and `link_blobs` with the
  documented semantics, and its docstring states the six rules.
- `unlinked_blobs` exists and is consulted by the ingestion stage on insert.
- The archive API is stable enough that an external script can produce
  segments that pass `verify-archive` without touching private helpers.
- A full synthetic round trip — write, seal, link, verify, fsck — passes.
- No `import` subcommand is added to the CLI.
- `make ci` green, coverage 100%.

## Deferred

- Reading the source database beyond the two tables documented above.
  Anything else in it is the script's business.
- Parallel `link_blobs` from multiple processes. Segments are written in
  parallel; linking is fast enough to do from one process at the end, and it
  keeps the single-writer rule intact.
- Re-importing to fix a bad import. Segments are immutable; the recovery is
  to delete the segment files and their rows and start over, which is
  acceptable for a one-time operation.

## Preconditions to verify before the import runs

The source schema is confirmed, so the remaining unknowns are quantities, not
structure. The script must print all of these before writing anything, and an
operator must read them:

```sql
-- Corpus size, to validate segment sizing and the disk budget.
SELECT COUNT(*), SUM(n_bytes), SUM(LENGTH(z_body)) FROM metadata_blob;

-- Wheels with a sidecar but no join key: these cannot be imported.
SELECT COUNT(*) FROM wheel WHERE has_metadata = 1 AND metadata_sha256 IS NULL;

-- Wheels whose join key resolves to nothing.
SELECT COUNT(*) FROM wheel w
 WHERE w.metadata_sha256 IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM metadata_blob b WHERE b.sha256 = w.metadata_sha256);

-- Filenames appearing under more than one project: conflicts with our
-- globally-unique wheels.filename.
SELECT COUNT(*) FROM (
  SELECT filename FROM wheel GROUP BY filename HAVING COUNT(DISTINCT project) > 1
);

-- Free space on the segments volume must exceed the projected segment size
-- with margin.
```

A non-zero result on any of the middle three is not a blocker — each has a
defined handling above — but each translates directly into how much work
step 4 has left, so it should be known in advance rather than discovered as a
surprise backlog.
