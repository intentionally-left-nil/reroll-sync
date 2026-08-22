# Database schema

This supersedes the previous `docs/db.md`. It is a breaking, from-scratch
schema: there is no migration path from the current database, and no data
in it needs to be preserved.

## Pragmas

```sql
PRAGMA auto_vacuum = INCREMENTAL;
```

Must be set before the first `CREATE TABLE`. Changing `auto_vacuum` later
requires a full `VACUUM` (needs ~2x disk and a long exclusive lock), so it
is a schema-time decision, not a runtime one. Incremental vacuum lets the
writer reclaim pages from bulk-delete reprocess campaigns in small
`PRAGMA incremental_vacuum(N)` chunks with no long lock and no 2x disk
spike.

Runtime pragmas (set on every connection, not schema-time):

| Pragma | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | |
| `synchronous` | `NORMAL` | A crash can lose the last few commits but never corrupts the database; everything in `wheels`/`wheel_repodata` is re-derivable from PyPI + the archive. |
| `wal_autocheckpoint` | `1000` (default) | Passive checkpoints at commit boundaries. Reuses WAL pages but never shrinks the file. |
| `busy_timeout` | `5000` | |
| `mmap_size` | a few GB | Large read win once the DB is ~15+ GB. |
| `cache_size` | `-262144` (256 MB) | |

Separately, the writer thread runs `PRAGMA wal_checkpoint(TRUNCATE)`
periodically between write batches. `TRUNCATE` is the only checkpoint mode
that resets the WAL file size to zero; autocheckpoint alone will not. See
`operations.md` for the health metrics that watch this.

## Tables

### `wheels`

The core table: one row per wheel filename. Kept narrow deliberately — see
"Why the wide table was split" below.

```sql
CREATE TABLE wheels (
  id                INTEGER PRIMARY KEY,        -- explicit; see note below
  filename          TEXT NOT NULL UNIQUE,
  project           TEXT NOT NULL,              -- pypi project name, normalized
  conda_name        TEXT,                       -- shard/publish key; NULL until converted
  state             INTEGER NOT NULL,
  lane              INTEGER NOT NULL DEFAULT 0, -- 0 = incremental, 1 = backfill

  -- normalized from the simple-index file entry; raw JSON is not retained
  url               TEXT NOT NULL,
  wheel_sha256      TEXT,
  metadata_sha256   TEXT,                       -- NULL => no PEP 658/714 sidecar published
  size              INTEGER,
  upload_time       TEXT,
  requires_python   TEXT,
  yanked            INTEGER NOT NULL DEFAULT 0,
  yanked_reason     TEXT,

  blob_sha256       TEXT,                       -- -> blobs.sha256, once archived

  serial            INTEGER NOT NULL,           -- project serial that produced this row
  change_seq        INTEGER NOT NULL,           -- global monotonic counter, for publish snapshots
  deleted_at        TEXT,                       -- tombstone; rows are never hard-deleted
  updated_at        TEXT NOT NULL
);

CREATE INDEX ix_wheels_queue   ON wheels(state, lane, project, id);
CREATE INDEX ix_wheels_conda   ON wheels(conda_name) WHERE conda_name IS NOT NULL;
CREATE INDEX ix_wheels_project ON wheels(project, id);
```

**`id` is an explicit `INTEGER PRIMARY KEY`, not derived from `filename`.**
A table whose only primary key is `TEXT` has an implicit rowid, and SQLite
is explicitly permitted to renumber implicit rowids during `VACUUM`. The
previous schema used `filename TEXT PRIMARY KEY` and then stored objects in
R2 keyed by `str(rowid)` — a single `VACUUM` would have silently repointed
every stored object at the wrong wheel. An explicit integer PK removes this
hazard and gives every child table (`wheel_repodata`, `skips`, `work`,
`errors`) a cheap 8-byte foreign key instead of a ~70-byte filename.

**`ix_wheels_queue` orders by `(state, lane, project, id)`, not
`(state, lane, id)`.** The `project` column before `id` is load-bearing for
publish efficiency: it makes the backfill lane naturally process one
project's wheels together, so a package's shard is built once per project
rather than once per wheel arrival. See `publishing.md`, "Project-complete
gating."

**`state`** is an explicit integer state machine rather than the old
approach of deriving state from which nullable columns are set. See
`pipeline.md` for the full state diagram. This makes queue selection a
single index seek and queue depth a range `COUNT(*)`, instead of scanning
the whole table. The `fsck` command (`operations.md`) verifies that `state`
agrees with the columns it implies, to catch drift.

**`yanked` is an attribute, not a state.** A yank flip changes what's
served in repodata (yanked wheels are excluded — see `publishing.md`) but
does not move a wheel out of `READY`. It does mark the wheel's package
dirty for republishing.

**No `wheel_metadata` column.** The previous schema persisted the parsed
METADATA JSON as an intermediate between the download and convert steps.
It has been dropped: parse and convert are now one fused CPU-bound step
(see `pipeline.md`), so there is never a durable in-between state to store.
Re-running convert alone (e.g. after a name-mapper change) means
re-running parse too, but that only costs ~5 ms/wheel now that bytes are
local — cheaper than the ~36 GB and per-wheel write that persisting it
cost.

**No `pypi_simple` JSON column.** The raw simple-index file entry is
normalized into typed columns instead of stored as opaque JSON. The
previous approach cost ~6-12 GB across 12M wheels for a blob that indexed
nothing.

### `wheel_repodata`

The reroll conversion output. Split out of `wheels` so that advancing state
elsewhere in the pipeline never has to rewrite this payload.

```sql
CREATE TABLE wheel_repodata (
  wheel_id        INTEGER PRIMARY KEY REFERENCES wheels(id),
  repodata_zst    BLOB NOT NULL,   -- zstd-compressed JSON of the WheelRecord tuple
  name_conv_zst   BLOB,            -- zstd-compressed JSON of the NameResolution tuple
  reroll_version  TEXT NOT NULL
);
```

Stored zstd-compressed (typically 4-5x on this kind of JSON) rather than as
plain SQLite JSON, since every consumer deserializes the whole record with
pydantic and none need SQL-level JSON access into it.

### Archive tables

```sql
CREATE TABLE segments (
  id         INTEGER PRIMARY KEY,
  sealed_at  TEXT,                 -- NULL while the segment is still open (*.open on disk)
  bytes      INTEGER,
  records    INTEGER,
  footer_sha TEXT
);

CREATE TABLE blobs (
  sha256      TEXT PRIMARY KEY,    -- sha256 of the raw METADATA bytes
  segment_id  INTEGER NOT NULL REFERENCES segments(id),
  block_no    INTEGER NOT NULL,
  offset      INTEGER NOT NULL,    -- byte offset within the decompressed block
  length      INTEGER NOT NULL
);
```

This is a cache over the archive's own on-disk footers, not the only copy
of the index -- see `archive.md`. It can be rebuilt from the segment files
alone.

### `unlinked_blobs`

A staging table for the one-time bulk import of the pre-existing METADATA
corpus (`operations.md`, "Cold start"). The import joins its source blobs to
`wheels` rows by filename; a blob whose filename has no `wheels` row yet gets
recorded here instead of being discarded.

```sql
CREATE TABLE unlinked_blobs (
  filename TEXT PRIMARY KEY NOT NULL,
  sha256   TEXT NOT NULL,          -- -> blobs.sha256; the bytes are already archived
  noted_at TEXT NOT NULL
);

### `skips`

Replaces the previous single `skip_reason` column. Attributes a skip to
the stage that produced it and, critically, distinguishes a permanent
property of the wheel from one version's judgment call.

```sql
CREATE TABLE skips (
  wheel_id       INTEGER NOT NULL REFERENCES wheels(id),
  stage          TEXT NOT NULL,
  reason         TEXT NOT NULL,
  permanent      INTEGER NOT NULL, -- 1 = no reroll upgrade can ever fix this
  reroll_version TEXT,             -- NULL iff permanent = 1
  created_at     TEXT NOT NULL,
  PRIMARY KEY (wheel_id, stage)
);
```

`permanent = 1` examples: no PEP 658/714 metadata sidecar published at all.
`permanent = 0` examples: a specific `reroll_version` raised
`RerollUnconvertableError` on this wheel's METADATA.

This split is what makes "I fixed the parser, bulk re-run" actually work:

```sql
-- Clear only the skips a specific reroll version is responsible for.
DELETE FROM skips WHERE permanent = 0 AND reroll_version < '0.5.0';
UPDATE wheels SET state = NEED_CONVERT
  WHERE id IN (SELECT wheel_id FROM skips WHERE ...) OR ...;
```

without also re-touching the permanently-hopeless wheels that no reroll
upgrade will ever change.

### `work`

Sparse: a row exists only for a wheel+stage currently in trouble. Steady
state this table is nearly empty.

```sql
CREATE TABLE work (
  wheel_id        INTEGER NOT NULL REFERENCES wheels(id),
  stage           TEXT NOT NULL,
  attempts        INTEGER NOT NULL,
  next_attempt_at TEXT NOT NULL,   -- exponential backoff + jitter
  last_error      TEXT,
  quarantined_at  TEXT,            -- set once attempts exceed the max; needs admin action
  PRIMARY KEY (wheel_id, stage)
);
```

Queue selection for a stage is `<state predicate> AND wheel_id NOT IN
(<recently-deferred set>)`; the deferred set is small enough for the
dispatcher to hold in memory. See `pipeline.md`.

### `dirty_packages`

Drives publishing. Written by the same writer thread that writes
`wheel_repodata`, `wheels.deleted_at`, and `wheels.yanked`.

```sql
CREATE TABLE dirty_packages (
  conda_name       TEXT PRIMARY KEY,
  last_dirtied_seq INTEGER NOT NULL   -- from the same change_seq counter as wheels
);
```

### Publishing state

```sql
CREATE TABLE shard_index (          -- current published state, per subdir
  subdir     TEXT NOT NULL,
  conda_name TEXT NOT NULL,
  sha256     BLOB NOT NULL,         -- 32 raw bytes; content address of the shard
  PRIMARY KEY (subdir, conda_name)
);

CREATE TABLE objects (               -- shards that have been uploaded, for deferred GC
  sha256              BLOB PRIMARY KEY,
  last_referenced_seq INTEGER NOT NULL,
  uploaded_at         TEXT NOT NULL,
  deleted_at          TEXT
);
```

See `publishing.md` for how these are used.

### `errors`

```sql
CREATE TABLE errors (
  id             INTEGER PRIMARY KEY,
  wheel_id       INTEGER REFERENCES wheels(id),   -- integer FK, not filename
  error_category TEXT NOT NULL,
  error_subcat   TEXT,
  details        TEXT,
  reroll_version TEXT NOT NULL,
  created_at     TEXT NOT NULL
);
CREATE INDEX ix_errors_cat ON errors(error_category, created_at);
```

Append-only and unbounded; needs a retention policy (the GC pass in
`operations.md` should drop rows past a configurable age).

## Why the wide table was split

The previous `wheels` table carried `pypi_simple` + `wheel_metadata` +
`repodata` + `name_conversions` inline -- roughly 8 KB/row, overflowing
onto multiple B-tree pages. Every `UPDATE` that advanced a wheel's state
rewrote the *entire* row, and every rewritten page is logged to the WAL.
Across a 12M-wheel cold start, three state-advancing updates per wheel at
~8 KB each is on the order of 290 GB pushed through the WAL, all of which
then also has to be checkpointed back into the main database file.

Splitting the payload into `wheel_repodata` (written once, by `INSERT`,
when a wheel reaches `READY`) means every other state transition touches
only the ~150-byte `wheels` row. It also makes bulk reprocessing cheap and
space-reclaiming: `DELETE FROM wheel_repodata WHERE reroll_version < ?`
actually frees pages, where the old `UPDATE ... SET repodata = NULL` left
the row bloated at its previous size.

## Disk budget

| Item | Size |
|---|---|
| Segments (sealed, immutable) | ~6 GB |
| Open segment + staging | ~200 MB |
| sqlite: `wheels` + indexes | ~4 GB |
| sqlite: `wheel_repodata` (zstd) | ~8 GB |
| sqlite: `shard_index` (~3M rows) | ~200 MB |
| WAL (TRUNCATE-checkpointed) | < 500 MB steady; alarm above 2 GB |
| **Total** | **~19 GB** |

Nothing is ever decompressed in bulk to disk; reads against the archive
decompress one ~4 MB block at a time (`archive.md`).
