# 01 — Schema and database layer

**Depends on:** nothing. Do this first; everything else builds on it.

## Goal

Replace `schema.py` and `db.py` wholesale with the Phase 1 schema from
`docs/db.md`, plus connection setup that makes the WAL stay flat and the
writes stay cheap.

No data has been written anywhere, so this is a clean break. Do not write
migrations from the old schema. Delete `reroll_sync.db` from the working
tree if present (it is gitignored, but a stale file will fail validation).

## Why the old schema has to go

Four problems, all invisible at small scale and fatal at 12M rows:

1. **`wheels` has no explicit integer primary key.** `filename TEXT PRIMARY
   KEY` leaves an *implicit* rowid, and sqlite is explicitly permitted to
   renumber implicit rowids during `VACUUM`. `metadata_sync.py` used
   `str(wheel.rowid)` as the storage key, so a single `VACUUM` would
   silently repoint every stored object at the wrong wheel.
2. **Write amplification.** `wheels` carried `pypi_simple` +
   `wheel_metadata` + `repodata` + `name_conversions`, roughly 8 KB/row,
   overflowing to multiple pages. Every `UPDATE` rewrites the whole row and
   every rewritten page goes through the WAL: 12M × 3 state advances ×
   8 KB ≈ 290 GB of WAL traffic on a cold start.
3. **`skip_reason` is one unattributed column.** It cannot distinguish
   "PyPI publishes no PEP 658 sidecar" (permanent, no reroll upgrade will
   ever fix it) from "reroll 0.4.0 rejected this" (retry after upgrade).
   Without the distinction, every reroll bump either re-fetches hundreds of
   thousands of hopeless wheels or never retries the ones it should.
4. **State derived from four nullable columns** makes queue selection and
   queue depth full scans. An explicit `state` column with a composite index
   makes both index seeks.

## Requirements

### Connection setup

`db.py` exposes two connection factories with different roles.

`connect_writer(path)` — the single runtime writer, plus offline bulk tools:

```
PRAGMA journal_mode = WAL
PRAGMA synchronous = NORMAL
PRAGMA busy_timeout = 5000
PRAGMA foreign_keys = ON
PRAGMA cache_size = -262144        -- 256 MB
PRAGMA mmap_size = 4294967296      -- 4 GB
PRAGMA wal_autocheckpoint = 1000
```

`connect_reader(path)` — read-only, for CLI introspection and any read
path outside the writer thread. Opens with `mode=ro` via URI, same
`cache_size` / `mmap_size` / `busy_timeout`, `query_only = ON`.

`synchronous = NORMAL` is correct here: a crash can lose the last few
commits but cannot corrupt the file, and every row is re-derivable from
PyPI or from the segment store.

### `auto_vacuum` must be decided at creation time

```
PRAGMA auto_vacuum = INCREMENTAL
```

This **must** be issued before the first `CREATE TABLE`. Changing it later
requires a full `VACUUM`: double the disk (26 GB of scratch for a 13 GB
database) and a long exclusive lock. Bulk reprocess campaigns delete
millions of rows from `wheel_repodata`, so incremental page reclamation is
required rather than nice-to-have.

`init_db` must issue it as the first statement on a fresh database and must
verify it reads back as `2` (incremental) on an existing one, raising if
not.

### Schema versioning

Set `PRAGMA user_version = 1` at creation. `init_db` raises
`SchemaVersionError` if an existing database reports a different version.

Phase 2 will bump to `2` and add tables. Adding a *table* later is safe
with the existing validator (it only checks tables that already exist).
Adding or changing a *column* is not, and Phase 1 does not need to solve
it: until there is production data, the migration path is "drop and
recreate". State that in the module docstring so nobody builds a migration
framework prematurely.

### State enum

Define in `schema.py` as an `enum.IntEnum` named `WheelState`, stored as
`INTEGER`:

| Value | Name | Meaning |
|---|---|---|
| 0 | `NEED_METADATA` | No blob yet; eligible for the fetch stage |
| 1 | `NO_METADATA` | PyPI publishes no PEP 658 sidecar for this file |
| 2 | `NEED_CONVERT` | Blob archived; eligible for the convert stage |
| 3 | `READY` | `wheel_repodata` row exists |
| 4 | `SKIPPED` | reroll rejected it; see `skips` |
| 5 | `QUARANTINED` | Transient attempts exhausted; needs an admin |
| 6 | `DELETED` | Tombstoned — no longer present on PyPI |

Legal transitions are enumerated in `docs/pipeline.md`. Encode them as a
frozen mapping in `schema.py` (`ALLOWED_TRANSITIONS`) so spec 07 can assert
against it and spec 11's `fsck` can detect illegal states.

### Tables

Create exactly these nine. `docs/db.md` also documents `dirty_packages`,
`shard_index`, and `objects` — **do not create them in Phase 1.**

`unlinked_blobs` is not in `docs/db.md`; it exists for spec 13's bulk import
and its self-healing follow-up in spec 08. Flag it to the owner as a doc
addition rather than editing `docs/db.md` yourself.

```sql
CREATE TABLE pypi_index (
  name       TEXT PRIMARY KEY NOT NULL,
  serial     INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE wheels (
  id                INTEGER PRIMARY KEY,
  filename          TEXT NOT NULL UNIQUE,
  project           TEXT NOT NULL,
  conda_name        TEXT,
  state             INTEGER NOT NULL,
  lane              INTEGER NOT NULL DEFAULT 0,

  url               TEXT NOT NULL,
  wheel_sha256      TEXT,
  metadata_sha256   TEXT,
  size              INTEGER,
  upload_time       TEXT,
  requires_python   TEXT,
  yanked            INTEGER NOT NULL DEFAULT 0,
  yanked_reason     TEXT,

  blob_sha256       TEXT,
  serial            INTEGER NOT NULL,
  change_seq        INTEGER NOT NULL,
  deleted_at        TEXT,
  updated_at        TEXT NOT NULL
);
CREATE INDEX ix_wheels_queue      ON wheels (state, lane, project, id);
CREATE INDEX ix_wheels_conda_name ON wheels (conda_name)
  WHERE conda_name IS NOT NULL;
CREATE INDEX ix_wheels_project    ON wheels (project, id);
CREATE INDEX ix_wheels_change_seq ON wheels (change_seq);

CREATE TABLE segments (
  id         INTEGER PRIMARY KEY,
  sealed_at  TEXT,
  bytes      INTEGER,
  records    INTEGER,
  footer_sha TEXT
);

CREATE TABLE blobs (
  sha256     TEXT PRIMARY KEY NOT NULL,
  segment_id INTEGER NOT NULL REFERENCES segments(id),
  block_no   INTEGER NOT NULL,
  offset     INTEGER NOT NULL,
  length     INTEGER NOT NULL
);
CREATE INDEX ix_blobs_segment ON blobs (segment_id, block_no);

CREATE TABLE wheel_repodata (
  wheel_id            INTEGER PRIMARY KEY NOT NULL REFERENCES wheels(id),
  repodata_zst        BLOB NOT NULL,
  name_conv_zst       BLOB,
  requires_prerelease INTEGER NOT NULL DEFAULT 0,
  reroll_version      TEXT NOT NULL
);
CREATE INDEX ix_wheel_repodata_version ON wheel_repodata (reroll_version);
CREATE INDEX ix_wheel_repodata_prerelease ON wheel_repodata (requires_prerelease)
  WHERE requires_prerelease = 1;

CREATE TABLE skips (
  wheel_id       INTEGER NOT NULL REFERENCES wheels(id),
  stage          TEXT NOT NULL,
  reason         TEXT NOT NULL,
  permanent      INTEGER NOT NULL,
  reroll_version TEXT,
  created_at     TEXT NOT NULL,
  PRIMARY KEY (wheel_id, stage)
);
CREATE INDEX ix_skips_retryable ON skips (reroll_version) WHERE permanent = 0;

CREATE TABLE work (
  wheel_id        INTEGER NOT NULL REFERENCES wheels(id),
  stage           TEXT NOT NULL,
  attempts        INTEGER NOT NULL,
  next_attempt_at TEXT NOT NULL,
  last_error      TEXT,
  quarantined_at  TEXT,
  PRIMARY KEY (wheel_id, stage)
);
CREATE INDEX ix_work_ready ON work (stage, next_attempt_at)
  WHERE quarantined_at IS NULL;

CREATE TABLE errors (
  id             INTEGER PRIMARY KEY,
  wheel_id       INTEGER REFERENCES wheels(id),
  error_category TEXT NOT NULL,
  error_subcat   TEXT,
  details        TEXT,
  reroll_version TEXT NOT NULL,
  created_at     TEXT NOT NULL
);
CREATE INDEX ix_errors_cat ON errors (error_category, created_at);

-- Bulk-import bridge (spec 13). A blob whose filename has no wheels row yet;
-- spec 08's ingestion links it on insert and deletes the row.
CREATE TABLE unlinked_blobs (
  filename TEXT PRIMARY KEY NOT NULL,
  sha256   TEXT NOT NULL,
  noted_at TEXT NOT NULL
);
```

### Design points that must not be "helpfully" changed

Record these as short module docstring notes so a later agent doesn't undo
them:

- **`requires_prerelease` lives on `wheel_repodata`, not `wheels`.** It is
  an output of conversion, so it must be written atomically with, and
  deleted alongside, the repodata it describes. Putting it on `wheels`
  allows it to drift from the repodata during a reprocess campaign.
- **`ix_wheels_queue` includes `project` before `id`.** This is not
  cosmetic. Processing backfill in project order is what keeps Phase 2's
  shard uploads at ~650k instead of ~12M, because all wheels of a project
  then land in one publish pass.
- **Raw `pypi_simple` JSON is not retained.** Normalized columns instead:
  the raw entry cost 6–12 GB for 12M rows and indexed nothing. The set of
  columns above is exactly what the pipeline consumes.
- **`wheel_metadata` is not stored at all.** It is a pure intermediate,
  re-derivable from the archived bytes in ~5 ms. Persisting it cost ~36 GB.
- **`skips.reroll_version` is NULL if and only if `permanent = 1`.** Enforce
  in `fsck` (spec 11).
- **`blobs.sha256` is the sha256 of the raw METADATA bytes**, which PyPI
  publishes in `core-metadata` and which the fetch stage verifies. Blobs
  are content-addressed and shared: many wheels may reference one blob,
  because platform wheels of one version frequently have byte-identical
  METADATA.

### Keep from the existing code

The declarative `Column` / `Index` / `Table` / `ForeignKey` dataclasses in
`schema.py` and the introspection-based validator in `db.py` are good and
should be preserved in shape. They are what stops the DDL and the runtime
check from drifting. Extend them as needed for partial indexes
(`Index.where`) and integer primary keys without `AUTOINCREMENT`.

`AUTOINCREMENT` is not wanted on any table: it adds a `sqlite_sequence` row
and forbids rowid reuse, and nothing here needs monotonic-forever ids.

## Tests to write first

Each bullet is at least one test. Extend `tests/test_schema.py` (new) and
`tests/test_db.py` (rewritten).

**Pragmas and creation**

- A fresh database reports `user_version == 1`.
- A fresh database reports `auto_vacuum == 2`.
- `init_db` on a database created with `auto_vacuum = NONE` raises rather
  than silently continuing.
- `init_db` on a database with `user_version = 99` raises
  `SchemaVersionError`.
- `connect_writer` reports `journal_mode == "wal"`.
- `connect_reader` on a nonexistent path raises rather than creating a file.
- `connect_reader` rejects a write with `sqlite3.OperationalError`.
- `init_db` is idempotent: calling it twice leaves version, pragmas, and
  every table unchanged.

**Validation** — port and extend the existing `test_db.py` coverage:

- A missing column, an extra column, a wrong type, a wrong NOT NULL, a
  wrong primary key, a missing/extra foreign key, a missing/extra index,
  wrong index columns, and a wrong index uniqueness each produce a
  `SchemaMismatchError` naming the specific problem.
- A partial index whose `WHERE` clause differs is detected. (The existing
  validator does not check `WHERE` clauses at all — this is new behaviour
  and needs its own test.)
- Validation never alters an existing table.

**Schema shape**

- `wheels` has an explicit `INTEGER PRIMARY KEY` named `id`, and
  `PRAGMA table_info` reports `pk = 1` for it. This is the VACUUM-stability
  regression test — assert it directly.
- No table uses `AUTOINCREMENT` (`sqlite_sequence` does not exist).
- `dirty_packages`, `shard_index`, and `objects` do **not** exist. This is
  a Phase 1 boundary test; Phase 2 deletes it.
- Every `WheelState` value round-trips through an `INSERT`/`SELECT`.
- `ALLOWED_TRANSITIONS` contains exactly the edges in
  `docs/pipeline.md`, and contains no self-edges.

**Behaviour under the real constraints**

- Inserting a `wheels` row with a duplicate `filename` violates the unique
  constraint.
- Inserting a `wheel_repodata` row for a nonexistent `wheel_id` violates the
  foreign key (proves `foreign_keys = ON` is actually in effect on the
  writer connection).
- Two `wheels` rows may share one `blob_sha256`.
- `ix_wheels_conda_name` is not consulted for rows with `conda_name IS NULL`
  — assert via `EXPLAIN QUERY PLAN` that a `conda_name = ?` lookup uses the
  index.
- `EXPLAIN QUERY PLAN` for the fetch-stage queue query
  (`WHERE state = ? AND lane = ? ORDER BY project, id LIMIT ?`) uses
  `ix_wheels_queue` and does **not** contain `SCAN`. This is the test that
  keeps the queue from silently regressing to a full scan.

## Acceptance criteria

- `schema.py` and `db.py` are fully rewritten; no reference to
  `pypi_simple`, `wheel_metadata`, `metadata_reroll_version`, `repodata`,
  `name_conversions`, `skip_reason`, or `metadata_downloaded_at` remains
  anywhere in `src/`.
- `reroll_sync init` creates the nine tables with the exact DDL above and
  is idempotent.- Every queue query used by later specs has an `EXPLAIN QUERY PLAN` test
  proving it is index-driven.
- `make ci` green, coverage 100%.

## Deferred

- `dirty_packages`, `shard_index`, `objects` (Phase 2).
- Any migration framework. Drop-and-recreate until there is real data.
- `PRAGMA incremental_vacuum` *invocation* — the pragma is set here so the
  option exists; the writer thread calls it periodically in spec 06.

## Notes for the implementer

`docs/db.md` is the end-state reference and is authoritative on intent. If
you find a genuine conflict between it and this spec, stop and surface it
rather than editing either — `docs/` is human-authored (`AGENTS.md`).
