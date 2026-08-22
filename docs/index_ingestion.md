# PyPI index ingestion

Supersedes the previous `docs/index_ingestion.md`. Keeps the core
algorithm and its partial-failure safety property, but removes the
insert-only assumption: this design must support deletion and yanking of
existing wheels, which the previous version explicitly did not attempt.

## What changes from the previous design

* **Not insert-only.** The previous algorithm only ever inserted new
  `wheels` rows (`ON CONFLICT(filename) DO NOTHING`) and only ever
  inserted-or-refreshed `pypi_index.serial`. It never revisited a wheel
  once inserted, so a `yanked` flip on PyPI never reached the database.
  When a project's serial advances, its previously-seen files must now be
  **diffed**, not just unioned with.
* **No raw JSON retained.** The previous `wheels.pypi_simple` column stored
  each file's simple-index entry as opaque JSON (~6-12 GB across 12M
  wheels, indexing nothing). This is replaced by normalizing the fields
  that matter into typed `wheels` columns (`db.md`): `url`,
  `wheel_sha256`, `metadata_sha256`, `size`, `upload_time`,
  `requires_python`, `yanked`, `yanked_reason`.
* **Conditional GET on `/simple/`.** Not previously done; see below.
* **Concurrency and a dedicated rate reserve.** The previous algorithm
  fetched project pages one at a time on a single blocking connection
  with no rate limiting at all. It now runs under the `pypi.org` bucket
  reserve described in `pipeline.md` (200/min, shared with index polls),
  at roughly 32 requests in flight.

## Update algorithm

1. Conditional `GET /simple/` (ETag / `If-Modified-Since`). A `304`
   short-circuits the entire pass -- nothing has changed since the last
   run, and no per-project work is needed.
2. On a `200`, parse the response and compare `meta._last-serial` against
   the last-seen value; if unchanged, likewise short-circuit.
3. `SELECT name, serial FROM pypi_index` for an in-memory map of
   previously-synced projects.
4. Filter to projects that are missing locally, or whose index serial is
   newer than the stored one.
5. For each outdated project, under the `pypi.org` bucket reserve:
   1. `GET /simple/{name}/`.
   2. **Diff** the response's file list against the project's existing
      `wheels` rows (matched by `filename`):
      * A file present in the response but not locally: insert a new
        `wheels` row in state `NEED_METADATA` (or `NO_METADATA` --
        see below), lane assigned per the incremental/backfill rule in
        `pipeline.md`.
      * A file present in both, where the entry's `yanked` value differs
        from the stored value: update `yanked`/`yanked_reason` and mark
        the wheel's `conda_name` dirty (if it has converted to one
        already; see `publishing.md`, "What dirties a package").
      * A file present locally but **absent** from the response: set
        `deleted_at` (tombstone, never a hard delete) and mark the
        wheel's `conda_name` dirty, same as a yank flip.
   3. Only once every file in the response has been reconciled:
      `INSERT INTO pypi_index (name, serial, updated_at) ... ON CONFLICT
      DO UPDATE SET serial = excluded.serial, updated_at =
      excluded.updated_at`, using the serial **from the project page**,
      not the top-level index.
6. If any step for a project fails partway, stop processing that project
   and move on to the next. The `pypi_index` upsert for that project does
   not happen, so a later pass retries it from scratch. This preserves the
   previous design's core safety property: `pypi_index.serial` only
   advances once every file for that project has been fully reconciled.

## Metadata sidecar availability

`metadata_hashes()` (existing logic in `pypi_client.py`, kept as-is)
distinguishes "no PEP 658/714 sidecar published" from "sidecar published
but unhashed" from "sidecar published with a hash." Only the first case
routes a wheel to the permanent `NO_METADATA` state (`pipeline.md`) --
this is a `skips` row with `permanent = 1` once encountered downstream,
not something that ingestion itself writes to `skips` directly, since
ingestion's job is only to populate `metadata_sha256` (`NULL` or not) on
the `wheels` row.

## Rate limiting

Ingestion draws from the `pypi.org` reserve of the hierarchical token
bucket described in `pipeline.md` (200/min of the global 2000/min),
shared between the index poll and per-project page fetches. This reserve
is intentionally separate from the `files.pythonhosted.org` reserve used
for `.metadata` sidecar fetches, so that a large backlog of pending
metadata fetches can never delay index polling or project-page diffing.

Project pages are fetched with up to ~32 requests in flight, consistent
with the executor sizing table in `pipeline.md`.
