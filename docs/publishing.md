# Publishing: sharded repodata

Output is **sharded repodata only** (per CEP-16). There is no monolithic
`repodata.json` in this design -- it was the worst-scaling job in the
system (a multi-gigabyte rebuild that cannot be incremental) and adds
nothing for consumers on modern conda/pixi/rattler.

**Scope for this iteration: correctness of the generated files only.**
There is no CDN in front of the bucket yet, and no purge behavior. That is
explicitly deferred to a follow-up; see "Deferred to a follow-up" at the
end of this document. Content-addressed shards and upload-before-reference
ordering are kept now because they cost nothing today and are exactly what
makes the follow-up purely additive.

## Layout

```
<base>/<subdir>/repodata_shards.msgpack.zst      mutable index
<base>/<subdir>/shards/<sha256-hex>.msgpack.zst  immutable, content-addressed
```

One shard index per `subdir` (`noarch`, `linux-64`, `osx-arm64`, ...),
because `reroll.stages.get_wheel_records` returns one `WheelRecord` per
platform a wheel supports -- a single `macosx_*_universal2` wheel yields
both an `osx-64` and an `osx-arm64` record, and a pure-Python wheel yields
a `noarch` record. Shards are keyed `(subdir, conda_name)`.

The exact field names inside the shard index, and the hash domain (hash of
the compressed bytes as served, vs. hash of the uncompressed msgpack
payload) must be pinned against the CEP-16 spec text during
implementation, ideally backed by a fixture test against a known-good
published shard rather than inferred from prose. This is a wire-format
compatibility contract with external tooling, not an internal detail this
document should guess at.

## Publish unit vs. shard unit

The **shard key** is `(subdir, conda_name)` -- one shard per platform per
package. The **publish unit** is `conda_name` alone: a single indexed read
of `wheels WHERE conda_name = ?` returns every wheel for that package
across every subdir, so one read is enough to rebuild all of that
package's shards in one pass.

`dirty_packages` (`db.md`) is keyed by `conda_name` for this reason.

## A publish pass

No global snapshot is taken, and no read transaction spans the whole pass.
The package is the unit of atomicity:

1. Short transaction: read up to N dirty packages from `dirty_packages`.
2. **Per package**, a short transaction: one indexed read of
   `wheels WHERE conda_name = ?` joined to `wheel_repodata`. Record the
   max `change_seq` seen. This is a single indexed lookup -- milliseconds.
3. With no transaction open: group the package's records by `subdir`,
   **excluding yanked wheels and tombstoned (`deleted_at IS NOT NULL`)
   wheels**, msgpack-encode and zstd-compress each subdir's shard, hash
   the result.
4. For each subdir shard whose hash differs from the current
   `shard_index` row: upload it (see "No-op elision" below), verify, and
   record it in `objects` with the current `change_seq` as
   `last_referenced_seq`.
5. Short transaction: update the relevant `shard_index` rows, then
   `DELETE FROM dirty_packages WHERE conda_name = ? AND last_dirtied_seq
   <= ?` -- the `<=` comparison against the max `change_seq` recorded in
   step 2 is what makes this race-free: a package re-dirtied *during* the
   pass has a higher `last_dirtied_seq` and correctly survives to the next
   pass rather than being incorrectly cleared.
6. Once per pass (not once per package): for each subdir touched during
   the pass, serialize its shard index from `shard_index` (keyset-paginated
   reads, since even a per-subdir index can be large) and upload it.

Because each shard is built from one consistent read of one package, and
shards are fetched independently by clients, there is no cross-shard or
cross-package consistency requirement to preserve -- which is exactly why
no long-lived global reader is needed. This is a deliberate change from
treating the whole pass as one snapshot: a snapshot spanning thousands of
packages would hold a reader open for the entire shard-build phase, and
per `pipeline.md` a checkpoint cannot advance past that reader.

### No-op elision

Content-addressed shards mean a rebuild that produces byte-identical
output to what's already published needs neither a re-upload nor a
`shard_index` write -- the hash comparison in step 4 already skips it.
This matters because the per-package publish unit is coarser than the
per-subdir shard: touching one wheel in `linux-64` triggers a rebuild of
that package's `noarch` shard too, but if `noarch` didn't actually change,
its rebuild is free once hashed.

## Ordering and correctness without a CDN

Even with no CDN to reason about yet, the write ordering matters for
correctness: **a shard must be uploaded and confirmed before any shard
index references it.** A reader of the shard index must never be able to
name a shard that doesn't yet exist in the bucket. This ordering is kept
strict now specifically because it is the one piece of this pass that the
follow-up CDN work cannot retrofit after the fact without a risk window.

`Cache-Control` metadata is set at upload time (via the S3-compatible
client's `CacheControl` param) even though nothing currently reads it:
`public, max-age=31536000, immutable` on shard objects, a short max-age on
the index object. Setting it now costs nothing and means the follow-up
does not need a second pass over already-published objects.

## What dirties a package

* A wheel belonging to it gains, loses, or changes its `wheel_repodata`
  (new wheel converted, or reprocessed after a reroll upgrade).
* **A `conda_name` remap dirties two packages.** If a mapper change moves
  a wheel from `foo` to `bar`, both the old and new `conda_name` must be
  marked dirty -- the dirtying code must read the previous `conda_name`
  before it is overwritten, or the old package's shard will be stale with
  no signal to rebuild it.
* A wheel is tombstoned (vanished from PyPI) or has its `yanked` flag
  flipped -- both change shard content without necessarily touching
  `wheel_repodata`.

## Project-complete gating

This is load-bearing for backfill efficiency, not an optimization to skip.

Naively, if a project's 18 wheels are converted across 18 different
dispatcher batches, its package could be dirtied and published 18 times,
each publish orphaning the previous shard version. Across a 12M-wheel
corpus with ~650k projects, the difference between "one shard build per
project" and "one shard build per wheel" is the difference between
roughly 650k uploads and roughly 12M.

Two mechanisms address this together:

1. **The backfill lane is ordered `(project, id)`** (see
   `ix_wheels_queue` in `db.md` and the lane discussion in
   `pipeline.md`), so a dispatcher batch covering N wheels naturally
   contains most or all of the wheels for each project it touches, rather
   than a scattered sample across many projects.
2. **A dirty package with any non-terminal wheel belonging to the same
   project is deferred from publishing**, up to a maximum deferral of
   about one hour -- so that a single quarantined or slow-to-convert wheel
   cannot stall that package's shard indefinitely.

Together, these produce roughly one shard build per project during
backfill, rather than one per wheel arrival.

## Garbage collection

Deferred, and never run in the same pass as a shard-index write: delete
objects from the bucket whose `last_referenced_seq` in `objects` is older
than the current `shard_index` watermark **and** whose `uploaded_at` is
older than a grace period.

With no CDN yet, the grace period only needs to cover in-flight readers of
the previous index version -- on the order of one hour is enough. Once a
CDN and purge behavior exist (the follow-up), this grace period must
instead exceed the index object's `max-age` with generous slack, since a
client may be reading a cached index far longer than one hour.

`objects` (`db.md`) exists so GC can find candidates by an indexed query
rather than listing the bucket.

## Trigger

One policy, deliberately not split into a "bulk" mode and an "ongoing"
mode:

> Publish a pass when `dirty_count >= 2000` **or**
> `oldest_dirty_age >= 5 minutes`. Never more than one pass in flight at
> once.

During backfill, the size trigger dominates, and because of
project-complete gating each pass is an efficient batch of coherent
per-project rebuilds. At steady incremental rates, the time trigger
dominates, producing small, frequent passes and repodata that is stale by
at most a few minutes. Both are the same code path -- there is no regime
detection, and no separate "bulk import" trigger to build or maintain.

## Deferred to a follow-up

Explicitly out of scope for this iteration:

* Fronting the bucket with a CDN.
* Purge-on-publish for the shard index object (Cloudflare zone purge API,
  zone ID, and a scoped purge token all need to be provisioned first).
* Purge failure handling and a purge-specific circuit breaker.
* Tuning the shard index's `max-age` against observed purge latency, and
  correspondingly widening the GC grace period described above.

Kept now because it costs nothing today and is what makes the above
additive rather than a rework: content-addressed immutable shards (so only
one small object per subdir will ever need purging, never the shards
themselves), `Cache-Control` set at upload time, strict
upload-before-reference ordering, and deferred GC with a grace period
already driven by an indexed table instead of a bucket listing.
