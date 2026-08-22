# Archive store

Where the raw `.dist-info/METADATA` bytes for every wheel live. Replaces
the previous design of uploading each wheel's metadata to R2 individually,
keyed by rowid, and re-downloading it from R2 during the parse step.

## Format

```
segments/000042.zst       sealed, immutable, never rewritten
segments/000043.open      in progress; excluded from backups by the *.open suffix

Layout of one segment file:

  [block 0][block 1] ... [block N-1][footer][trailer]

trailer (32 bytes, fixed, at EOF):
  magic "RSSEG1" | footer_offset: u64 | footer_len: u64 | checksum

footer (one zstd frame):
  per-block byte offsets, and per-record (sha256, block_no, offset, length)
```

A **block** is one independently-decompressible zstd frame holding roughly
4 MB of concatenated raw METADATA bytes. Records within a block are sorted
by project name before compression, so consecutive versions of the same
project end up adjacent -- METADATA across versions of one project is
usually near-identical, which is where most of the compression ratio comes
from. Expect roughly 8-12:1, versus roughly 3:1 if each ~5 KB file were
compressed independently.

The footer makes a segment **self-describing**: the `blobs` table in the
main database (`db.md`) is a cache over these footers, not the only record
of what a segment contains. The `verify-archive` command (`operations.md`)
walks every segment's footer, confirms each record's sha256, and
reconciles against `blobs` -- this is the disaster-recovery path, and it
costs only the footers themselves (roughly 200 MB across the whole store),
not a re-read of the compressed payload.

## Content addressing

Blobs are keyed by the **sha256 of the raw METADATA bytes**, the same
value PyPI already publishes in a file's `core-metadata` /
`dist-info-metadata` field and the same value `metadata_sync` already
verifies against on download. Platform wheels of the same project version
frequently have byte-identical METADATA, so this key naturally dedups
them -- likely 1.5-3x fewer stored records than one per wheel filename.

Two consequences that are accepted, not solved:

* **Blobs are never garbage-collected.** Segments are immutable, so
  reclaiming a blob's space would mean rewriting the segment it lives in,
  which defeats the "sealed segments are backed up exactly once, forever"
  property this design exists for. A wheel deleted from PyPI orphans its
  blob permanently. At an expected ~6 GB total and slow growth, this is an
  accepted leak, not a deferred feature.
* **No compaction of small segments.** Compacting would rewrite files,
  which is exactly the re-upload cost this design avoids. ~100 segments
  for 12M records is a perfectly fine file count to leave alone.

## Read path

* **Random access to one blob**: look up `(segment_id, block_no, offset,
  length)` in `blobs`, read that block's byte range from the segment file,
  decompress the ~4 MB block (roughly 3 ms), slice out the record.
* **Full scan** (bulk re-convert after a reroll upgrade): stream a
  segment's blocks in file order; since each block is an independent zstd
  frame, one core can own one block (or one segment) at a time with no
  cross-block dependency.

Because blocks are independent frames and segments are independent files,
both the read path and the write path parallelize linearly across cores.

## Write path (sealing)

A segment is sealed on **whichever comes first: 64 MB of compressed
output, or 6 hours since it was opened.**

Size alone is not sufficient: at steady incremental rates (roughly 33
wheels/s, ~16 KB/s compressed) a segment sized purely by bytes would stay
open for days, and an open segment is data that has not yet been backed
up. The 6-hour bound caps the at-risk window regardless of arrival rate.
This does mean some segments end up well under 64 MB; that's fine.

Mechanics: write blocks to `segments/NNNNNN.open`, `fsync`, then rename
atomically to `segments/NNNNNN.zst`. The backup mechanism (external to
this codebase -- see `operations.md`) excludes the `*.open` suffix.
Once sealed, a segment is never opened for writing again, so it is backed
up exactly once and never re-uploaded.

## Public API

`SegmentWriter` (append blocks, seal) and a block-level reader must be
public API on `reroll_sync.archive`, because the one-off script that
imports the existing 12M-row METADATA blob table needs to produce a valid
store rather than reinventing this format. See `operations.md`, "Cold
start," for how that script is expected to use it.

`verify-archive` is the paired CLI command: it must be able to reconstruct
`blobs` from segment footers alone, and this is exercised by tests as part
of the format's guarantee, not just as an operational nice-to-have.

## Disk accounting

At the target scale (~12M records, ~60 GB raw), the store is expected to
occupy roughly 6 GB sealed plus at most one segment's worth
(≤ 64 MB, bounded further by the 6-hour seal) unsealed at any time. Nothing
in this design ever holds more than one decompressed block (~4 MB) in
memory or on disk at once -- there is no step that inflates the whole
store back to its raw ~60 GB size.
