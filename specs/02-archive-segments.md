# 02 — Archive: zstd segment store

**Depends on:** 01 (needs `segments` and `blobs` tables).

## Goal

A local, append-only, content-addressed store for raw METADATA bytes.
Sealed segments are immutable, so the existing server-level backup uploads
each one exactly once and never again.

New package: `src/reroll_sync/archive/`. This is the module the one-off
import script (spec 13) depends on, so it is worth doing early.

## Why this shape

- 12M × ~5 KB is ~60 GB raw (confirm the exact figure from the source
  corpus's `SUM(n_bytes)` — see spec 13). That busts the 20 GB
  uncompressed-at-rest cap as loose files, and 12M inodes changing constantly
  is miserable to back up.
- Compressing each ~5 KB body alone is weak: the existing corpus does exactly
  that with zlib level 6 and measures **2.81x**. Grouping records into ~4 MB
  blocks and sorting by project first should do substantially better, because
  consecutive versions of one project have highly similar METADATA. Note this
  is *similarity*, not identity — byte-identical bodies are a separate,
  measured ~1% (see below) and are not what blocking exploits.
- Sealed-and-immutable is the property that makes backup cheap: an
  incremental backup of the segment directory transfers only new segments.
- Reading one record must not require decompressing a whole segment, so
  blocks are independent zstd frames and the index records byte offsets.

## File format

```
segments/000042.zst     sealed, immutable, never rewritten
segments/000043.open    in progress; backups must exclude *.open

layout:
  [block 0][block 1] ... [block N-1][footer][trailer]
```

- **Block** — one independently-decompressible zstd frame. Its decompressed
  payload is the concatenation of record bytes, in the order the footer
  lists them. Target ~4 MB decompressed; close a block once adding the next
  record would exceed it, so a single record larger than the target gets a
  block of its own.
- **Footer** — one zstd frame holding a msgpack (or JSON; see below)
  document:
  ```
  {
    "version": 1,
    "blocks":  [{"offset": <u64 file offset>, "length": <u64 compressed len>,
                 "raw_length": <u64 decompressed len>}, ...],
    "records": [{"sha256": <32 raw bytes>, "block_no": <u32>,
                 "offset": <u32>, "length": <u32>}, ...]
  }
  ```
- **Trailer** — exactly 32 bytes at EOF, little-endian:
  ```
  magic         8 bytes   b"RSSEG1\0\0"
  footer_offset u64
  footer_length u64
  footer_crc32  u32
  reserved      4 bytes   zero
  ```

Use msgpack if `msgpack` is already a dependency by the time you implement
this; otherwise JSON is acceptable for Phase 1 and the `version` field lets
Phase 2 change it. Whichever you pick, the choice is part of the on-disk
contract — record it in the module docstring and cover it with a
byte-level test.

### The footer makes segments self-describing

`blobs` is a **rebuildable cache**, not the only copy of the index. A future
`reindex-archive` command can reconstruct every `blobs` row by walking
footers. That is the disaster-recovery story and it costs ~200 MB across the
whole store. Do not omit the per-record entries from the footer on the
grounds that the database already has them.

## Requirements

### `SegmentWriter`

Public API — spec 13's import script imports this, so treat it as a
supported interface, not an internal helper.

```
SegmentWriter(
    directory: Path,
    segment_id: int,
    *,
    block_target_bytes: int = 4 * 1024 * 1024,
    level: int = 10,
    now: Callable[[], float] = time.monotonic,
)
```

- `add(data: bytes) -> BlobLocation` — appends a record, returns
  `BlobLocation(sha256, segment_id, block_no, offset, length)`. Computes the
  sha256 itself.
- **Deduplicates within the segment**: adding identical bytes twice returns
  the same location and stores them once. This exists for **idempotency, not
  space** — see the measured dedup note below.
- `seal() -> SegmentStats` — flushes the open block, writes footer and
  trailer, `fsync`, then atomically renames `NNNNNN.open` to `NNNNNN.zst`.
  Idempotent; a second call raises.
- `should_seal() -> bool` — true once compressed bytes ≥ 64 MB **or** the
  writer has been open ≥ 6 hours.
- Usable as a context manager; abnormal exit leaves the `.open` file in
  place without renaming.

**Sealing on elapsed time is not optional.** At incremental rates the store
grows ~16 KB/s compressed, so a size-only policy would leave a segment open
for weeks, and an open segment is unbacked-up data. The time trigger bounds
the at-risk window to 6 hours.

Records within a block should be added in project order by the caller for
compression; the writer does not reorder (it cannot — it streams). Document
that the caller is responsible for ordering, and that ordering affects
compression ratio but never correctness.

### Exact-duplicate dedup: measured at ~1%

**The measurement:** ~1% of METADATA files are byte-identical across wheels —
same sha256, more than one wheel filename referencing them. That is a fact
about this corpus; no explanation for it is offered or needed here.

What it does and does not imply:

- **Content addressing is still the right key**, independently of the 1%.
  `sha256` of the raw body is the value PyPI publishes in `core-metadata`, the
  value the fetch stage already verifies against, and the value the existing
  corpus is already keyed by (spec 13). It makes a stored blob self-verifying
  and makes storing one idempotent. None of that depends on how often two
  wheels share a body.
- **Dedup is not a space lever.** Do not size the store, the disk budget, or
  the segment thresholds around it, and do not build cross-segment dedup
  machinery to chase 1%. Within-segment dedup is cheap (a dict of sha256s the
  writer already computes) so keep it, but its real value is that a wheel
  re-fetched after a crash or retry cannot be stored twice.
- **It says nothing about compression.** Exact duplicates and compressible
  redundancy are different quantities: two bodies can differ by one byte,
  contribute nothing to the 1%, and still compress almost to nothing together
  in the same block. The compression target below is measured separately.

### Compression target

The source corpus is currently stored as per-body zlib level 6, measured at
**2.81x**. That is the floor to beat, and beating it is the only
justification for this module existing.

This is a separate quantity from the ~1% exact-duplicate figure above and must
be measured separately. Do not assert an invented ratio. Assert two things
against a realistic sample:

1. Block-level zstd on project-ordered input achieves **≥ 4x**.
2. It is **strictly better** than compressing each record independently with
   the same codec and level.

The second assertion is the one that catches a regression to per-record
framing, which would silently give up all the cross-record savings while
still passing a fixed-ratio check.

### `SegmentReader`

- `read(location: BlobLocation) -> bytes` — decompress the one block,
  slice, verify the sha256 matches, return. Raises `CorruptSegmentError` on
  mismatch.
- **Caches the most recently decompressed block** (single-slot is enough,
  LRU of 2–4 is better). Sequential reads within a block must not
  re-decompress. This is what makes the convert stage's bulk path fast.
- `iter_records(segment_id)` — streams every `(sha256, bytes)` in file
  order, decompressing each block exactly once. Used by bulk re-convert and
  by `verify-archive`.
- Reads the footer once per segment and caches the block offset table.

### `ArchiveStore`

Thin facade over a directory plus the `blobs`/`segments` tables.

- `location_for(sha256) -> BlobLocation | None` — one indexed lookup.
- `get(sha256) -> bytes`.
- `open_writer()` / `current_writer()` — allocates the next `segment_id`,
  inserts a `segments` row with `sealed_at = NULL`.
- On seal, updates the `segments` row with `sealed_at`, `bytes`, `records`,
  `footer_sha`.
- **Crash recovery on startup**: a `.open` file on disk whose `segments` row
  is unsealed is *not* trusted. Truncate it and start a new segment;
  re-fetch anything that referenced it. Do not attempt to salvage a partial
  open segment — the footer is written last, so a partial file has no index,
  and the wheels involved are still recoverable from PyPI. There must be a
  test for this.

Blobs are **never garbage collected**. Segments are immutable, so space
cannot be reclaimed without rewriting them, which is exactly the re-upload
cost the design avoids. A deleted wheel orphans its blob permanently. At
~6 GB with slow growth this is the right trade — state it in the docstring
so nobody builds a compactor.

Likewise **no compaction of small segments.** Time-based sealing will
produce some undersized segments; there will not be many, and merging them
would dirty already-backed-up files.

### `verify-archive`

A read-only integrity pass, surfaced as a CLI command in spec 12:

- Every `segments` row with `sealed_at` set has a `NNNNNN.zst` on disk with
  a valid trailer and footer.
- Every footer record's sha256 matches the bytes it points at.
- Every `blobs` row resolves to a footer record with identical
  `(block_no, offset, length)`.
- Every footer record has a `blobs` row.
- Reports discrepancies; never repairs. Repair is a separate future command.
- Must stream: never load a whole segment, never hold a long read
  transaction (chunk `blobs` lookups by `segment_id`).

## Tests to write first

**Round trip and format**

- One record: write, seal, read back identical bytes.
- Many records spanning multiple blocks: every one reads back correctly.
- A record larger than `block_target_bytes` gets its own block and round
  trips.
- Zero-length record round trips.
- A record whose bytes are exactly `block_target_bytes` — off-by-one on the
  block-close condition.
- Trailer is exactly 32 bytes, starts with the magic, and its
  `footer_offset` + `footer_length` locate the footer frame.
- The block count and record count in the footer match what was written.
- Blocks are independently decompressible: decompressing block *k*'s byte
  range alone, without touching blocks before it, yields the right payload.

**Dedup**

- Adding identical bytes twice returns identical locations.
- Adding identical bytes twice stores the payload once (assert the
  segment's decompressed size, or the footer's block `raw_length`).
- Two different records that happen to share a prefix are stored separately.

**Sealing**

- `should_seal()` is false below both thresholds.
- `should_seal()` is true once compressed bytes cross 64 MB (inject a small
  threshold).
- `should_seal()` is true once 6 hours elapse with the writer under
  threshold — **using an injected clock, never `sleep`**.
- `seal()` renames `.open` to `.zst` and the `.open` file no longer exists.
- A second `seal()` raises.
- Abandoning a writer without sealing leaves `.open` and no `.zst`.

**Corruption and recovery**

- A truncated segment (footer removed) raises `CorruptSegmentError` on
  open, and names the segment.
- A segment whose trailer magic is wrong raises.
- A record whose stored bytes are flipped fails the sha256 check on `read`.
- A footer `crc32` mismatch raises.
- Startup with an `.open` file plus an unsealed `segments` row truncates the
  file and allocates a fresh `segment_id`, and does not raise.
- Startup with an `.open` file whose `segments` row is *missing* also
  recovers cleanly.

**Reader efficiency**

- Two `read()` calls for records in the same block decompress that block
  once. Assert by counting calls on an injected decompressor.
- `iter_records` decompresses each block exactly once.

**Store integration**

- `location_for` on an unknown sha256 returns `None`.
- `get` on an unknown sha256 raises.
- Sealing updates the `segments` row's `sealed_at`, `bytes`, `records`.
- `verify-archive` reports a `blobs` row pointing at the wrong offset.
- `verify-archive` reports a footer record with no `blobs` row.
- `verify-archive` on a clean store reports nothing.

## Acceptance criteria

- `reroll_sync.archive` exposes `SegmentWriter`, `SegmentReader`,
  `ArchiveStore`, `BlobLocation`, and `CorruptSegmentError` from its
  `__init__.py` (the one place an `__all__` is permitted per `AGENTS.md`).
- The on-disk format is documented in the package docstring precisely enough
  to write an independent reader from it.
- No test sleeps; every time-dependent behaviour uses an injected clock.
- A synthetic corpus of ~1,000 realistic METADATA documents added in project
  order compresses at **≥ 4x**, and **strictly better** than the same corpus
  compressed one frame per record. Both assertions are required: the ratio
  alone would still pass if blocking regressed to per-record framing.
  The per-body zlib-6 baseline on the real corpus is 2.81x — beating it is
  the reason this module exists.
- `make ci` green, coverage 100%.

## Deferred

- `reindex-archive` (rebuild `blobs` from footers). The footer carries what
  it needs; the command can wait.
- A trained zstd dictionary. With 4 MB blocks it buys very little, and it
  would become a permanent artifact that must be preserved alongside every
  segment forever. Deliberately rejected, not merely deferred.
- Segment compaction and blob GC — permanently out, per above.

## Open question to resolve while implementing

`msgpack` vs JSON for the footer. If Phase 2 will need `msgpack` anyway for
shard serialization, add it now and use it. Otherwise JSON keeps Phase 1
dependency-light. Either way the `version` field in the footer is what makes
the choice revisitable, so do not skip it.
