"""Local, append-only, content-addressed segment store for raw METADATA bytes.

Sealed segments are immutable, so a segment is backed up exactly once and
never re-uploaded. See ``docs/archive.md`` for why this exists.

File layout::

    segments/NNNNNN.zst   sealed, immutable, never rewritten
    segments/NNNNNN.open  in progress; backups exclude the ``.open`` suffix

``NNNNNN`` is a zero-padded six-digit decimal segment id (e.g. ``000042``).

Layout of one sealed segment file, from byte offset 0 through EOF::

    [block 0][block 1] ... [block N-1][footer][trailer]

**Block** -- one independently-decompressible zstd frame (a one-shot
compress/decompress call, not a multi-frame stream). Its decompressed
payload is the concatenation of record bytes, in the order the footer's
``records`` list gives for that block. A block holds up to
``block_target_bytes`` (default 4 MiB) of raw bytes; it closes once adding
the next record would exceed that, so one record larger than the target
gets a block entirely to itself.

**Footer** -- one more independent zstd frame, holding a msgpack-encoded
document::

    {
      "version": 1,
      "blocks":  [{"offset": <u64>, "length": <u64>, "raw_length": <u64>}, ...],
      "records": [{"sha256": <32 raw bytes>, "block_no": <u32>,
                    "offset": <u32>, "length": <u32>}, ...]
    }

``blocks[i]`` gives block *i*'s byte range within the file (``offset``,
compressed ``length``) and its decompressed size (``raw_length``).
``records[j].sha256`` is the raw 32-byte sha256 digest (msgpack bin, not
hex) of that record's bytes; ``block_no``/``offset``/``length`` locate it
within its block's decompressed payload. This is the disaster-recovery
index: every ``blobs`` row is rebuildable from footers alone, so the
footer carries per-record entries even though ``blobs`` also has them.

The footer is msgpack, not JSON: Phase 2's shard-publishing format
(``docs/publishing.md``) is msgpack + zstd already, so this reuses that
dependency rather than adding a second serialization format for Phase 1
alone. ``version`` is what lets a later phase change this encoding without
breaking old segments.

**Trailer** -- exactly 32 bytes at EOF, little-endian::

    magic         8 bytes   b"RSSEG1\\0\\0"
    footer_offset u64       byte offset of the footer frame
    footer_length u64       compressed byte length of the footer frame
    footer_crc32  u32       zlib.crc32 of the footer frame's compressed bytes
    reserved      4 bytes   zero

A reader locates the footer via the trailer, verifies ``footer_crc32``,
decompresses and msgpack-decodes it, and from there can locate and verify
any block or record without reading anything else.

Records are deduplicated by sha256 within one segment -- for idempotency
(a wheel re-fetched after a crash stores its bytes once), not as a space
optimization. The writer never reorders records, so callers should add
them in project order for the best compression. Segments are never
compacted and blobs are never garbage-collected: both would mean
rewriting an already-sealed, already-backed-up segment.
"""

from .errors import CorruptSegmentError
from .location import BlobLocation
from .reader import SegmentReader
from .store import ArchiveStore
from .writer import SegmentWriter

__all__ = [
    "ArchiveStore",
    "BlobLocation",
    "CorruptSegmentError",
    "SegmentReader",
    "SegmentWriter",
]
