"""On-disk wire format for one archive segment: blocks, footer, trailer.

See ``reroll_sync.archive`` for the full segment file layout this module
implements the byte-level encode/decode for.
"""

from __future__ import annotations

import struct
import zlib
from collections.abc import Iterable
from dataclasses import dataclass

import msgpack
import zstandard

from .errors import CorruptSegmentError

MAGIC = b"RSSEG1\0\0"
TRAILER_SIZE = 32
FOOTER_VERSION = 1

_TRAILER_STRUCT = struct.Struct("<8sQQI4s")
_RESERVED = b"\x00" * 4


@dataclass(frozen=True)
class BlockEntry:
    """One block's position and sizes within a segment file."""

    offset: int
    length: int
    raw_length: int


@dataclass(frozen=True)
class RecordEntry:
    """One record's raw 32-byte sha256 digest and position within its block."""

    sha256: bytes
    block_no: int
    offset: int
    length: int


@dataclass(frozen=True)
class Footer:
    """A decoded footer document: every block and record in a segment."""

    version: int
    blocks: tuple[BlockEntry, ...]
    records: tuple[RecordEntry, ...]


def compress_block(data: bytes, *, level: int) -> bytes:
    """Compress ``data`` into one independently-decompressible zstd frame."""
    return zstandard.ZstdCompressor(level=level).compress(data)


def decompress_block(data: bytes) -> bytes:
    """Decompress one zstd frame written by :func:`compress_block`."""
    return zstandard.ZstdDecompressor().decompress(data)


def encode_footer(
    blocks: Iterable[BlockEntry], records: Iterable[RecordEntry], *, level: int
) -> bytes:
    """Encode blocks/records as msgpack, then compress as one zstd frame."""
    payload = {
        "version": FOOTER_VERSION,
        "blocks": [
            {"offset": b.offset, "length": b.length, "raw_length": b.raw_length} for b in blocks
        ],
        "records": [
            {"sha256": r.sha256, "block_no": r.block_no, "offset": r.offset, "length": r.length}
            for r in records
        ],
    }
    return compress_block(msgpack.packb(payload, use_bin_type=True), level=level)


def decode_footer(compressed: bytes, *, segment_id: int) -> Footer:
    """Decompress and parse a footer frame written by :func:`encode_footer`.

    Raises :class:`CorruptSegmentError` if ``compressed`` is not a valid
    zstd frame or does not decode to the expected msgpack document shape.
    """
    try:
        raw = decompress_block(compressed)
        payload = msgpack.unpackb(raw, raw=False)
    except Exception as exc:
        raise CorruptSegmentError(segment_id, f"footer is not valid: {exc}") from exc
    blocks = tuple(
        BlockEntry(offset=b["offset"], length=b["length"], raw_length=b["raw_length"])
        for b in payload["blocks"]
    )
    records = tuple(
        RecordEntry(
            sha256=r["sha256"], block_no=r["block_no"], offset=r["offset"], length=r["length"]
        )
        for r in payload["records"]
    )
    return Footer(version=payload["version"], blocks=blocks, records=records)


def encode_trailer(*, footer_offset: int, footer_length: int, footer_crc32: int) -> bytes:
    """Encode the fixed 32-byte trailer written at the end of a segment file."""
    return _TRAILER_STRUCT.pack(MAGIC, footer_offset, footer_length, footer_crc32, _RESERVED)


def decode_trailer(data: bytes, *, segment_id: int) -> tuple[int, int, int]:
    """Decode a 32-byte trailer into ``(footer_offset, footer_length, footer_crc32)``."""
    if len(data) != TRAILER_SIZE:
        raise CorruptSegmentError(
            segment_id, f"trailer is {len(data)} bytes, expected {TRAILER_SIZE}"
        )
    magic, footer_offset, footer_length, crc32_value, _reserved = _TRAILER_STRUCT.unpack(data)
    if magic != MAGIC:
        raise CorruptSegmentError(segment_id, f"trailer magic {magic!r} does not match {MAGIC!r}")
    return footer_offset, footer_length, crc32_value


def footer_crc32(compressed_footer: bytes) -> int:
    """Checksum of the compressed footer frame, as stored in the trailer."""
    return zlib.crc32(compressed_footer)
