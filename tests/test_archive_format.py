import struct

import msgpack
import pytest
import zstandard

from reroll_sync.archive.errors import CorruptSegmentError
from reroll_sync.archive.format import (
    MAGIC,
    TRAILER_SIZE,
    BlockEntry,
    RecordEntry,
    compress_block,
    decode_footer,
    decode_trailer,
    decompress_block,
    encode_footer,
    encode_trailer,
    footer_crc32,
)

_SHA_A = b"a" * 32
_SHA_B = b"b" * 32


# --- block compression ----------------------------------------------------


def test_compress_then_decompress_block_round_trips():
    payload = b"some record bytes" * 100
    compressed = compress_block(payload, level=3)
    assert decompress_block(compressed) == payload


def test_compressed_block_is_a_standalone_zstd_frame():
    payload = b"some record bytes" * 100
    compressed = compress_block(payload, level=3)
    assert zstandard.ZstdDecompressor().decompress(compressed) == payload


# --- footer -----------------------------------------------------------------


def test_encode_then_decode_footer_round_trips_blocks_and_records():
    blocks = (BlockEntry(offset=0, length=10, raw_length=20),)
    records = (RecordEntry(sha256=_SHA_A, block_no=0, offset=0, length=20),)

    encoded = encode_footer(blocks, records, level=3)
    footer = decode_footer(encoded, segment_id=1)

    assert footer.version == 1
    assert footer.blocks == blocks
    assert footer.records == records


def test_footer_is_a_standalone_zstd_frame_holding_msgpack():
    blocks = (BlockEntry(offset=0, length=10, raw_length=20),)
    records = (RecordEntry(sha256=_SHA_A, block_no=0, offset=0, length=20),)

    encoded = encode_footer(blocks, records, level=3)
    raw = zstandard.ZstdDecompressor().decompress(encoded)
    payload = msgpack.unpackb(raw, raw=False)

    assert payload == {
        "version": 1,
        "blocks": [{"offset": 0, "length": 10, "raw_length": 20}],
        "records": [{"sha256": _SHA_A, "block_no": 0, "offset": 0, "length": 20}],
    }


def test_footer_records_preserve_order_and_multiple_entries():
    blocks = (
        BlockEntry(offset=0, length=10, raw_length=20),
        BlockEntry(offset=10, length=8, raw_length=15),
    )
    records = (
        RecordEntry(sha256=_SHA_A, block_no=0, offset=0, length=20),
        RecordEntry(sha256=_SHA_B, block_no=1, offset=0, length=15),
    )

    encoded = encode_footer(blocks, records, level=3)
    footer = decode_footer(encoded, segment_id=7)

    assert footer.blocks == blocks
    assert footer.records == records


def test_decode_footer_on_garbage_bytes_raises_corrupt_segment_error():
    with pytest.raises(CorruptSegmentError) as exc_info:
        decode_footer(b"not a zstd frame", segment_id=42)
    assert exc_info.value.segment_id == 42


def test_decode_footer_on_valid_zstd_frame_of_non_msgpack_bytes_raises():
    # A well-formed zstd frame whose payload isn't valid msgpack at all
    # (an unterminated msgpack map header) must still be reported as this
    # segment's corruption, not propagate a raw msgpack exception.
    bad_payload = b"\x81"  # msgpack: "a map with one pair follows" then nothing
    compressed = zstandard.ZstdCompressor(level=3).compress(bad_payload)
    with pytest.raises(CorruptSegmentError) as exc_info:
        decode_footer(compressed, segment_id=5)
    assert exc_info.value.segment_id == 5


# --- footer crc32 -----------------------------------------------------------


def test_footer_crc32_matches_zlib_crc32_of_the_compressed_frame():
    import zlib

    encoded = encode_footer((), (), level=3)
    assert footer_crc32(encoded) == zlib.crc32(encoded)


def test_footer_crc32_changes_if_a_single_byte_is_flipped():
    encoded = bytearray(encode_footer((), (), level=3))
    original = footer_crc32(bytes(encoded))
    encoded[0] ^= 0xFF
    assert footer_crc32(bytes(encoded)) != original


# --- trailer -----------------------------------------------------------------


def test_trailer_is_exactly_32_bytes():
    trailer = encode_trailer(footer_offset=100, footer_length=50, footer_crc32=123)
    assert len(trailer) == TRAILER_SIZE == 32


def test_trailer_starts_with_the_magic():
    trailer = encode_trailer(footer_offset=100, footer_length=50, footer_crc32=123)
    assert trailer[:8] == MAGIC == b"RSSEG1\0\0"


def test_trailer_round_trips_footer_offset_and_length_and_crc():
    trailer = encode_trailer(footer_offset=12345, footer_length=678, footer_crc32=0xDEADBEEF)
    footer_offset, footer_length, crc = decode_trailer(trailer, segment_id=1)
    assert (footer_offset, footer_length, crc) == (12345, 678, 0xDEADBEEF)


def test_trailer_reserved_bytes_are_zero():
    trailer = encode_trailer(footer_offset=1, footer_length=1, footer_crc32=1)
    assert trailer[28:32] == b"\x00\x00\x00\x00"


def test_decode_trailer_wrong_length_raises_corrupt_segment_error():
    with pytest.raises(CorruptSegmentError) as exc_info:
        decode_trailer(b"too short", segment_id=9)
    assert exc_info.value.segment_id == 9


def test_decode_trailer_wrong_magic_raises_corrupt_segment_error():
    bad = struct.pack("<8sQQI4s", b"BADMAGC\0", 0, 0, 0, b"\x00" * 4)
    with pytest.raises(CorruptSegmentError) as exc_info:
        decode_trailer(bad, segment_id=3)
    assert exc_info.value.segment_id == 3
