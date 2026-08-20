import struct

import pytest
import zstandard

from reroll_sync.archive.errors import CorruptSegmentError
from reroll_sync.archive.format import MAGIC, TRAILER_SIZE
from reroll_sync.archive.reader import SegmentReader
from reroll_sync.archive.writer import SegmentWriter


def _sealed_writer(tmp_path, segment_id=1, **kwargs):
    writer = SegmentWriter(tmp_path, segment_id, now=lambda: 0.0, **kwargs)
    return writer


# --- round trip --------------------------------------------------------------


def test_one_record_round_trips(tmp_path):
    writer = _sealed_writer(tmp_path)
    location = writer.add(b"hello world")
    writer.seal()

    reader = SegmentReader(tmp_path)
    assert reader.read(location) == b"hello world"


def test_many_records_across_multiple_blocks_all_round_trip(tmp_path):
    writer = _sealed_writer(tmp_path, block_target_bytes=64)
    payloads = [f"record number {i}".encode() * 3 for i in range(80)]
    locations = [writer.add(p) for p in payloads]
    writer.seal()

    reader = SegmentReader(tmp_path)
    for location, payload in zip(locations, payloads, strict=True):
        assert reader.read(location) == payload


def test_record_larger_than_block_target_round_trips(tmp_path):
    writer = _sealed_writer(tmp_path, block_target_bytes=100)
    big = b"y" * 5000
    location = writer.add(big)
    writer.seal()

    reader = SegmentReader(tmp_path)
    assert reader.read(location) == big


def test_zero_length_record_round_trips(tmp_path):
    writer = _sealed_writer(tmp_path)
    location = writer.add(b"")
    writer.seal()

    reader = SegmentReader(tmp_path)
    assert reader.read(location) == b""


def test_blocks_decompress_independently_via_reader(tmp_path):
    writer = _sealed_writer(tmp_path, block_target_bytes=20)
    loc_a = writer.add(b"first-block-data")
    loc_b = writer.add(b"x" * 30)  # forces a new block
    writer.seal()

    reader = SegmentReader(tmp_path)
    # Reading the second block's record must not require the first block.
    assert reader.read(loc_b) == b"x" * 30
    assert reader.read(loc_a) == b"first-block-data"


def test_iter_records_yields_every_record_in_file_order(tmp_path):
    writer = _sealed_writer(tmp_path, block_target_bytes=30)
    payloads = [f"item-{i}".encode() for i in range(20)]
    locations = [writer.add(p) for p in payloads]
    writer.seal()

    reader = SegmentReader(tmp_path)
    seen = list(reader.iter_records(1))
    assert [sha for sha, _ in seen] == [loc.sha256 for loc in locations]
    assert [data for _, data in seen] == payloads


def test_footer_records_reports_sha256_block_no_offset_length(tmp_path):
    writer = _sealed_writer(tmp_path)
    location = writer.add(b"payload")
    writer.seal()

    reader = SegmentReader(tmp_path)
    [entry] = reader.footer_records(1)
    assert entry == (location.sha256, location.block_no, location.offset, location.length)


# --- reader efficiency -------------------------------------------------------


def test_two_reads_in_the_same_block_decompress_it_once(tmp_path):
    writer = _sealed_writer(tmp_path, block_target_bytes=1000)
    loc_a = writer.add(b"aaaa")
    loc_b = writer.add(b"bbbb")
    writer.seal()

    calls = []

    def counting_decompress(data):
        calls.append(data)
        return zstandard.ZstdDecompressor().decompress(data)

    reader = SegmentReader(tmp_path, decompress=counting_decompress)
    reader.read(loc_a)
    reader.read(loc_b)

    assert len(calls) == 1


def test_iter_records_decompresses_each_block_exactly_once(tmp_path):
    writer = _sealed_writer(tmp_path, block_target_bytes=20)
    for i in range(10):
        writer.add(f"entry-{i}".encode() * 3)
    writer.seal()

    reader_probe = SegmentReader(tmp_path)
    n_blocks = len(reader_probe.footer_records(1)) and len(
        {block_no for _, block_no, _, _ in reader_probe.footer_records(1)}
    )

    calls = []

    def counting_decompress(data):
        calls.append(data)
        return zstandard.ZstdDecompressor().decompress(data)

    reader = SegmentReader(tmp_path, decompress=counting_decompress)
    list(reader.iter_records(1))

    assert len(calls) == n_blocks


def test_block_cache_evicts_the_least_recently_used_block(tmp_path):
    writer = _sealed_writer(tmp_path, block_target_bytes=4)
    locations = [writer.add(bytes([i]) * 4) for i in range(6)]
    writer.seal()

    calls = []

    def counting_decompress(data):
        calls.append(data)
        return zstandard.ZstdDecompressor().decompress(data)

    reader = SegmentReader(tmp_path, block_cache_size=2, decompress=counting_decompress)
    reader.read(locations[0])
    reader.read(locations[1])
    reader.read(locations[2])  # evicts block 0 from a 2-slot cache
    calls.clear()
    reader.read(locations[0])  # must decompress again: was evicted

    assert len(calls) == 1


def test_footer_is_read_once_per_segment_and_cached(tmp_path):
    writer = _sealed_writer(tmp_path)
    location = writer.add(b"payload")
    writer.seal()

    reader = SegmentReader(tmp_path)
    reader.read(location)
    reader.read(location)

    assert len(reader._footers) == 1


# --- corruption and recovery -------------------------------------------------


def test_file_shorter_than_trailer_raises_corrupt_segment_error(tmp_path):
    path = tmp_path / "000001.zst"
    path.write_bytes(b"short")

    reader = SegmentReader(tmp_path)
    with pytest.raises(CorruptSegmentError, match="shorter than"):
        reader.footer_records(1)


def test_truncated_segment_raises_corrupt_segment_error_naming_the_segment(tmp_path):
    writer = _sealed_writer(tmp_path)
    writer.add(b"payload")
    writer.seal()

    path = tmp_path / "000001.zst"
    data = path.read_bytes()
    path.write_bytes(data[: -(TRAILER_SIZE + 5)])  # chop off the footer and trailer

    reader = SegmentReader(tmp_path)
    with pytest.raises(CorruptSegmentError) as exc_info:
        reader.footer_records(1)
    assert exc_info.value.segment_id == 1


def test_trailer_pointing_outside_the_file_raises(tmp_path):
    writer = _sealed_writer(tmp_path)
    writer.add(b"payload")
    writer.seal()

    path = tmp_path / "000001.zst"
    data = bytearray(path.read_bytes())
    # Overwrite the trailer with one whose footer_offset is absurd.
    bad_trailer = struct.pack("<8sQQI4s", MAGIC, 999_999, 10, 0, b"\x00" * 4)
    data[-TRAILER_SIZE:] = bad_trailer
    path.write_bytes(bytes(data))

    reader = SegmentReader(tmp_path)
    with pytest.raises(CorruptSegmentError, match="outside the file"):
        reader.footer_records(1)


def test_wrong_trailer_magic_raises(tmp_path):
    writer = _sealed_writer(tmp_path)
    writer.add(b"payload")
    writer.seal()

    path = tmp_path / "000001.zst"
    data = bytearray(path.read_bytes())
    data[-TRAILER_SIZE : -TRAILER_SIZE + 8] = b"BADMAGIC"
    path.write_bytes(bytes(data))

    reader = SegmentReader(tmp_path)
    with pytest.raises(CorruptSegmentError):
        reader.footer_records(1)


def test_flipped_byte_within_decompressed_payload_fails_sha256_check(tmp_path):
    # Construct a reader whose decompressor silently returns tampered bytes,
    # to isolate the sha256 check from zstd's own frame-integrity check.
    writer = _sealed_writer(tmp_path)
    location = writer.add(b"payload-bytes")
    writer.seal()

    def tampering_decompress(data):
        real = zstandard.ZstdDecompressor().decompress(data)
        return bytes([real[0] ^ 0xFF]) + real[1:]

    reader = SegmentReader(tmp_path, decompress=tampering_decompress)
    with pytest.raises(CorruptSegmentError, match="sha256"):
        reader.read(location)


def test_footer_crc32_mismatch_raises(tmp_path):
    writer = _sealed_writer(tmp_path)
    writer.add(b"payload")
    writer.seal()

    path = tmp_path / "000001.zst"
    data = bytearray(path.read_bytes())
    # Flip a byte inside the footer frame (before the trailer) without
    # touching the trailer's own crc32 field, so decode_trailer still
    # succeeds but the crc comparison must fail.
    footer_area_index = len(data) - TRAILER_SIZE - 1
    data[footer_area_index] ^= 0xFF
    path.write_bytes(bytes(data))

    reader = SegmentReader(tmp_path)
    with pytest.raises(CorruptSegmentError, match="crc32"):
        reader.footer_records(1)
