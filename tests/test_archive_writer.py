import hashlib

import pytest

from reroll_sync.archive.format import TRAILER_SIZE, decode_footer, decode_trailer
from reroll_sync.archive.writer import SegmentStats, SegmentWriter


def _read_footer(path, segment_id):
    """Read the trailer and footer straight off disk, bypassing SegmentReader.

    Kept separate from the reader so writer tests do not depend on it.
    """
    data = path.read_bytes()
    trailer = data[-TRAILER_SIZE:]
    footer_offset, footer_length, crc = decode_trailer(trailer, segment_id=segment_id)
    footer_bytes = data[footer_offset : footer_offset + footer_length]
    return decode_footer(footer_bytes, segment_id=segment_id), crc, footer_bytes


def _mutable_clock(start=0.0):
    state = {"t": start}

    def now():
        return state["t"]

    def advance(seconds):
        state["t"] += seconds

    return now, advance


# --- block boundaries -------------------------------------------------------


def test_single_small_record_is_written_and_sealed(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    writer.add(b"hello world")
    stats = writer.seal()

    assert isinstance(stats, SegmentStats)
    assert stats.segment_id == 1
    assert stats.records == 1
    assert not writer.path.exists()
    assert (tmp_path / "000001.zst").exists()


def test_many_records_span_multiple_blocks(tmp_path):
    writer = SegmentWriter(tmp_path, 1, block_target_bytes=100, now=lambda: 0.0)
    for i in range(50):
        writer.add(f"record-{i}".encode() * 5)
    writer.seal()

    footer, _crc, _footer_bytes = _read_footer(tmp_path / "000001.zst", 1)
    assert len(footer.blocks) > 1
    assert len(footer.records) == 50


def test_record_larger_than_block_target_gets_its_own_block(tmp_path):
    writer = SegmentWriter(tmp_path, 1, block_target_bytes=100, now=lambda: 0.0)
    writer.add(b"small")
    big = b"x" * 1000
    writer.add(big)
    writer.add(b"small-again")
    writer.seal()

    footer, _crc, _ = _read_footer(tmp_path / "000001.zst", 1)
    big_records = [r for r in footer.records if r.length == 1000]
    assert len(big_records) == 1
    big_block_no = big_records[0].block_no
    big_block = footer.blocks[big_block_no]
    assert big_block.raw_length == 1000


def test_zero_length_record_round_trips(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    location = writer.add(b"")
    writer.seal()

    footer, _crc, _ = _read_footer(tmp_path / "000001.zst", 1)
    # A lone zero-length record still needs a real (empty) block behind it,
    # not just a footer record with nothing to decompress.
    assert len(footer.blocks) == 1
    assert footer.records[0].length == 0
    assert location.length == 0
    assert location.sha256 == hashlib.sha256(b"").hexdigest()


def test_record_exactly_block_target_bytes_gets_the_whole_block_alone(tmp_path):
    writer = SegmentWriter(tmp_path, 1, block_target_bytes=10, now=lambda: 0.0)
    writer.add(b"0123456789")  # exactly block_target_bytes
    writer.add(b"next")  # must start a new block, not append to the full one
    writer.seal()

    footer, _crc, _ = _read_footer(tmp_path / "000001.zst", 1)
    assert len(footer.blocks) == 2
    assert footer.blocks[0].raw_length == 10
    assert footer.records[0].block_no == 0
    assert footer.records[1].block_no == 1


# --- footer/trailer bookkeeping --------------------------------------------


def test_sealing_an_empty_segment_with_no_records_writes_a_valid_empty_footer(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    stats = writer.seal()

    assert stats.records == 0
    footer, _crc, _ = _read_footer(tmp_path / "000001.zst", 1)
    assert footer.blocks == ()
    assert footer.records == ()


def test_block_and_record_counts_in_footer_match_what_was_written(tmp_path):
    writer = SegmentWriter(tmp_path, 1, block_target_bytes=20, now=lambda: 0.0)
    added = [writer.add(f"rec{i}".encode()) for i in range(10)]
    writer.seal()

    footer, _crc, _ = _read_footer(tmp_path / "000001.zst", 1)
    assert len(footer.records) == len(added)
    assert {r.sha256.hex() for r in footer.records} == {loc.sha256 for loc in added}


def test_trailer_footer_offset_and_length_locate_the_footer_frame(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    writer.add(b"payload")
    writer.seal()

    path = tmp_path / "000001.zst"
    data = path.read_bytes()
    trailer = data[-TRAILER_SIZE:]
    footer_offset, footer_length, crc = decode_trailer(trailer, segment_id=1)

    footer_bytes = data[footer_offset : footer_offset + footer_length]
    assert footer_offset + footer_length + TRAILER_SIZE == len(data)
    decode_footer(footer_bytes, segment_id=1)  # does not raise


def test_footer_sha256_in_stats_matches_the_actual_footer_bytes(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    writer.add(b"payload")
    stats = writer.seal()

    _footer, _crc, footer_bytes = _read_footer(tmp_path / "000001.zst", 1)
    assert stats.footer_sha256 == hashlib.sha256(footer_bytes).hexdigest()


def test_blocks_are_independently_decompressible(tmp_path):
    import zstandard

    writer = SegmentWriter(tmp_path, 1, block_target_bytes=20, now=lambda: 0.0)
    writer.add(b"first-block-data")
    writer.add(b"x" * 30)  # forces a new block
    writer.seal()

    footer, _crc, _ = _read_footer(tmp_path / "000001.zst", 1)
    data = (tmp_path / "000001.zst").read_bytes()
    decompressor = zstandard.ZstdDecompressor()

    for block in footer.blocks:
        chunk = data[block.offset : block.offset + block.length]
        # Decompressing only this block's own byte range, with no other
        # blocks available, must still succeed and yield the right size.
        assert len(decompressor.decompress(chunk)) == block.raw_length


# --- dedup -------------------------------------------------------------------


def test_adding_identical_bytes_twice_returns_identical_locations(tmp_path):
    with SegmentWriter(tmp_path, 1, now=lambda: 0.0) as writer:
        first = writer.add(b"same bytes")
        second = writer.add(b"same bytes")
    assert first == second


def test_adding_identical_bytes_twice_stores_the_payload_once(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    writer.add(b"same bytes")
    writer.add(b"same bytes")
    writer.seal()

    footer, _crc, _ = _read_footer(tmp_path / "000001.zst", 1)
    assert len(footer.records) == 1
    assert footer.blocks[0].raw_length == len(b"same bytes")


def test_two_records_sharing_a_prefix_are_stored_separately(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    writer.add(b"prefix-aaa")
    writer.add(b"prefix-bbb")
    writer.seal()

    footer, _crc, _ = _read_footer(tmp_path / "000001.zst", 1)
    assert len(footer.records) == 2


# --- sealing -----------------------------------------------------------------


def test_should_seal_false_below_both_thresholds(tmp_path):
    now, advance = _mutable_clock()
    with SegmentWriter(
        tmp_path, 1, seal_bytes=1_000_000, seal_after_seconds=3600, now=now
    ) as writer:
        writer.add(b"tiny")
        advance(10)
        assert writer.should_seal() is False


def test_should_seal_true_once_compressed_bytes_cross_threshold(tmp_path):
    with SegmentWriter(
        tmp_path,
        1,
        block_target_bytes=10,
        seal_bytes=5,
        seal_after_seconds=3600,
        now=lambda: 0.0,
    ) as writer:
        assert writer.should_seal() is False
        writer.add(b"0123456789")  # fills and closes one block, >= seal_bytes compressed
        writer.add(b"next-block-trigger")  # forces the full block to flush
        assert writer.should_seal() is True


def test_should_seal_true_once_six_hours_elapse_under_byte_threshold(tmp_path):
    now, advance = _mutable_clock()
    with SegmentWriter(
        tmp_path, 1, seal_bytes=1_000_000, seal_after_seconds=6 * 3600, now=now
    ) as writer:
        writer.add(b"tiny")
        advance(6 * 3600)
        assert writer.should_seal() is True


def test_seal_renames_open_to_zst_and_open_no_longer_exists(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    writer.add(b"data")
    open_path = writer.path
    assert open_path.exists()

    writer.seal()

    assert not open_path.exists()
    assert (tmp_path / "000001.zst").exists()


def test_second_seal_raises(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    writer.add(b"data")
    writer.seal()
    with pytest.raises(RuntimeError):
        writer.seal()


def test_add_after_seal_raises(tmp_path):
    writer = SegmentWriter(tmp_path, 1, now=lambda: 0.0)
    writer.add(b"data")
    writer.seal()
    with pytest.raises(RuntimeError):
        writer.add(b"more data")


def test_abandoning_a_writer_without_sealing_leaves_open_and_no_zst(tmp_path):
    with SegmentWriter(tmp_path, 1, now=lambda: 0.0) as writer:
        writer.add(b"data")

    assert (tmp_path / "000001.open").exists()
    assert not (tmp_path / "000001.zst").exists()


def test_context_manager_closes_the_file_without_sealing_on_normal_exit(tmp_path):
    with SegmentWriter(tmp_path, 1, now=lambda: 0.0) as writer:
        writer.add(b"data")
        open_path = writer.path

    assert open_path.exists()
    assert not (tmp_path / "000001.zst").exists()


def test_context_manager_closes_the_file_on_exception_without_sealing(tmp_path):
    paths = {}

    def _add_then_raise():
        with SegmentWriter(tmp_path, 1, now=lambda: 0.0) as writer:
            writer.add(b"data")
            paths["open"] = writer.path
            raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        _add_then_raise()

    assert paths["open"].exists()
    assert not (tmp_path / "000001.zst").exists()


def test_context_manager_after_explicit_seal_does_not_double_close(tmp_path):
    with SegmentWriter(tmp_path, 1, now=lambda: 0.0) as writer:
        writer.add(b"data")
        writer.seal()

    assert (tmp_path / "000001.zst").exists()
