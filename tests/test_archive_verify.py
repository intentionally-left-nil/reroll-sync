import pytest
import zstandard

from reroll_sync.archive.reader import SegmentReader
from reroll_sync.archive.store import ArchiveStore
from reroll_sync.archive.verify import VerifyReport, verify_archive
from reroll_sync.db import connect_writer, init_db


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    connection = connect_writer(db_path)
    try:
        yield connection
    finally:
        connection.close()


def _sealed_store(tmp_path, conn, records=(b"one", b"two", b"three")):
    store = ArchiveStore(tmp_path / "segments", conn)
    for record in records:
        store.add(record)
    store.seal_writer(store.current_writer())
    return store


# --- clean store ---------------------------------------------------------


def test_clean_store_reports_nothing(tmp_path, conn):
    store = _sealed_store(tmp_path, conn)
    report = verify_archive(store)
    assert report == VerifyReport(())
    assert report.ok is True


def test_store_with_no_sealed_segments_reports_nothing(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    report = verify_archive(store)
    assert report.ok is True


# --- blobs table discrepancies --------------------------------------------


def test_blobs_row_pointing_at_the_wrong_offset_is_reported(tmp_path, conn):
    store = _sealed_store(tmp_path, conn)
    sha256, segment_id = conn.execute("SELECT sha256, segment_id FROM blobs LIMIT 1").fetchone()
    conn.execute(
        "UPDATE blobs SET offset = offset + 999 WHERE sha256 = ? AND segment_id = ?",
        (sha256, segment_id),
    )
    conn.commit()

    report = verify_archive(store)

    assert not report.ok
    assert any(sha256 in p and "footer says" in p for p in report.problems)


def test_footer_record_with_no_blobs_row_is_reported(tmp_path, conn):
    store = _sealed_store(tmp_path, conn)
    conn.execute("DELETE FROM blobs")
    conn.commit()

    report = verify_archive(store)

    assert not report.ok
    assert any("has no blobs row" in p for p in report.problems)
    assert len(report.problems) == 3  # one per record added in _sealed_store


def test_blobs_row_with_no_matching_footer_record_is_reported(tmp_path, conn):
    store = _sealed_store(tmp_path, conn)
    segment_id = store.sealed_segment_ids()[0]
    conn.execute(
        "INSERT INTO blobs (sha256, segment_id, block_no, offset, length) VALUES (?, ?, 0, 0, 1)",
        ("f" * 64, segment_id),
    )
    conn.commit()

    report = verify_archive(store)

    assert not report.ok
    assert any("has no matching footer record" in p for p in report.problems)


# --- content corruption ---------------------------------------------------


def test_record_bytes_not_matching_their_footer_sha256_is_reported(tmp_path, conn):
    store = _sealed_store(tmp_path, conn)

    def tampering_decompress(data):
        real = zstandard.ZstdDecompressor().decompress(data)
        if not real:
            return real
        return bytes([real[0] ^ 0xFF]) + real[1:]

    store.reader = SegmentReader(tmp_path / "segments", decompress=tampering_decompress)

    report = verify_archive(store)

    assert not report.ok
    assert any("does not match its bytes" in p for p in report.problems)


# --- missing/corrupt segment files ----------------------------------------


def test_missing_zst_file_for_a_sealed_segment_is_reported(tmp_path, conn):
    store = _sealed_store(tmp_path, conn)
    segment_id = store.sealed_segment_ids()[0]
    (tmp_path / "segments" / f"{segment_id:06d}.zst").unlink()

    report = verify_archive(store)

    assert not report.ok
    assert len(report.problems) == 1
    assert f"{segment_id:06d}" in report.problems[0]


def test_corrupt_trailer_on_a_sealed_segment_is_reported_not_raised(tmp_path, conn):
    store = _sealed_store(tmp_path, conn)
    segment_id = store.sealed_segment_ids()[0]
    path = tmp_path / "segments" / f"{segment_id:06d}.zst"
    path.write_bytes(b"too short to contain a trailer")

    report = verify_archive(store)

    assert not report.ok
