"""Tests for `fsck.py`: one test per invariant, plus chunking, no-writes,
and example-id-capping guarantees.
"""

from __future__ import annotations

import sqlite3
from typing import cast

import pytest

from reroll_sync.db import connect_reader, init_db
from reroll_sync.fsck import FsckReport, Violation, run
from reroll_sync.schema import WheelState
from reroll_sync.writer import ReadTxnWatchdog

# ---------------------------------------------------------------------------
# Fixtures/helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "fsck.db")
    init_db(path)
    return path


@pytest.fixture
def reader(db_path):
    conn = connect_reader(db_path)
    yield conn
    conn.close()


def _insert_wheel(
    conn: sqlite3.Connection,
    *,
    filename: str,
    project: str = "proj",
    state: WheelState = WheelState.NEED_METADATA,
    blob_sha256: str | None = None,
    metadata_sha256: str | None = None,
    conda_name: str | None = None,
    deleted_at: str | None = None,
    serial: int = 1,
    change_seq: int = 1,
    updated_at: str = "2024-01-01T00:00:00+00:00",
) -> int:
    cursor = conn.execute(
        "INSERT INTO wheels "
        "(filename, project, conda_name, state, lane, url, blob_sha256, metadata_sha256, "
        "serial, change_seq, deleted_at, updated_at) "
        "VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
        (
            filename,
            project,
            conda_name,
            int(state),
            f"https://example.test/{filename}",
            blob_sha256,
            metadata_sha256,
            serial,
            change_seq,
            deleted_at,
            updated_at,
        ),
    )
    conn.commit()
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _insert_skip(
    conn: sqlite3.Connection,
    *,
    wheel_id: int,
    stage: str = "convert",
    permanent: bool,
    reroll_version: str | None,
    reason: str = "reason",
) -> None:
    conn.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (wheel_id, stage, reason, int(permanent), reroll_version, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()


def _insert_work(
    conn: sqlite3.Connection,
    *,
    wheel_id: int,
    stage: str = "convert",
    attempts: int = 1,
    quarantined_at: str | None = None,
    next_attempt_at: str = "2024-01-01T00:00:00+00:00",
) -> None:
    conn.execute(
        "INSERT INTO work (wheel_id, stage, attempts, next_attempt_at, quarantined_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (wheel_id, stage, attempts, next_attempt_at, quarantined_at),
    )
    conn.commit()


def _insert_segment(conn: sqlite3.Connection, *, segment_id: int, sealed: bool = True) -> None:
    if sealed:
        conn.execute(
            "INSERT INTO segments (id, sealed_at, bytes, records, footer_sha) "
            "VALUES (?, '2024-01-01T00:00:00+00:00', 100, 1, 'x')",
            (segment_id,),
        )
    else:
        conn.execute("INSERT INTO segments (id) VALUES (?)", (segment_id,))
    conn.commit()


def _insert_blob(
    conn: sqlite3.Connection,
    *,
    sha256: str,
    segment_id: int,
    block_no: int = 0,
    offset: int = 0,
    length: int = 10,
) -> None:
    conn.execute(
        "INSERT INTO blobs (sha256, segment_id, block_no, offset, length) VALUES (?, ?, ?, ?, ?)",
        (sha256, segment_id, block_no, offset, length),
    )
    conn.commit()


def _violation(report: FsckReport, invariant: str) -> Violation | None:
    for v in report.violations:
        if v.invariant == invariant:
            return v
    return None


# ---------------------------------------------------------------------------
# Clean database
# ---------------------------------------------------------------------------


def test_a_clean_database_reports_nothing_and_is_ok(reader):
    report = run(reader)
    assert report.violations == ()
    assert report.ok is True


# ---------------------------------------------------------------------------
# State consistency
# ---------------------------------------------------------------------------


def test_invariant_1a_ready_without_repodata_row(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.close()

    report = run(reader)
    v = _violation(report, "1a_ready_without_repodata")
    assert v is not None
    assert v.count == 1
    assert v.example_ids == (wheel_id,)
    assert v.informational is False


def test_invariant_1b_repodata_row_without_ready_state(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT)
    conn.execute(
        "INSERT INTO wheel_repodata (wheel_id, repodata_zst, reroll_version) VALUES (?, ?, '1.0')",
        (wheel_id, b"x"),
    )
    conn.commit()
    conn.close()

    report = run(reader)
    v = _violation(report, "1b_repodata_without_ready")
    assert v is not None
    assert v.count == 1
    assert v.example_ids == (wheel_id,)


def test_invariant_2_need_convert_without_blob(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="a-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT, blob_sha256=None
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "2_need_convert_without_blob")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_2_need_convert_with_unresolved_blob(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        blob_sha256="deadbeef",
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "2_need_convert_without_blob")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_3_need_metadata_with_blob_set(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        state=WheelState.NEED_METADATA,
        blob_sha256="deadbeef",
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "3_need_metadata_with_blob")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_4_no_metadata_with_metadata_sha256_set(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        state=WheelState.NO_METADATA,
        metadata_sha256="deadbeef",
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "4_no_metadata_with_metadata_sha256")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_5_skipped_without_skips_row(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.SKIPPED)
    conn.close()

    report = run(reader)
    v = _violation(report, "5_skipped_without_skips_row")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_6_quarantined_without_work_row(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.QUARANTINED)
    conn.close()

    report = run(reader)
    v = _violation(report, "6_quarantined_without_work_row")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_6_quarantined_with_work_row_missing_quarantined_at(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.QUARANTINED)
    _insert_work(conn, wheel_id=wheel_id, quarantined_at=None)
    conn.close()

    report = run(reader)
    v = _violation(report, "6_quarantined_without_work_row")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_7a_deleted_without_deleted_at(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="a-1.0-py3-none-any.whl", state=WheelState.DELETED, deleted_at=None
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "7a_deleted_without_deleted_at")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_7b_deleted_at_without_deleted_state(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        state=WheelState.READY,
        deleted_at="2024-01-01T00:00:00+00:00",
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "7b_deleted_at_without_deleted_state")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_8_state_outside_wheelstate(db_path, reader):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO wheels "
        "(filename, project, state, lane, url, serial, change_seq, updated_at) "
        "VALUES "
        "('a-1.0-py3-none-any.whl', 'proj', 99, 0, 'https://x', 1, 1, '2024-01-01T00:00:00Z')"
    )
    conn.commit()
    (wheel_id,) = conn.execute(
        "SELECT id FROM wheels WHERE filename = 'a-1.0-py3-none-any.whl'"
    ).fetchone()
    conn.close()

    report = run(reader)
    v = _violation(report, "8_state_outside_wheelstate")
    assert v is not None
    assert v.example_ids == (wheel_id,)


# ---------------------------------------------------------------------------
# Skip attribution
# ---------------------------------------------------------------------------


def test_invariant_9a_permanent_skip_with_reroll_version_set(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.SKIPPED)
    _insert_skip(conn, wheel_id=wheel_id, permanent=True, reroll_version="1.0")
    conn.close()

    report = run(reader)
    v = _violation(report, "9a_permanent_skip_with_reroll_version")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_9b_non_permanent_skip_without_reroll_version(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.SKIPPED)
    _insert_skip(conn, wheel_id=wheel_id, permanent=False, reroll_version=None)
    conn.close()

    report = run(reader)
    v = _violation(report, "9b_non_permanent_skip_without_reroll_version")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_10_stale_skip_for_wheel_not_skipped(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT)
    _insert_skip(conn, wheel_id=wheel_id, permanent=True, reroll_version=None)
    conn.close()

    report = run(reader)
    v = _violation(report, "10_stale_skip")
    assert v is not None
    assert v.example_ids == (wheel_id,)


# ---------------------------------------------------------------------------
# Work table
# ---------------------------------------------------------------------------


def test_invariant_11_work_row_for_terminal_wheel(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.READY)
    _insert_work(conn, wheel_id=wheel_id)
    conn.close()

    report = run(reader)
    v = _violation(report, "11_work_row_for_terminal_wheel")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_11_work_row_for_quarantined_wheel_is_not_a_violation(db_path, reader):
    # QUARANTINED deliberately keeps a work row -- not "terminal" here.
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.QUARANTINED)
    _insert_work(conn, wheel_id=wheel_id, quarantined_at="2024-01-01T00:00:00+00:00")
    conn.close()

    report = run(reader)
    assert _violation(report, "11_work_row_for_terminal_wheel") is None


def test_invariant_12a_attempts_exceeds_max(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT)
    _insert_work(conn, wheel_id=wheel_id, attempts=9)
    conn.close()

    report = run(reader, max_attempts=8)
    v = _violation(report, "12a_attempts_exceeds_max")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_12b_max_attempts_without_quarantine(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT)
    _insert_work(conn, wheel_id=wheel_id, attempts=8, quarantined_at=None)
    conn.close()

    report = run(reader, max_attempts=8)
    v = _violation(report, "12b_max_attempts_without_quarantine")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_12b_max_attempts_with_quarantine_is_not_a_violation(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.QUARANTINED)
    _insert_work(conn, wheel_id=wheel_id, attempts=8, quarantined_at="2024-01-01T00:00:00+00:00")
    conn.close()

    report = run(reader, max_attempts=8)
    assert _violation(report, "12b_max_attempts_without_quarantine") is None


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------


def test_invariant_13_blob_sha256_unresolved(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        blob_sha256="deadbeef",
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "13_blob_sha256_unresolved")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_14_blob_segment_unresolved(db_path, reader):
    conn = sqlite3.connect(db_path)
    _insert_blob(conn, sha256="deadbeef", segment_id=42)
    conn.close()

    report = run(reader)
    v = _violation(report, "14_blob_segment_unresolved")
    assert v is not None
    assert v.example_ids == ("deadbeef",)


def test_invariant_15_sealed_segment_missing_from_disk(tmp_path, db_path):
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    store = ArchiveStore(tmp_path / "segments", conn)
    # A sealed row with no corresponding file on disk at all.
    conn.execute(
        "INSERT INTO segments (id, sealed_at, bytes, records, footer_sha) "
        "VALUES (7, '2024-01-01T00:00:00+00:00', 100, 1, 'x')"
    )
    conn.commit()

    reader_conn = connect_reader(db_path)
    try:
        report = run(reader_conn, archive_store=store)
        v = _violation(report, "15_sealed_segment_integrity")
        assert v is not None
        assert v.count >= 1
    finally:
        reader_conn.close()
        conn.close()


def test_invariant_15_example_ids_are_capped_at_the_configured_limit(tmp_path, db_path):
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    store = ArchiveStore(tmp_path / "segments", conn)
    # 30 sealed segment rows, none with a corresponding file on disk.
    for segment_id in range(30):
        conn.execute(
            "INSERT INTO segments (id, sealed_at, bytes, records, footer_sha) "
            "VALUES (?, '2024-01-01T00:00:00+00:00', 100, 1, 'x')",
            (segment_id,),
        )
    conn.commit()

    reader_conn = connect_reader(db_path)
    try:
        report = run(reader_conn, archive_store=store, example_limit=5)
        v = _violation(report, "15_sealed_segment_integrity")
        assert v is not None
        assert v.count == 30
        assert len(v.example_ids) == 5
    finally:
        reader_conn.close()
        conn.close()


def test_invariant_15_does_not_call_full_byte_level_verification(tmp_path, db_path, monkeypatch):
    """Invariant 15 must stay lightweight: it never decompresses/hashes every record.

    Wraps `SegmentReader.iter_records` -- the primitive a full,
    byte-level pass (`archive.verify.verify_archive`) uses to decompress
    and hash every record -- with a counter, and asserts it is never
    called even with many sealed segments present.
    """
    from reroll_sync.archive.reader import SegmentReader
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    store = ArchiveStore(tmp_path / "segments", conn)
    for _ in range(25):
        store.add(f"record for segment {_}".encode())
        store.seal_writer(store.current_writer())

    call_count = {"n": 0}
    original_iter_records = SegmentReader.iter_records

    def _counting_iter_records(self, segment_id):
        call_count["n"] += 1
        return original_iter_records(self, segment_id)

    monkeypatch.setattr(SegmentReader, "iter_records", _counting_iter_records)

    reader_conn = connect_reader(db_path)
    try:
        report = run(reader_conn, archive_store=store)
        assert _violation(report, "15_sealed_segment_integrity") is None
    finally:
        reader_conn.close()
        conn.close()

    assert call_count["n"] == 0


def test_invariant_15_performs_no_writes_via_the_archive_store_connection(
    tmp_path, db_path, reader
):
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    store = ArchiveStore(tmp_path / "segments", conn)
    store.add(b"metadata bytes")
    store.seal_writer(store.current_writer())

    class _NoWriteConn:
        """Wraps a real connection, raising on any mutating `execute`."""

        _MUTATING_PREFIXES = ("INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT", "ROLLBACK", "PRAGMA")

        def __init__(self, real: sqlite3.Connection) -> None:
            self._real = real

        def execute(self, sql: str, parameters=()):
            if sql.strip().upper().startswith(_NoWriteConn._MUTATING_PREFIXES):
                raise AssertionError(
                    f"fsck attempted a mutating statement via archive_store: {sql!r}"
                )
            return self._real.execute(sql, parameters)

        def __getattr__(self, name):
            return getattr(self._real, name)

    # Swap in the write-guarded connection only after setup writes are done.
    store._conn = cast(sqlite3.Connection, _NoWriteConn(conn))

    try:
        report = run(reader, archive_store=store)
        assert _violation(report, "15_sealed_segment_integrity") is None
    finally:
        conn.close()


def test_invariant_15_footer_record_missing_its_blobs_row(tmp_path, db_path):
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    store = ArchiveStore(tmp_path / "segments", conn)
    location = store.add(b"metadata bytes")
    store.seal_writer(store.current_writer())
    conn.execute("DELETE FROM blobs WHERE sha256 = ?", (location.sha256,))
    conn.commit()

    reader_conn = connect_reader(db_path)
    try:
        report = run(reader_conn, archive_store=store)
        v = _violation(report, "15_sealed_segment_integrity")
        assert v is not None
        assert v.count == 1
        example = v.example_ids[0]
        assert isinstance(example, str)
        assert "has no blobs row" in example
    finally:
        reader_conn.close()
        conn.close()


def test_invariant_15_blobs_row_position_disagrees_with_the_footer(tmp_path, db_path):
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    store = ArchiveStore(tmp_path / "segments", conn)
    location = store.add(b"metadata bytes")
    store.seal_writer(store.current_writer())
    conn.execute("UPDATE blobs SET offset = offset + 1 WHERE sha256 = ?", (location.sha256,))
    conn.commit()

    reader_conn = connect_reader(db_path)
    try:
        report = run(reader_conn, archive_store=store)
        v = _violation(report, "15_sealed_segment_integrity")
        assert v is not None
        assert v.count == 1
        example = v.example_ids[0]
        assert isinstance(example, str)
        assert "footer says" in example
    finally:
        reader_conn.close()
        conn.close()


def test_invariant_15_blobs_row_has_no_matching_footer_record(tmp_path, db_path):
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    store = ArchiveStore(tmp_path / "segments", conn)
    location = store.add(b"metadata bytes")
    store.seal_writer(store.current_writer())
    _insert_blob(conn, sha256="0" * 64, segment_id=location.segment_id)

    reader_conn = connect_reader(db_path)
    try:
        report = run(reader_conn, archive_store=store)
        v = _violation(report, "15_sealed_segment_integrity")
        assert v is not None
        assert v.count == 1
        example = v.example_ids[0]
        assert isinstance(example, str)
        assert "has no matching footer record" in example
    finally:
        reader_conn.close()
        conn.close()


def test_invariant_16_orphaned_blob_is_informational(db_path, reader):
    conn = sqlite3.connect(db_path)
    _insert_segment(conn, segment_id=1, sealed=True)
    _insert_blob(conn, sha256="deadbeef", segment_id=1)
    conn.close()

    report = run(reader)
    v = _violation(report, "16_orphaned_blob")
    assert v is not None
    assert v.count == 1
    assert v.example_ids == ("deadbeef",)
    assert v.informational is True
    assert report.ok is True  # informational-only findings exit zero


# ---------------------------------------------------------------------------
# Sequences
# ---------------------------------------------------------------------------


def test_invariant_17_duplicate_change_seq_with_differing_updated_at(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id_1 = _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        change_seq=5,
        updated_at="2024-01-01T00:00:00+00:00",
    )
    wheel_id_2 = _insert_wheel(
        conn,
        filename="b-1.0-py3-none-any.whl",
        change_seq=5,
        updated_at="2024-01-02T00:00:00+00:00",
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "17_duplicate_change_seq")
    assert v is not None
    assert v.count == 2
    assert set(v.example_ids) == {wheel_id_1, wheel_id_2}


def test_invariant_17_duplicate_change_seq_with_same_updated_at_is_not_a_violation(db_path, reader):
    conn = sqlite3.connect(db_path)
    _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        change_seq=5,
        updated_at="2024-01-01T00:00:00+00:00",
    )
    _insert_wheel(
        conn,
        filename="b-1.0-py3-none-any.whl",
        change_seq=5,
        updated_at="2024-01-01T00:00:00+00:00",
    )
    conn.close()

    report = run(reader)
    assert _violation(report, "17_duplicate_change_seq") is None


def test_invariant_17_window_first_query_plan_uses_ix_wheels_change_seq(db_path, reader):
    from reroll_sync.fsck import _duplicate_change_seq_window_first_query

    sql = _duplicate_change_seq_window_first_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", (10,)).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_change_seq" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


def test_invariant_17_window_next_query_plan_uses_ix_wheels_change_seq(db_path, reader):
    from reroll_sync.fsck import _duplicate_change_seq_window_next_query

    sql = _duplicate_change_seq_window_next_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", (5, 10)).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_change_seq" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


def test_invariant_17_aggregate_query_plan_uses_ix_wheels_change_seq(db_path, reader):
    from reroll_sync.fsck import _duplicate_change_seq_aggregate_query

    sql = _duplicate_change_seq_aggregate_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", (1, 100)).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_change_seq" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


def test_invariant_17_pages_through_multiple_read_txns_for_a_large_duplicate_free_table(
    db_path, reader
):
    from reroll_sync.fsck import _check_duplicate_change_seq

    conn = sqlite3.connect(db_path)
    for i in range(2500):
        _insert_wheel(conn, filename=f"pkg-{i}-1.0-py3-none-any.whl", change_seq=i)
    conn.close()

    call_count = {"n": 0}

    class _CountingWatchdog(ReadTxnWatchdog):
        def record(self, duration_ms: float, over_budget: bool) -> None:
            call_count["n"] += 1
            super().record(duration_ms, over_budget)

    watchdog = _CountingWatchdog()
    violation = _check_duplicate_change_seq(
        reader, chunk_size=500, example_limit=20, budget=1.0, watchdog=watchdog
    )

    assert violation is None
    # 2500 rows / 500 per window == 5 windows == 5 read_txns: never one
    # unbounded transaction over the whole table.
    assert call_count["n"] == 5


def test_invariant_18_writer_change_seq_matches_is_not_a_violation(db_path, reader):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", change_seq=5)
    conn.close()

    report = run(reader, writer_change_seq=5)
    assert _violation(report, "18_writer_change_seq_mismatch") is None


def test_invariant_18_writer_change_seq_mismatch(db_path, reader):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", change_seq=5)
    conn.close()

    report = run(reader, writer_change_seq=6)
    v = _violation(report, "18_writer_change_seq_mismatch")
    assert v is not None
    assert v.count == 1
    example = v.example_ids[0]
    assert isinstance(example, str)
    assert "db_max=5" in example
    assert "writer=6" in example


def test_invariant_18_is_skipped_without_writer_change_seq(db_path, reader):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", change_seq=5)
    conn.close()

    report = run(reader)  # writer_change_seq not supplied
    assert _violation(report, "18_writer_change_seq_mismatch") is None


# ---------------------------------------------------------------------------
# Cross-cutting
# ---------------------------------------------------------------------------


def test_invariant_19_conda_name_set_outside_ready(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        conda_name="numpy",
    )
    conn.close()

    report = run(reader)
    v = _violation(report, "19_conda_name_outside_ready")
    assert v is not None
    assert v.example_ids == (wheel_id,)


def test_invariant_20_tombstoned_wheel_with_repodata_is_informational(db_path, reader):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        state=WheelState.DELETED,
        deleted_at="2024-01-01T00:00:00+00:00",
    )
    conn.execute(
        "INSERT INTO wheel_repodata (wheel_id, repodata_zst, reroll_version) VALUES (?, ?, '1.0')",
        (wheel_id, b"x"),
    )
    conn.commit()
    conn.close()

    report = run(reader)
    v = _violation(report, "20_tombstoned_with_repodata")
    assert v is not None
    assert v.example_ids == (wheel_id,)
    assert v.informational is True
    # Note: this same row also violates invariant 1b (repodata exists but
    # state != READY), which is not informational -- so report.ok is False
    # here for a real reason unrelated to invariant 20 itself.


# ---------------------------------------------------------------------------
# General guarantees
# ---------------------------------------------------------------------------


def test_a_single_violation_among_ten_thousand_clean_rows_is_found(db_path, reader):
    conn = sqlite3.connect(db_path)
    for i in range(10_000):
        _insert_wheel(
            conn, filename=f"pkg-{i}-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA
        )
    bad_id = _insert_wheel(conn, filename="bad-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.close()

    report = run(reader, chunk_size=500)
    v = _violation(report, "1a_ready_without_repodata")
    assert v is not None
    assert v.count == 1
    assert v.example_ids == (bad_id,)


def test_scans_are_chunked_into_bounded_read_transactions(db_path, reader):
    from reroll_sync.fsck import _chunked_scan

    conn = sqlite3.connect(db_path)
    for i in range(1000):
        _insert_wheel(conn, filename=f"pkg-{i}-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.close()

    call_count = {"n": 0}

    class _CountingWatchdog(ReadTxnWatchdog):
        def record(self, duration_ms: float, over_budget: bool) -> None:
            call_count["n"] += 1
            super().record(duration_ms, over_budget)

    watchdog = _CountingWatchdog()
    base_query = "SELECT id AS value, rowid AS cursor_rowid FROM wheels WHERE state = ?"
    results = list(
        _chunked_scan(
            reader,
            base_query,
            [int(WheelState.READY)],
            chunk_size=100,
            budget=1.0,
            watchdog=watchdog,
            label="test",
        )
    )

    assert len(results) == 1000
    assert call_count["n"] == 10
    assert watchdog.snapshot().over_budget_count == 0


def test_example_ids_are_capped_at_the_configured_limit(db_path, reader):
    conn = sqlite3.connect(db_path)
    for i in range(50):
        _insert_wheel(conn, filename=f"pkg-{i}-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.close()

    report = run(reader, example_limit=5)
    v = _violation(report, "1a_ready_without_repodata")
    assert v is not None
    assert v.count == 50
    assert len(v.example_ids) == 5


def test_fsck_performs_no_writes(db_path, reader):
    class _NoWriteConn:
        """Wraps a real connection, raising on any mutating `execute`."""

        _MUTATING_PREFIXES = ("INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT", "ROLLBACK", "PRAGMA")

        def __init__(self, real: sqlite3.Connection) -> None:
            self._real = real

        def execute(self, sql: str, parameters=()):
            if sql.strip().upper().startswith(_NoWriteConn._MUTATING_PREFIXES):
                raise AssertionError(f"fsck attempted a mutating statement: {sql!r}")
            return self._real.execute(sql, parameters)

        def __getattr__(self, name):
            return getattr(self._real, name)

    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.close()

    guarded = cast(sqlite3.Connection, _NoWriteConn(reader))
    report = run(guarded, writer_change_seq=1)
    assert any(v.invariant == "1a_ready_without_repodata" for v in report.violations)


def test_invariant_15_is_not_a_violation_for_a_clean_sealed_archive(tmp_path, db_path):
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    store = ArchiveStore(tmp_path / "segments", conn)
    store.add(b"metadata bytes")
    store.seal_writer(store.current_writer())

    reader_conn = connect_reader(db_path)
    try:
        report = run(reader_conn, archive_store=store)
        assert _violation(report, "15_sealed_segment_integrity") is None
    finally:
        reader_conn.close()
        conn.close()


def test_invariant_17_example_ids_are_capped_across_multiple_groups(db_path, reader):
    conn = sqlite3.connect(db_path)
    ids = []
    # Two separate duplicate-change_seq groups, each with differing
    # updated_at, together contributing more member ids than example_limit.
    for group, seq in enumerate((10, 20)):
        for i in range(3):
            ids.append(
                _insert_wheel(
                    conn,
                    filename=f"pkg-{group}-{i}-1.0-py3-none-any.whl",
                    change_seq=seq,
                    updated_at=f"2024-01-0{i + 1}T00:00:00+00:00",
                )
            )
    conn.close()

    report = run(reader, example_limit=2)
    v = _violation(report, "17_duplicate_change_seq")
    assert v is not None
    assert v.count == 6
    assert len(v.example_ids) == 2
