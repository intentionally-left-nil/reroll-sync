"""Tests for daemon.unquarantine: the raw WriteOp campaign that clears
`work.quarantined_at` and requeues QUARANTINED wheels, since dispatcher.py
exposes no such campaign itself (only `reprocess`, for non-quarantined
selectors).
"""

from __future__ import annotations

import sqlite3

import pytest

from reroll_sync.daemon.unquarantine import UnsupportedSelectorError, unquarantine
from reroll_sync.db import init_db
from reroll_sync.dispatcher import ProjectSelector, RerollVersionBelow, SkippedOnly, StateSelector
from reroll_sync.schema import WheelState
from reroll_sync.writer import Writer


def _writer_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 50")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "unquarantine.db")
    init_db(path)
    return path


@pytest.fixture
def writer(db_path):
    conn = _writer_conn(db_path)
    w = Writer(conn, batch_size=1, batch_interval=1_000_000.0)
    w.start()
    yield w
    if not w._stopped:
        w.stop(drain=False)


@pytest.fixture
def reader(db_path):
    conn = sqlite3.connect(str(db_path))
    yield conn
    conn.close()


def _insert_wheel(
    db_path: str,
    *,
    filename: str,
    project: str = "widget",
    state: WheelState = WheelState.QUARANTINED,
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, serial, change_seq, updated_at) "
            "VALUES (?, ?, ?, 0, 'https://example.test/x', 1, 1, '2024-01-01T00:00:00+00:00')",
            (filename, project, int(state)),
        )
        conn.commit()
        assert cursor.lastrowid is not None
        return cursor.lastrowid
    finally:
        conn.close()


def _insert_work(
    db_path: str, wheel_id: int, *, stage: str = "fetch", quarantined: bool = True
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO work (wheel_id, stage, attempts, next_attempt_at, quarantined_at) "
            "VALUES (?, ?, 9, '2024-01-01T00:00:00+00:00', ?)",
            (wheel_id, stage, "2024-01-01T00:00:00+00:00" if quarantined else None),
        )
        conn.commit()
    finally:
        conn.close()


def test_unquarantine_all_resets_state_and_clears_work_row(db_path, reader, writer):
    wheel_id = _insert_wheel(db_path, filename="a-1.0-py3-none-any.whl")
    _insert_work(db_path, wheel_id, stage="fetch")

    affected = unquarantine(reader, writer, StateSelector(state=WheelState.QUARANTINED))

    assert affected == 1
    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.NEED_METADATA)
    work_row = reader.execute("SELECT 1 FROM work WHERE wheel_id = ?", (wheel_id,)).fetchone()
    assert work_row is None


def test_unquarantine_clears_work_rows_from_every_stage(db_path, reader, writer):
    """A wheel quarantined via convert must not leave a stale 'convert' work
    row with quarantined_at set behind -- that would silently block it from
    ever being claimed for convert again once it comes back around.
    """
    wheel_id = _insert_wheel(db_path, filename="a-1.0-py3-none-any.whl")
    _insert_work(db_path, wheel_id, stage="convert")

    unquarantine(reader, writer, StateSelector(state=WheelState.QUARANTINED))

    work_row = reader.execute("SELECT 1 FROM work WHERE wheel_id = ?", (wheel_id,)).fetchone()
    assert work_row is None


def test_unquarantine_by_project_only_affects_that_project(db_path, reader, writer):
    wheel_1 = _insert_wheel(db_path, filename="a-1.0-py3-none-any.whl", project="proj-a")
    wheel_2 = _insert_wheel(db_path, filename="b-1.0-py3-none-any.whl", project="proj-b")

    affected = unquarantine(reader, writer, ProjectSelector(project="proj-a"))

    assert affected == 1
    row1 = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_1,)).fetchone()
    assert row1[0] == int(WheelState.NEED_METADATA)
    row2 = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_2,)).fetchone()
    assert row2[0] == int(WheelState.QUARANTINED)


def test_unquarantine_never_touches_a_non_quarantined_wheel(db_path, reader, writer):
    wheel_id = _insert_wheel(
        db_path, filename="a-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT
    )

    affected = unquarantine(reader, writer, StateSelector(state=WheelState.QUARANTINED))

    assert affected == 0
    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.NEED_CONVERT)


def test_unquarantine_with_no_quarantined_wheels_is_a_noop(db_path, reader, writer):
    affected = unquarantine(reader, writer, StateSelector(state=WheelState.QUARANTINED))
    assert affected == 0


def test_unquarantine_is_chunked(db_path, reader, writer):
    for i in range(5):
        _insert_wheel(db_path, filename=f"chunked-{i}-1.0-py3-none-any.whl")

    calls = []
    real_submit_and_wait = writer.submit_and_wait

    def _counting_submit_and_wait(op):
        calls.append(op)
        return real_submit_and_wait(op)

    writer.submit_and_wait = _counting_submit_and_wait

    affected = unquarantine(
        reader, writer, StateSelector(state=WheelState.QUARANTINED), chunk_size=2
    )

    assert affected == 5
    assert len(calls) == 3  # ceil(5 / 2)


def test_unquarantine_with_state_selector_for_a_different_state_matches_nothing(
    db_path, reader, writer
):
    wheel_id = _insert_wheel(db_path, filename="a-1.0-py3-none-any.whl")

    affected = unquarantine(reader, writer, StateSelector(state=WheelState.SKIPPED))

    assert affected == 0
    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.QUARANTINED)


def test_unquarantine_rejects_reroll_version_below_selector(db_path, reader, writer):
    with pytest.raises(UnsupportedSelectorError):
        unquarantine(reader, writer, RerollVersionBelow(version="1.0"))


def test_unquarantine_rejects_skipped_only_selector(db_path, reader, writer):
    with pytest.raises(UnsupportedSelectorError):
        unquarantine(reader, writer, SkippedOnly())


def test_unquarantine_bumps_change_seq(db_path, reader, writer):
    wheel_id = _insert_wheel(db_path, filename="a-1.0-py3-none-any.whl")
    before = writer.current_seq()

    unquarantine(reader, writer, StateSelector(state=WheelState.QUARANTINED))

    row = reader.execute("SELECT change_seq FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] > before
