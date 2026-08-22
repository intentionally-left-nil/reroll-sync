"""Tests for the gc stage: bounded deletion of old `errors` rows."""

from __future__ import annotations

import sqlite3

import pytest

from reroll_sync.daemon.stages.gc import GcStage
from reroll_sync.db import init_db
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
    path = str(tmp_path / "gc.db")
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


def _insert_error(db_path: str, *, created_at: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO errors (error_category, reroll_version, created_at) "
            "VALUES ('x', '1.0', ?)",
            (created_at,),
        )
        conn.commit()
    finally:
        conn.close()


def _count_errors(reader) -> int:
    (count,) = reader.execute("SELECT COUNT(*) FROM errors").fetchone()
    return count


def test_deletes_errors_older_than_retention(db_path, reader, writer):
    _insert_error(db_path, created_at="2020-01-01T00:00:00+00:00")
    _insert_error(db_path, created_at="2020-01-01T00:00:00+00:00")
    stage = GcStage(writer, retention_days=30, now=lambda: 1_700_000_000.0)

    did_work = stage.iterate()

    assert did_work is True
    assert _count_errors(reader) == 0


def test_keeps_errors_within_retention(db_path, reader, writer):
    now = 1_700_000_000.0
    recent_iso = "2023-11-14T22:13:20+00:00"  # a few seconds before `now`
    _insert_error(db_path, created_at=recent_iso)
    stage = GcStage(writer, retention_days=30, now=lambda: now)

    did_work = stage.iterate()

    assert did_work is False
    assert _count_errors(reader) == 1


def test_noop_when_no_old_errors_returns_false(db_path, reader, writer):
    stage = GcStage(writer, retention_days=30, now=lambda: 1_700_000_000.0)
    assert stage.iterate() is False


def test_is_chunked_across_multiple_iterations(db_path, reader, writer):
    for _ in range(5):
        _insert_error(db_path, created_at="2020-01-01T00:00:00+00:00")
    stage = GcStage(writer, retention_days=30, now=lambda: 1_700_000_000.0, chunk_size=2)

    assert stage.iterate() is True
    assert _count_errors(reader) == 3
    assert stage.iterate() is True
    assert _count_errors(reader) == 1
    assert stage.iterate() is True
    assert _count_errors(reader) == 0
    assert stage.iterate() is False
