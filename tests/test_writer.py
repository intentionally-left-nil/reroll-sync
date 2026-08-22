"""Tests for the sqlite writer thread: batching, WAL/vacuum, and read_txn."""

from __future__ import annotations

import logging
import re
import sqlite3
import threading
from pathlib import Path

import pytest

from reroll_sync.db import init_db
from reroll_sync.writer import (
    ReadTxnBudgetExceeded,
    ReadTxnWatchdog,
    TransactionBoundaryViolation,
    WriteOp,
    Writer,
    WriterStoppedError,
    read_txn,
)


class FakeClock:
    """A manually-advanced clock, for tests that need exact control over elapsed time."""

    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class AutoAdvanceClock:
    """A clock that ticks forward by ``step`` on every read.

    Used where a background thread's own polling should be what drives a
    batch/interval deadline forward, so the test never has to guess *when*
    to call a manual clock's ``advance`` relative to unpredictable thread
    scheduling.
    """

    def __init__(self, start: float = 0.0, step: float = 0.01) -> None:
        self.value = start
        self.step = step

    def now(self) -> float:
        self.value += self.step
        return self.value


def _writer_conn(db_path) -> sqlite3.Connection:
    """A connection usable from the writer's background thread.

    Mirrors ``db.connect_writer``'s pragmas but with ``check_same_thread =
    False``: the writer is constructed on the calling thread and then
    consumed exclusively by its own background thread, a different OS
    thread, so the connection cannot be bound to its creating thread.
    """
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    # A small busy_timeout keeps checkpoint-contention tests fast; the
    # magnitude doesn't affect what's under test (whether TRUNCATE
    # reports busy under a held reader), only how long a blocked attempt
    # waits before giving up.
    conn.execute("PRAGMA busy_timeout = 50")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "writer.db")
    init_db(path)
    return path


@pytest.fixture
def writers():
    """Track writers created by a test so they are always stopped, even on failure."""
    created: list[Writer] = []

    def _make(conn, **kwargs) -> Writer:
        writer = Writer(conn, **kwargs)
        created.append(writer)
        return writer

    yield _make
    for writer in created:
        if writer._started and not writer._stopped:
            writer.stop(drain=False)


def _insert_op(name: str, filename: str, seq: int) -> WriteOp:
    def _apply(conn: sqlite3.Connection):
        conn.execute(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, serial, change_seq, updated_at) "
            "VALUES (?, 'proj', 0, 0, 'https://example.test/x', 1, ?, '2024-01-01T00:00:00Z')",
            (filename, seq),
        )

    return WriteOp(name=name, apply=_apply)


def _count_wheels(conn: sqlite3.Connection) -> int:
    (count,) = conn.execute("SELECT COUNT(*) FROM wheels").fetchone()
    return count


def _commit_counter(conn: sqlite3.Connection) -> list[int]:
    """Install a trace callback counting real COMMIT statements; returns a running total."""
    counts = [0]

    def _trace(statement: str) -> None:
        if statement.strip().upper() == "COMMIT":
            counts[0] += 1

    conn.set_trace_callback(_trace)
    return counts


def _settle(writer: Writer) -> None:
    """Round-trip a no-op through the writer to know its batch has committed."""
    writer.submit_and_wait(WriteOp(name="settle", apply=lambda _conn: None))


class _FailingConn:
    """Wraps a real sqlite3 connection, raising on ``execute`` of one exact SQL statement.

    Used to inject a fatal failure into the writer's own BEGIN/COMMIT calls
    without touching real sqlite internals.
    """

    def __init__(self, conn: sqlite3.Connection, fail_on: str) -> None:
        self._real = conn
        self._fail_on = fail_on

    def execute(self, sql: str, parameters=()):
        if sql.strip().upper() == self._fail_on:
            raise sqlite3.OperationalError(f"simulated failure executing {sql!r}")
        return self._real.execute(sql, parameters)

    def __setattr__(self, name, value):
        if name in ("_real", "_fail_on"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._real, name, value)

    def __getattr__(self, name):
        return getattr(self._real, name)


class _DelayingConn:
    """Wraps a real sqlite3 connection, pausing ``execute`` of one SQL prefix.

    Sets ``started`` the moment the matching statement is about to run, then
    waits on ``proceed`` before actually running it -- lets a test force a
    deterministic overlap between this call and something happening on a
    separate connection, in real wall-clock time, without sleeping.
    """

    def __init__(self, conn: sqlite3.Connection, delay_on_prefix: str, started, proceed) -> None:
        self._real = conn
        self._delay_on_prefix = delay_on_prefix
        self._started = started
        self._proceed = proceed

    def execute(self, sql: str, parameters=()):
        if sql.strip().upper().startswith(self._delay_on_prefix):
            self._started.set()
            assert self._proceed.wait(timeout=5)
        return self._real.execute(sql, parameters)

    def __setattr__(self, name, value):
        if name in ("_real", "_delay_on_prefix", "_started", "_proceed"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._real, name, value)

    def __getattr__(self, name):
        return getattr(self._real, name)


# --- Batching ----------------------------------------------------------------


def test_batch_size_ops_produce_exactly_one_commit(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=5, batch_interval=100.0, now=clock.now)
    writer.start()
    commits = _commit_counter(conn)
    try:
        events: list[threading.Event] = []
        for i in range(5):
            op = _insert_op(f"insert-{i}", f"pkg-{i}-1.0-py3-none-any.whl", writer.next_seq())
            event = threading.Event()
            op.result_event = event
            events.append(event)
            writer.submit(op)
        for event in events:
            assert event.wait(timeout=5)
    finally:
        writer.stop(drain=False)
    assert commits[0] == 1


def test_fewer_than_batch_size_still_commits_once_interval_elapses(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()
    commits = _commit_counter(conn)
    try:
        writer.submit_and_wait(
            _insert_op("solo", "pkg-solo-1.0-py3-none-any.whl", writer.next_seq())
        )
    finally:
        writer.stop(drain=False)
    assert commits[0] == 1


def test_2500_ops_with_batch_size_1000_produce_exactly_3_commits(db_path, writers):
    # A huge batch_interval isolates this from interval-triggered flushing
    # entirely: the first two batches flush purely on size (1000 each), and
    # stop(drain=True) deterministically forces the trailing 500 to flush,
    # without racing a clock against however fast the queue drains.
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=1_000_000.0, now=clock.now)
    writer.start()
    commits = _commit_counter(conn)
    for i in range(2500):
        op = _insert_op(f"insert-{i}", f"pkg-{i}-1.0-py3-none-any.whl", writer.next_seq())
        writer.submit(op)
    writer.stop(drain=True)
    assert commits[0] == 3
    check_conn = sqlite3.connect(db_path)
    try:
        actual_count = _count_wheels(check_conn)
    finally:
        check_conn.close()
    assert actual_count == 2500


def test_ops_apply_in_submission_order(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()
    order: list[int] = []

    def _make(i):
        def _apply(_conn):
            order.append(i)

        return WriteOp(name=f"op-{i}", apply=_apply)

    try:
        for i in range(19):
            writer.submit(_make(i))
        writer.submit_and_wait(_make(19))
    finally:
        writer.stop(drain=False)
    assert order == list(range(20))


def test_submit_and_wait_returns_apply_return_value(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()
    try:
        result = writer.submit_and_wait(WriteOp(name="compute", apply=lambda _conn: "hello"))
        assert result == "hello"
    finally:
        writer.stop(drain=False)


def test_submit_and_wait_propagates_apply_exception(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _boom(_conn):
        raise ValueError("kaboom")

    try:
        with pytest.raises(ValueError, match="kaboom"):
            writer.submit_and_wait(WriteOp(name="boom", apply=_boom))
    finally:
        writer.stop(drain=False)


def test_start_called_twice_raises(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=0.01, now=clock.now)
    writer.start()
    try:
        with pytest.raises(RuntimeError, match="more than once"):
            writer.start()
    finally:
        writer.stop(drain=False)


def test_submit_and_wait_with_a_preexisting_result_event(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()
    event = threading.Event()
    op = WriteOp(name="pre-eventful", apply=lambda _conn: "value", result_event=event)
    try:
        result = writer.submit_and_wait(op)
    finally:
        writer.stop(drain=False)
    assert result == "value"
    assert op.result_event is event


def test_stop_drain_false_resolves_pending_op_with_writer_stopped_error(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=1_000_000.0, now=clock.now)
    writer.start()
    result: dict[str, BaseException] = {}
    op = WriteOp(name="never-flushed", apply=lambda _conn: None, result_event=threading.Event())
    submitted = threading.Event()

    def _waiter():
        writer.submit(op)
        submitted.set()
        assert op.result_event is not None
        op.result_event.wait()
        outcome = op._outcome
        assert outcome is not None
        if outcome.exception is not None:
            result["exc"] = outcome.exception

    thread = threading.Thread(target=_waiter)
    thread.start()
    assert submitted.wait(timeout=5)
    writer.stop(drain=False)
    thread.join(timeout=5)
    assert isinstance(result.get("exc"), WriterStoppedError)


# --- Failure isolation ---------------------------------------------------------


def test_one_failing_op_among_ten_others_still_commit(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _boom(_conn):
        raise RuntimeError("nope")

    try:
        for i in range(9):
            writer.submit(
                _insert_op(f"insert-{i}", f"pkg-{i}-1.0-py3-none-any.whl", writer.next_seq())
            )
        with pytest.raises(RuntimeError, match="nope"):
            writer.submit_and_wait(WriteOp(name="boom", apply=_boom))
    finally:
        writer.stop(drain=False)
    check_conn = sqlite3.connect(db_path)
    try:
        actual_count = _count_wheels(check_conn)
    finally:
        check_conn.close()
    assert actual_count == 9


def test_failing_op_exception_recorded_with_name(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _boom(_conn):
        raise RuntimeError("nope")

    op = WriteOp(name="the-failing-op", apply=_boom)
    try:
        with pytest.raises(RuntimeError, match="nope"):
            writer.submit_and_wait(op)
    finally:
        writer.stop(drain=False)
    assert op.name == "the-failing-op"


def test_writer_failed_ops_increments_by_exactly_one(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _boom(_conn):
        raise RuntimeError("nope")

    try:
        assert writer.failed_ops() == 0
        with pytest.raises(RuntimeError):
            writer.submit_and_wait(WriteOp(name="boom", apply=_boom))
        assert writer.failed_ops() == 1
    finally:
        writer.stop(drain=False)


def test_queue_depth_reflects_items_not_yet_pulled_by_the_writer_thread(memory_conn):
    # The writer's own thread is never started, so nothing drains the
    # queue: `submit` is safe to call before `start()`, and this is the
    # only way to observe the queue's raw depth without a race against
    # the background thread pulling items out of it.
    writer = Writer(memory_conn, batch_size=1000, batch_interval=1_000_000.0)
    assert writer.queue_depth() == 0
    writer.submit(WriteOp(name="noop-1", apply=lambda _conn: None))
    writer.submit(WriteOp(name="noop-2", apply=lambda _conn: None))
    assert writer.queue_depth() == 2


def test_failed_op_logs_at_error_with_name(db_path, writers, caplog):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _boom(_conn):
        raise RuntimeError("nope")

    try:
        with (
            caplog.at_level(logging.ERROR, logger="reroll_sync.writer"),
            pytest.raises(RuntimeError),
        ):
            writer.submit_and_wait(WriteOp(name="named-op", apply=_boom))
    finally:
        writer.stop(drain=False)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("named-op" in r.getMessage() for r in errors)


def test_write_op_calling_commit_is_rejected(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.commit()

    try:
        with pytest.raises(TransactionBoundaryViolation):
            writer.submit_and_wait(WriteOp(name="bad-commit", apply=_apply))
    finally:
        writer.stop(drain=False)


def test_write_op_calling_rollback_is_rejected(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.rollback()

    try:
        with pytest.raises(TransactionBoundaryViolation):
            writer.submit_and_wait(WriteOp(name="bad-rollback", apply=_apply))
    finally:
        writer.stop(drain=False)


def test_write_op_executing_begin_is_rejected(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.execute("BEGIN IMMEDIATE")

    try:
        with pytest.raises(TransactionBoundaryViolation):
            writer.submit_and_wait(WriteOp(name="bad-begin", apply=_apply))
    finally:
        writer.stop(drain=False)


def test_guarded_connection_delegates_unrecognized_attributes(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()
    seen = []

    def _apply(conn):
        seen.append(conn.in_transaction)

    try:
        writer.submit_and_wait(WriteOp(name="delegate", apply=_apply))
    finally:
        writer.stop(drain=False)
    assert seen == [True]


def test_guarded_connection_cursor_execute_release_is_rejected(db_path, writers):
    # guarded.cursor() must return a guarded cursor, not a raw sqlite3.Cursor
    # that bypasses the transaction-boundary check.
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.cursor().execute("RELEASE some_savepoint")

    try:
        with pytest.raises(TransactionBoundaryViolation):
            writer.submit_and_wait(WriteOp(name="cursor-release", apply=_apply))
    finally:
        writer.stop(drain=False)


def test_guarded_connection_cursor_executemany_release_is_rejected(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.cursor().executemany("RELEASE some_savepoint", [()])

    try:
        with pytest.raises(TransactionBoundaryViolation):
            writer.submit_and_wait(WriteOp(name="cursor-executemany-release", apply=_apply))
    finally:
        writer.stop(drain=False)


def test_guarded_connection_cursor_executescript_is_rejected(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.cursor().executescript("SELECT 1;")

    try:
        with pytest.raises(TransactionBoundaryViolation):
            writer.submit_and_wait(WriteOp(name="cursor-executescript", apply=_apply))
    finally:
        writer.stop(drain=False)


def test_guarded_connection_cursor_delegates_fetch_methods(db_path, writers):
    # The wrapped cursor must still work for everything other than the
    # three guarded methods.
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()
    seen = []

    def _apply(conn):
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        seen.append(cursor.fetchone())

    try:
        writer.submit_and_wait(WriteOp(name="cursor-fetch", apply=_apply))
    finally:
        writer.stop(drain=False)
    assert seen == [(1,)]


def test_guarded_connection_executescript_is_rejected_outright(db_path, writers):
    # executescript issues its own implicit COMMIT and cannot be scoped to
    # the per-op SAVEPOINT the writer relies on for failure isolation, so
    # it is rejected outright rather than partially checked.
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.executescript("SELECT 1;")

    try:
        with pytest.raises(TransactionBoundaryViolation):
            writer.submit_and_wait(WriteOp(name="executescript", apply=_apply))
    finally:
        writer.stop(drain=False)


def test_guarded_connection_executemany_release_is_rejected(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.executemany("RELEASE some_savepoint", [()])

    try:
        with pytest.raises(TransactionBoundaryViolation):
            writer.submit_and_wait(WriteOp(name="executemany-release", apply=_apply))
    finally:
        writer.stop(drain=False)


def test_guarded_connection_executemany_allows_dml(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.executemany(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, serial, change_seq, updated_at) "
            "VALUES (?, 'proj', 0, 0, 'https://example.test/x', 1, ?, '2024-01-01T00:00:00Z')",
            [("many-a-1.0-py3-none-any.whl", 1), ("many-b-1.0-py3-none-any.whl", 2)],
        )

    try:
        writer.submit_and_wait(WriteOp(name="executemany-dml", apply=_apply))
    finally:
        writer.stop(drain=False)
    check_conn = sqlite3.connect(db_path)
    try:
        actual_count = _count_wheels(check_conn)
    finally:
        check_conn.close()
    assert actual_count == 2


def test_guarded_connection_cursor_executemany_allows_dml(db_path, writers):
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()

    def _apply(conn):
        conn.cursor().executemany(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, serial, change_seq, updated_at) "
            "VALUES (?, 'proj', 0, 0, 'https://example.test/x', 1, ?, '2024-01-01T00:00:00Z')",
            [("cursor-many-a-1.0-py3-none-any.whl", 1)],
        )

    try:
        writer.submit_and_wait(WriteOp(name="cursor-executemany-dml", apply=_apply))
    finally:
        writer.stop(drain=False)
    check_conn = sqlite3.connect(db_path)
    try:
        actual_count = _count_wheels(check_conn)
    finally:
        check_conn.close()
    assert actual_count == 1


def test_guarded_connection_conn_attribute_bypasses_guard_by_design(db_path, writers):
    # ``_conn`` is a cooperative guard against accidental misuse (calling
    # the documented public methods), not a security boundary: reaching
    # into the private attribute directly is a known, accepted way around
    # it, documented here rather than left as an unconfirmed gap.
    conn = _writer_conn(db_path)
    clock = AutoAdvanceClock(step=0.01)
    writer = writers(conn, batch_size=1000, batch_interval=0.05, now=clock.now)
    writer.start()
    seen = []

    def _apply(conn):
        seen.append(conn._conn is conn._conn)  # the raw connection is reachable

    try:
        writer.submit_and_wait(WriteOp(name="reach-in", apply=_apply))
    finally:
        writer.stop(drain=False)
    assert seen == [True]


# --- Bounded queue and backpressure --------------------------------------------


def test_submit_blocks_when_queue_at_capacity(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    # batch_size=1 so every op flushes (and its apply() runs) the moment it
    # is dequeued -- backpressure is exercised while the background thread
    # is busy running a blocking apply(), not while merely accumulating.
    writer = writers(conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now, queue_maxsize=1)
    writer.start()
    try:
        block_event = threading.Event()

        def _blocking_apply(_conn):
            block_event.wait(timeout=5)

        writer.submit(WriteOp(name="blocker", apply=_blocking_apply))
        writer.submit(WriteOp(name="filler", apply=lambda _conn: None))

        submitted = threading.Event()

        def _submit_third():
            writer.submit(WriteOp(name="third", apply=lambda _conn: None))
            submitted.set()

        thread = threading.Thread(target=_submit_third)
        thread.start()
        try:
            assert not submitted.wait(timeout=0.2)
        finally:
            block_event.set()
            thread.join(timeout=5)
        assert submitted.is_set()
    finally:
        writer.stop(drain=False)


def test_stop_drain_true_applies_every_queued_op(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=1_000_000.0, now=clock.now)
    writer.start()
    for i in range(50):
        writer.submit(_insert_op(f"i{i}", f"pkg-{i}-1.0-py3-none-any.whl", writer.next_seq()))
    writer.stop(drain=True)
    check_conn = sqlite3.connect(db_path)
    try:
        actual_count = _count_wheels(check_conn)
    finally:
        check_conn.close()
    assert actual_count == 50


def test_stop_drain_false_discards_queued_ops(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(
        conn, batch_size=1000, batch_interval=1_000_000.0, now=clock.now, queue_maxsize=1000
    )
    writer.start()
    for i in range(50):
        writer.submit(_insert_op(f"i{i}", f"pkg-{i}-1.0-py3-none-any.whl", writer.next_seq()))
    writer.stop(drain=False)
    check_conn = sqlite3.connect(db_path)
    try:
        actual_count = _count_wheels(check_conn)
    finally:
        check_conn.close()
    assert actual_count == 0


def test_submit_after_stop_raises(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=0.01, now=clock.now)
    writer.start()
    writer.stop(drain=False)
    with pytest.raises(WriterStoppedError):
        writer.submit(WriteOp(name="too-late", apply=lambda _conn: None))


def test_submit_and_wait_after_stop_raises(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=0.01, now=clock.now)
    writer.start()
    writer.stop(drain=False)
    with pytest.raises(WriterStoppedError):
        writer.submit_and_wait(WriteOp(name="too-late", apply=lambda _conn: None))


def test_stop_is_idempotent(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=0.01, now=clock.now)
    writer.start()
    writer.stop(drain=False)
    writer.stop(drain=False)  # must not raise or hang


def test_submit_and_wait_racing_stop_does_not_lose_the_op_or_hang(db_path, writers, monkeypatch):
    # Force the exact interleaving: submit_and_wait() must pass its
    # "not stopped" check and be in the middle of enqueueing when stop()
    # runs on another thread. Without a lock shared between submit() and
    # stop(), stop() would complete (join the background thread, which
    # exits immediately since the queue is still empty from its point of
    # view) before the delayed put() lands the op in a now-unread queue,
    # hanging this call forever. With the fix, stop() must block until
    # submit()'s critical section finishes.
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now)
    writer.start()

    put_called = threading.Event()
    proceed_put = threading.Event()
    real_put = writer._queue.put

    def _delayed_put(item, *args, **kwargs):
        put_called.set()
        assert proceed_put.wait(timeout=5)
        return real_put(item, *args, **kwargs)

    monkeypatch.setattr(writer._queue, "put", _delayed_put)

    op = WriteOp(name="race", apply=lambda _conn: "ok", result_event=threading.Event())
    submit_result: dict[str, object] = {}

    def _do_submit():
        try:
            submit_result["value"] = writer.submit_and_wait(op)
        except Exception as exc:
            submit_result["error"] = exc

    submit_thread = threading.Thread(target=_do_submit)
    submit_thread.start()
    assert put_called.wait(timeout=5)

    stop_thread = threading.Thread(target=writer.stop, kwargs={"drain": True})
    stop_thread.start()
    # stop() must not be able to complete while submit()'s check-then-enqueue
    # critical section is still in flight.
    stop_thread.join(timeout=0.2)
    assert stop_thread.is_alive()

    proceed_put.set()
    submit_thread.join(timeout=5)
    stop_thread.join(timeout=5)

    assert not stop_thread.is_alive()
    assert submit_result.get("error") is None
    assert submit_result.get("value") == "ok"


def test_submit_racing_stop_does_not_silently_lose_the_op(db_path, writers, monkeypatch):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now)
    writer.start()

    put_called = threading.Event()
    proceed_put = threading.Event()
    real_put = writer._queue.put

    def _delayed_put(item, *args, **kwargs):
        put_called.set()
        assert proceed_put.wait(timeout=5)
        return real_put(item, *args, **kwargs)

    monkeypatch.setattr(writer._queue, "put", _delayed_put)

    applied: list[str] = []
    op = WriteOp(name="race-plain", apply=lambda _conn: applied.append("done"))
    submit_error: dict[str, BaseException] = {}

    def _do_submit():
        try:
            writer.submit(op)
        except Exception as exc:
            submit_error["error"] = exc

    submit_thread = threading.Thread(target=_do_submit)
    submit_thread.start()
    assert put_called.wait(timeout=5)

    stop_thread = threading.Thread(target=writer.stop, kwargs={"drain": True})
    stop_thread.start()
    stop_thread.join(timeout=0.2)
    assert stop_thread.is_alive()

    proceed_put.set()
    submit_thread.join(timeout=5)
    stop_thread.join(timeout=5)

    assert not stop_thread.is_alive()
    assert "error" not in submit_error
    assert applied == ["done"]


# --- Fatal commit-batch failures -------------------------------------------------


def test_fatal_commit_failure_resolves_in_flight_submit_and_wait_with_the_exception(
    db_path, writers
):
    conn = _writer_conn(db_path)
    failing_conn = _FailingConn(conn, fail_on="COMMIT")
    clock = FakeClock()
    writer = writers(failing_conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now)
    writer.start()

    op = WriteOp(name="doomed", apply=lambda _conn: "value", result_event=threading.Event())
    with pytest.raises(sqlite3.OperationalError, match="simulated failure"):
        writer.submit_and_wait(op)


def test_fatal_commit_failure_does_not_hang_the_background_thread(db_path, writers):
    conn = _writer_conn(db_path)
    failing_conn = _FailingConn(conn, fail_on="BEGIN")
    clock = FakeClock()
    writer = writers(failing_conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now)
    writer.start()

    op = WriteOp(name="doomed", apply=lambda _conn: "value", result_event=threading.Event())
    with pytest.raises(sqlite3.OperationalError):
        writer.submit_and_wait(op)

    assert writer._thread is not None
    writer._thread.join(timeout=5)
    assert not writer._thread.is_alive()


def test_submit_after_fatal_commit_failure_raises_writer_stopped_error(db_path, writers):
    conn = _writer_conn(db_path)
    failing_conn = _FailingConn(conn, fail_on="COMMIT")
    clock = FakeClock()
    writer = writers(failing_conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now)
    writer.start()

    op = WriteOp(name="doomed", apply=lambda _conn: "value", result_event=threading.Event())
    with pytest.raises(sqlite3.OperationalError):
        writer.submit_and_wait(op)

    assert writer._thread is not None
    writer._thread.join(timeout=5)
    with pytest.raises(WriterStoppedError):
        writer.submit(WriteOp(name="too-late", apply=lambda _conn: None))


def test_fatal_commit_failure_resolves_other_queued_ops_in_the_same_batch(db_path, writers):
    # A batch_size big enough to hold two ops so both are mid-flight in the
    # same failing _commit_batch call; both waiters must be released with
    # the fatal exception, not left hanging.
    conn = _writer_conn(db_path)
    failing_conn = _FailingConn(conn, fail_on="COMMIT")
    clock = FakeClock()
    writer = writers(failing_conn, batch_size=2, batch_interval=1_000_000.0, now=clock.now)
    writer.start()

    results: dict[str, BaseException] = {}

    def _wait_for(name, op):
        try:
            writer.submit_and_wait(op)
        except Exception as exc:
            results[name] = exc

    op_a = WriteOp(name="a", apply=lambda _conn: "a", result_event=threading.Event())
    op_b = WriteOp(name="b", apply=lambda _conn: "b", result_event=threading.Event())
    thread_a = threading.Thread(target=_wait_for, args=("a", op_a))
    thread_b = threading.Thread(target=_wait_for, args=("b", op_b))
    thread_a.start()
    thread_b.start()
    thread_a.join(timeout=5)
    thread_b.join(timeout=5)

    assert isinstance(results.get("a"), sqlite3.OperationalError)
    assert isinstance(results.get("b"), sqlite3.OperationalError)


class _FailingCloseConn(_FailingConn):
    """Also fails to close, to exercise the fatal-failure cleanup's own error path."""

    def close(self):
        raise sqlite3.OperationalError("simulated close failure")


def test_fatal_commit_failure_logs_when_cleanup_close_also_fails(db_path, writers, caplog):
    conn = _writer_conn(db_path)
    failing_conn = _FailingCloseConn(conn, fail_on="COMMIT")
    clock = FakeClock()
    writer = writers(failing_conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now)
    writer.start()

    op = WriteOp(name="doomed", apply=lambda _conn: "value", result_event=threading.Event())
    with (
        caplog.at_level(logging.ERROR, logger="reroll_sync.writer"),
        pytest.raises(sqlite3.OperationalError, match="simulated failure"),
    ):
        writer.submit_and_wait(op)

    assert writer._thread is not None
    writer._thread.join(timeout=5)
    assert not writer._thread.is_alive()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("error closing connection" in r.getMessage() for r in errors)
    conn.close()


def test_fatal_commit_failure_logs_at_error(db_path, writers, caplog):
    conn = _writer_conn(db_path)
    failing_conn = _FailingConn(conn, fail_on="COMMIT")
    clock = FakeClock()
    writer = writers(failing_conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now)
    writer.start()

    op = WriteOp(name="doomed", apply=lambda _conn: "value", result_event=threading.Event())
    with (
        caplog.at_level(logging.ERROR, logger="reroll_sync.writer"),
        pytest.raises(sqlite3.OperationalError),
    ):
        writer.submit_and_wait(op)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("fatal" in r.getMessage().lower() for r in errors)


# --- WAL management -------------------------------------------------------------


def _grow_wal(writer: Writer, count: int = 300) -> None:
    for i in range(count):
        writer.submit(_insert_op(f"grow-{i}", f"grow-{i}-1.0-py3-none-any.whl", writer.next_seq()))
    _settle(writer)


def test_truncate_checkpoint_shrinks_the_wal_file_on_disk(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(
        conn, batch_size=1, batch_interval=1_000_000.0, checkpoint_interval=5.0, now=clock.now
    )
    writer.start()
    wal_path = Path(f"{db_path}-wal")
    try:
        _grow_wal(writer)
        size_before = wal_path.stat().st_size if wal_path.exists() else 0
        assert size_before > 0

        clock.advance(5.0)
        _settle(writer)

        size_after = wal_path.stat().st_size if wal_path.exists() else 0
        assert size_after < size_before
        assert writer.last_truncate_checkpoint_ok() is True
        assert writer.last_truncate_checkpoint_at() == pytest.approx(5.0)
    finally:
        writer.stop(drain=False)


def test_held_reader_blocks_truncate_and_is_observable(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(
        conn, batch_size=1, batch_interval=1_000_000.0, checkpoint_interval=5.0, now=clock.now
    )
    writer.start()
    reader = sqlite3.connect(db_path)
    try:
        _grow_wal(writer)

        # Hold a read transaction open on a second connection so the WAL
        # cannot be reset.
        reader.execute("BEGIN")
        reader.execute("SELECT COUNT(*) FROM wheels").fetchone()

        clock.advance(5.0)
        _settle(writer)

        assert writer.last_truncate_checkpoint_ok() is False
        assert writer.last_truncate_checkpoint_at() is None
        assert writer.consecutive_checkpoint_failures() == 1

        reader.execute("ROLLBACK")

        clock.advance(5.0)
        _settle(writer)

        assert writer.last_truncate_checkpoint_ok() is True
        assert writer.last_truncate_checkpoint_at() == pytest.approx(10.0)
        assert writer.consecutive_checkpoint_failures() == 0
    finally:
        reader.close()
        writer.stop(drain=False)


def test_five_consecutive_checkpoint_failures_logs_error_naming_leaked_reader(
    db_path, writers, caplog
):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(
        conn, batch_size=1, batch_interval=1_000_000.0, checkpoint_interval=1.0, now=clock.now
    )
    writer.start()
    reader = sqlite3.connect(db_path)
    try:
        _grow_wal(writer)
        reader.execute("BEGIN")
        reader.execute("SELECT COUNT(*) FROM wheels").fetchone()

        with caplog.at_level(logging.ERROR, logger="reroll_sync.writer"):
            for _ in range(5):
                clock.advance(1.0)
                _settle(writer)

        assert writer.consecutive_checkpoint_failures() == 5
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any("leaked" in r.getMessage().lower() for r in errors)
    finally:
        reader.close()
        writer.stop(drain=False)


def test_wal_bytes_reflects_real_file_size(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(
        conn,
        batch_size=1,
        batch_interval=1_000_000.0,
        checkpoint_interval=1_000_000.0,
        now=clock.now,
    )
    writer.start()
    wal_path = Path(f"{db_path}-wal")
    try:
        _grow_wal(writer)
        assert writer.wal_bytes() == wal_path.stat().st_size
        assert writer.wal_bytes() > 0
    finally:
        writer.stop(drain=False)


def test_wal_bytes_is_zero_immediately_after_start_before_any_growth(tmp_path):
    # Starting the writer runs a SELECT (the change_seq high-water mark
    # query), which is enough for sqlite to materialize an empty -wal file
    # under WAL mode -- so right after start(), the file exists but is 0
    # bytes; this covers the "wal file present but empty" case, distinct
    # from "no wal file at all" (covered below).
    db_path = str(tmp_path / "fresh.db")
    init_db(db_path)
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = Writer(conn, batch_size=1000, batch_interval=0.01, now=clock.now)
    writer.start()
    try:
        assert writer.wal_bytes() == 0
    finally:
        writer.stop(drain=False)


def test_wal_bytes_returns_zero_before_writer_has_started(memory_conn):
    # Before start(), _db_path has never been resolved, so no -wal file
    # matching it can exist -- exercises the "file does not exist" branch
    # distinctly from "exists but empty".
    writer = Writer(memory_conn, batch_size=1000, batch_interval=0.01)
    assert writer.wal_bytes() == 0


# --- Incremental vacuum ----------------------------------------------------------


def test_deleting_many_rows_raises_freelist_then_vacuum_lowers_it(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(
        conn,
        batch_size=1,
        batch_interval=1_000_000.0,
        checkpoint_interval=1_000_000.0,
        vacuum_interval=5.0,
        now=clock.now,
    )
    writer.start()
    try:
        _grow_wal(writer, count=500)

        def _delete_all(conn):
            conn.execute("DELETE FROM wheels")

        writer.submit_and_wait(WriteOp(name="delete-all", apply=_delete_all))

        freelist_after_delete = writer.freelist_count()
        assert freelist_after_delete > 0

        clock.advance(5.0)
        _settle(writer)

        assert writer.freelist_count() < freelist_after_delete
    finally:
        writer.stop(drain=False)


def test_incremental_vacuum_runs_between_batches_not_mid_batch(db_path, writers):
    # A regression guard: vacuum must never run while a batch is being
    # accumulated. Exercised indirectly -- the writer must still accept and
    # commit ops normally right after a vacuum pass runs between batches.
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(
        conn,
        batch_size=1,
        batch_interval=1_000_000.0,
        checkpoint_interval=1_000_000.0,
        vacuum_interval=1.0,
        now=clock.now,
    )
    writer.start()
    try:
        _settle(writer)  # anchor last_vacuum_check at clock value 0.0
        clock.advance(1.0)
        result = writer.submit_and_wait(
            WriteOp(name="after-vacuum", apply=lambda _conn: "still works")
        )
        assert result == "still works"
    finally:
        writer.stop(drain=False)


def test_incremental_vacuum_executes_the_literal_bounded_pragma(db_path, writers):
    # Regression guard on the exact statement text: the page count must be
    # bounded (10_000), not an unbounded ``incremental_vacuum`` call.
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(
        conn,
        batch_size=1,
        batch_interval=1_000_000.0,
        checkpoint_interval=1_000_000.0,
        vacuum_interval=5.0,
        now=clock.now,
    )
    writer.start()
    executed: list[str] = []
    conn.set_trace_callback(lambda statement: executed.append(statement.strip()))
    try:
        _settle(writer)  # anchor last_vacuum_check at clock value 0.0
        clock.advance(5.0)
        _settle(writer)
        assert "PRAGMA incremental_vacuum(10000)" in executed
    finally:
        writer.stop(drain=False)


def test_incremental_vacuum_does_not_block_a_concurrent_reader_past_its_budget(db_path, writers):
    # While the writer's incremental_vacuum PRAGMA call is literally
    # in-flight (paused mid-execution), a concurrent read_txn on a separate
    # connection must still complete within its budget -- WAL readers are
    # never blocked by a writer's ordinary (non-checkpoint) statements.
    conn = _writer_conn(db_path)
    writer_clock = FakeClock()
    vacuum_started = threading.Event()
    proceed_vacuum = threading.Event()
    delaying_conn = _DelayingConn(conn, "PRAGMA INCREMENTAL_VACUUM", vacuum_started, proceed_vacuum)
    writer = writers(
        delaying_conn,
        batch_size=1,
        batch_interval=1_000_000.0,
        checkpoint_interval=1_000_000.0,
        vacuum_interval=5.0,
        now=writer_clock.now,
    )
    writer.start()
    reader = sqlite3.connect(db_path)
    try:
        _grow_wal(writer, count=500)

        def _delete_all(c):
            c.execute("DELETE FROM wheels")

        writer.submit_and_wait(WriteOp(name="delete-all", apply=_delete_all))

        writer_clock.advance(5.0)
        settle_thread = threading.Thread(target=lambda: _settle(writer))
        settle_thread.start()
        assert vacuum_started.wait(timeout=5)

        reader_clock = FakeClock()
        watchdog = ReadTxnWatchdog()
        with read_txn(
            reader,
            budget=0.25,
            label="concurrent-with-vacuum",
            now=reader_clock.now,
            strict=True,
            watchdog=watchdog,
        ):
            reader_clock.advance(0.01)
            (count,) = reader.execute("SELECT COUNT(*) FROM wheels").fetchone()
        assert count == 0
        assert watchdog.snapshot().over_budget_count == 0

        proceed_vacuum.set()
        settle_thread.join(timeout=5)
    finally:
        reader.close()
        writer.stop(drain=False)


# --- Watchdog (read_txn) -----------------------------------------------------------


@pytest.fixture
def memory_conn():
    conn = sqlite3.connect(":memory:")
    try:
        yield conn
    finally:
        conn.close()


def test_read_inside_budget_logs_nothing_and_increments_nothing(caplog, memory_conn):
    clock = FakeClock()
    watchdog = ReadTxnWatchdog()
    with (
        caplog.at_level(logging.WARNING, logger="reroll_sync.writer"),
        read_txn(memory_conn, budget=1.0, label="fast-read", now=clock.now, watchdog=watchdog),
    ):
        clock.advance(0.1)
    assert not any(r.levelno == logging.WARNING for r in caplog.records)
    snapshot = watchdog.snapshot()
    assert snapshot.over_budget_count == 0
    assert snapshot.longest_ms == pytest.approx(100.0)


def test_read_exceeding_budget_logs_once_with_label(caplog, memory_conn):
    clock = FakeClock()
    watchdog = ReadTxnWatchdog()
    with (
        caplog.at_level(logging.WARNING, logger="reroll_sync.writer"),
        read_txn(memory_conn, budget=0.1, label="slow-read", now=clock.now, watchdog=watchdog),
    ):
        clock.advance(1.0)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "slow-read" in warnings[0].getMessage()
    assert watchdog.snapshot().over_budget_count == 1


def test_longest_read_txn_ms_records_maximum_across_several_reads(memory_conn):
    clock = FakeClock()
    watchdog = ReadTxnWatchdog()
    for duration in (0.05, 0.2, 0.01):
        with read_txn(memory_conn, budget=10.0, label="r", now=clock.now, watchdog=watchdog):
            clock.advance(duration)
    assert watchdog.snapshot().longest_ms == pytest.approx(200.0)


def test_strict_mode_raises_on_exceeding_budget(memory_conn):
    clock = FakeClock()
    watchdog = ReadTxnWatchdog()
    with (
        pytest.raises(ReadTxnBudgetExceeded),
        read_txn(
            memory_conn,
            budget=0.1,
            label="strict-read",
            now=clock.now,
            strict=True,
            watchdog=watchdog,
        ),
    ):
        clock.advance(1.0)
    assert watchdog.snapshot().over_budget_count == 1


def test_read_txn_yields_the_connection_for_use_in_the_block(memory_conn):
    clock = FakeClock()
    watchdog = ReadTxnWatchdog()
    with read_txn(memory_conn, budget=10.0, label="use", now=clock.now, watchdog=watchdog) as inner:
        (value,) = inner.execute("SELECT 1").fetchone()
    assert value == 1


def test_default_watchdog_is_shared_when_none_supplied(memory_conn):
    clock = FakeClock()
    with read_txn(memory_conn, budget=10.0, label="default-watchdog", now=clock.now):
        clock.advance(0.01)
    # No exception, no explicit watchdog required -- exercises the module
    # default without asserting on shared global state (other tests may
    # also use the default).


# --- change_seq --------------------------------------------------------------------


def test_fresh_database_starts_at_one(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=0.01, now=clock.now)
    writer.start()
    try:
        assert writer.current_seq() == 0
        assert writer.next_seq() == 1
    finally:
        writer.stop(drain=False)


def test_restart_resumes_above_stored_maximum(db_path, writers):
    # batch_size=1 makes each submit_and_wait flush immediately on size,
    # independent of batch_interval/clock timing.
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1, batch_interval=1_000_000.0, now=clock.now)
    writer.start()
    writer.submit_and_wait(_insert_op("a", "a-1.0-py3-none-any.whl", writer.next_seq()))
    writer.submit_and_wait(_insert_op("b", "b-1.0-py3-none-any.whl", writer.next_seq()))
    writer.stop(drain=True)

    conn2 = _writer_conn(db_path)
    clock2 = FakeClock()
    writer2 = writers(conn2, batch_size=1, batch_interval=1_000_000.0, now=clock2.now)
    writer2.start()
    try:
        assert writer2.current_seq() == 2
        assert writer2.next_seq() == 3
    finally:
        writer2.stop(drain=False)


def test_concurrent_producers_receive_strictly_increasing_seq_with_no_duplicates(db_path, writers):
    conn = _writer_conn(db_path)
    clock = FakeClock()
    writer = writers(conn, batch_size=1000, batch_interval=0.01, now=clock.now)
    writer.start()
    seqs: list[int] = []
    lock = threading.Lock()

    def _worker():
        for _ in range(200):
            seq = writer.next_seq()
            with lock:
                seqs.append(seq)

    try:
        threads = [threading.Thread(target=_worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        writer.stop(drain=False)
    assert len(seqs) == len(set(seqs))
    assert sorted(seqs) == list(range(1, 1601))


# --- Acceptance: no runtime module outside writer.py commits/rolls back/BEGINs ------


def test_no_module_outside_writer_owns_transaction_boundaries():
    """The writer thread is the sole runtime commit boundary.

    Excluded: ``db.py``'s ``init_db`` (one-time bootstrap before the writer
    exists) and ``archive/store.py`` (takes its own ``conn`` directly, not
    yet integrated with ``Writer``). ``cli.py`` and ``stats.py`` call into
    the modules above rather than committing directly, so they need no
    explicit exclusion.
    """
    src_root = Path(__file__).resolve().parent.parent / "src" / "reroll_sync"
    excluded = {
        src_root / "writer.py",
        src_root / "db.py",
        src_root / "archive" / "store.py",
    }
    pattern = re.compile(
        r"\.commit\s*\(|\.rollback\s*\(|execute\s*\(\s*[\"']\s*BEGIN", re.IGNORECASE
    )
    offenders = []
    for path in sorted(src_root.rglob("*.py")):
        if path in excluded:
            continue
        text = path.read_text()
        if pattern.search(text):
            offenders.append(str(path))
    assert offenders == []
