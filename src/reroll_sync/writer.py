"""The single runtime sqlite writer thread, plus the read-transaction watchdog.

The only module in ``src/`` permitted to call ``conn.commit()``,
``conn.rollback()``, or execute ``BEGIN`` at runtime.
"""

from __future__ import annotations

import logging
import queue
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.02
_LEAKED_READER_THRESHOLD = 5
_VACUUM_PAGE_COUNT = 10_000
_BLOCKED_SQL_PREFIXES = ("BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE")


class TransactionBoundaryViolation(RuntimeError):
    """Raised when a ``WriteOp.apply`` tries to manage transaction boundaries itself."""


class WriterStoppedError(RuntimeError):
    """Raised by ``submit``/``submit_and_wait`` once the writer has been stopped."""


class ReadTxnBudgetExceeded(RuntimeError):
    """Raised by ``read_txn`` in strict mode when a read exceeds its budget."""


@dataclass
class WriteOp:
    """A unit of work the writer applies inside its own batch transaction.

    ``apply`` receives a connection proxy that rejects ``commit``,
    ``rollback``, and raw transaction-control SQL. Set ``result_event`` (or
    let :meth:`Writer.submit_and_wait` create one) to be notified once the
    op's batch has committed.
    """

    name: str
    apply: Callable[[sqlite3.Connection], Any]
    result_event: threading.Event | None = None
    _outcome: _Outcome | None = field(default=None, init=False, repr=False, compare=False)


class Writer:
    """Batches ``WriteOp`` s onto one connection from a single background thread.

    ``conn`` must be created with ``check_same_thread=False``: it is
    constructed on the caller's thread but consumed exclusively by this
    class's own background thread from :meth:`start` onward.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        batch_size: int = 1000,
        batch_interval: float = 0.1,
        checkpoint_interval: float = 60.0,
        vacuum_interval: float = 3600.0,
        now: Callable[[], float] = time.monotonic,
        queue_maxsize: int = 10_000,
    ) -> None:
        self._conn = conn
        self._batch_size = batch_size
        self._batch_interval = batch_interval
        self._checkpoint_interval = checkpoint_interval
        self._vacuum_interval = vacuum_interval
        self._now = now
        self._queue: queue.Queue[WriteOp] = queue.Queue(maxsize=queue_maxsize)

        self._started = False
        self._stopped = False
        self._drain_on_stop = True
        self._stop_requested = threading.Event()
        self._thread: threading.Thread | None = None
        self._lifecycle_lock = threading.Lock()

        self._seq_lock = threading.Lock()
        self._change_seq = 0

        self._failed_ops = 0
        self._last_truncate_checkpoint_at: float | None = None
        self._last_truncate_checkpoint_ok: bool | None = None
        self._consecutive_checkpoint_failures = 0
        self._freelist_count = 0
        self._db_path = ""

    def start(self) -> None:
        """Load the ``change_seq`` high-water mark and spawn the writer's background thread."""
        if self._started:
            raise RuntimeError("Writer.start() called more than once")
        self._conn.autocommit = True
        (max_seq,) = self._conn.execute(
            "SELECT COALESCE(MAX(change_seq), 0) FROM wheels"
        ).fetchone()
        self._change_seq = max_seq
        self._db_path = self._resolve_main_db_path()
        self._started = True
        self._thread = threading.Thread(target=self._run, name="reroll-sync-writer", daemon=True)
        self._thread.start()

    def stop(self, drain: bool = True) -> None:
        """Stop the writer.

        ``drain=True`` applies everything queued first; ``drain=False`` discards it.
        """
        with self._lifecycle_lock:
            if self._stopped:
                return
            self._drain_on_stop = drain
            self._stopped = True
        self._stop_requested.set()
        cast(threading.Thread, self._thread).join()

    def submit(self, op: WriteOp) -> None:
        """Enqueue ``op``, blocking if the bounded queue is full."""
        with self._lifecycle_lock:
            if self._stopped:
                raise WriterStoppedError("writer has been stopped")
            self._queue.put(op)

    def submit_and_wait(self, op: WriteOp) -> Any:
        """Enqueue ``op`` and block until its batch commits, returning ``apply``'s result."""
        if op.result_event is None:
            op.result_event = threading.Event()
        self.submit(op)
        op.result_event.wait()
        outcome = cast(_Outcome, op._outcome)
        if outcome.exception is not None:
            raise outcome.exception
        return outcome.value

    def next_seq(self) -> int:
        """Return the next strictly-increasing ``change_seq`` value."""
        with self._seq_lock:
            self._change_seq += 1
            return self._change_seq

    def current_seq(self) -> int:
        """Return the most recently issued (or loaded) ``change_seq`` value."""
        with self._seq_lock:
            return self._change_seq

    def queue_depth(self) -> int:
        """Return the number of ops submitted but not yet pulled off the queue."""
        return self._queue.qsize()

    def failed_ops(self) -> int:
        """Return the count of ops whose ``apply`` raised, across the writer's lifetime."""
        return self._failed_ops

    def wal_bytes(self) -> int:
        """Return the current ``-wal`` file size in bytes, or 0 if it does not exist."""
        wal_path = Path(f"{self._db_path}-wal")
        if not wal_path.exists():
            return 0
        return wal_path.stat().st_size

    def freelist_count(self) -> int:
        """Return the database's freelist page count, as of the last commit/checkpoint/vacuum."""
        return self._freelist_count

    def last_truncate_checkpoint_at(self) -> float | None:
        """Return when (per the injected clock) the last *successful* TRUNCATE checkpoint ran."""
        return self._last_truncate_checkpoint_at

    def last_truncate_checkpoint_ok(self) -> bool | None:
        """Return whether the most recent TRUNCATE checkpoint succeeded, or None if none ran."""
        return self._last_truncate_checkpoint_ok

    def consecutive_checkpoint_failures(self) -> int:
        """Return the current streak of failed TRUNCATE checkpoint attempts."""
        return self._consecutive_checkpoint_failures

    def _run(self) -> None:
        batch: list[WriteOp] = []
        batch_deadline: float | None = None
        last_checkpoint_check = self._now()
        last_vacuum_check = self._now()

        while True:
            try:
                op = self._queue.get(timeout=_POLL_INTERVAL)
            except queue.Empty:
                pass
            else:
                batch.append(op)
                if batch_deadline is None:
                    batch_deadline = self._now() + self._batch_interval

            stop_requested = self._stop_requested.is_set()
            size_reached = len(batch) >= self._batch_size
            interval_elapsed = batch_deadline is not None and self._now() >= batch_deadline
            # During drain, keep accumulating until the queue itself is
            # empty (or batch_size is hit) rather than flushing after every
            # single item once stop() has been requested.
            drain_flush = (
                stop_requested and self._drain_on_stop and bool(batch) and self._queue.empty()
            )

            flushed: list[WriteOp] | None = None
            outcomes: list[_Outcome] | None = None
            if batch and (size_reached or interval_elapsed or drain_flush):
                try:
                    outcomes = self._commit_batch(batch)
                except Exception as exc:
                    self._fail_fatally(exc, batch)
                    return
                self._refresh_freelist_count()
                flushed = batch
                batch = []
                batch_deadline = None

            if not batch:
                if self._now() - last_checkpoint_check >= self._checkpoint_interval:
                    self._run_checkpoint()
                    last_checkpoint_check = self._now()
                if self._now() - last_vacuum_check >= self._vacuum_interval:
                    self._run_vacuum()
                    last_vacuum_check = self._now()

            if flushed is not None:
                self._resolve(flushed, cast("list[_Outcome]", outcomes))

            if stop_requested:
                if not self._drain_on_stop:
                    self._discard_pending(batch)
                    break
                if self._queue.empty() and not batch:
                    break

        if self._drain_on_stop:
            self._run_checkpoint()
        self._conn.close()

    def _fail_fatally(self, exc: BaseException, in_flight_batch: list[WriteOp]) -> None:
        """Stop the writer after an unrecoverable error committing a batch.

        Resolves every op in ``in_flight_batch`` with ``exc`` (so no
        ``submit_and_wait`` caller hangs), discards anything still queued,
        and marks the writer stopped so subsequent ``submit`` calls raise
        instead of queuing into a writer nobody is draining.
        """
        logger.error("writer: fatal error committing batch, stopping: %s", exc, exc_info=exc)
        with self._lifecycle_lock:
            self._stopped = True
        self._stop_requested.set()
        outcome = _Outcome(exception=exc)
        self._resolve(in_flight_batch, [outcome] * len(in_flight_batch))
        self._discard_pending([])
        try:
            self._conn.close()
        except Exception:
            logger.error("writer: error closing connection after fatal failure", exc_info=True)

    def _commit_batch(self, batch: list[WriteOp]) -> list[_Outcome]:
        self._conn.execute("BEGIN")
        outcomes = [self._apply_op(op, f"op_{i}") for i, op in enumerate(batch)]
        self._conn.execute("COMMIT")
        return outcomes

    def _apply_op(self, op: WriteOp, savepoint_name: str) -> _Outcome:
        guarded = _GuardedConnection(self._conn, op.name)
        self._conn.execute(f"SAVEPOINT {savepoint_name}")
        try:
            result = op.apply(cast(sqlite3.Connection, guarded))
        except Exception as exc:
            self._conn.execute(f"ROLLBACK TO {savepoint_name}")
            self._conn.execute(f"RELEASE {savepoint_name}")
            self._failed_ops += 1
            logger.error("write op %r failed: %s", op.name, exc)
            return _Outcome(exception=exc)
        self._conn.execute(f"RELEASE {savepoint_name}")
        return _Outcome(value=result)

    def _resolve(self, ops: list[WriteOp], outcomes: list[_Outcome]) -> None:
        for op, outcome in zip(ops, outcomes, strict=True):
            op._outcome = outcome
            if op.result_event is not None:
                op.result_event.set()

    def _discard_pending(self, batch: list[WriteOp]) -> None:
        leftover = list(batch)
        while True:
            try:
                leftover.append(self._queue.get_nowait())
            except queue.Empty:
                break
        exc = WriterStoppedError("writer stopped with drain=False; op discarded")
        for op in leftover:
            op._outcome = _Outcome(exception=exc)
            if op.result_event is not None:
                op.result_event.set()

    def _run_checkpoint(self) -> None:
        busy, _log_frames, _checkpointed = self._conn.execute(
            "PRAGMA wal_checkpoint(TRUNCATE)"
        ).fetchone()
        if busy == 0:
            self._last_truncate_checkpoint_at = self._now()
            self._last_truncate_checkpoint_ok = True
            self._consecutive_checkpoint_failures = 0
        else:
            self._last_truncate_checkpoint_ok = False
            self._consecutive_checkpoint_failures += 1
            if self._consecutive_checkpoint_failures >= _LEAKED_READER_THRESHOLD:
                logger.error(
                    "writer: %d consecutive TRUNCATE checkpoint failures; "
                    "likely a leaked long-lived read transaction holding back the WAL",
                    self._consecutive_checkpoint_failures,
                )
        self._refresh_freelist_count()

    def _run_vacuum(self) -> None:
        self._conn.execute(f"PRAGMA incremental_vacuum({_VACUUM_PAGE_COUNT})")
        self._refresh_freelist_count()

    def _refresh_freelist_count(self) -> None:
        (self._freelist_count,) = self._conn.execute("PRAGMA freelist_count").fetchone()

    def _resolve_main_db_path(self) -> str:
        rows = self._conn.execute("PRAGMA database_list").fetchall()
        return next(filename for _seq, name, filename in rows if name == "main")


@dataclass(frozen=True)
class ReadTxnSnapshot:
    """A point-in-time snapshot of :class:`ReadTxnWatchdog` counters."""

    over_budget_count: int
    longest_ms: float


class ReadTxnWatchdog:
    """Aggregates :func:`read_txn` durations across arbitrary reader connections."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._over_budget_count = 0
        self._longest_ms = 0.0

    def record(self, duration_ms: float, over_budget: bool) -> None:
        with self._lock:
            if over_budget:
                self._over_budget_count += 1
            self._longest_ms = max(self._longest_ms, duration_ms)

    def snapshot(self) -> ReadTxnSnapshot:
        with self._lock:
            return ReadTxnSnapshot(self._over_budget_count, self._longest_ms)


_default_watchdog = ReadTxnWatchdog()


@contextmanager
def read_txn(
    conn: sqlite3.Connection,
    *,
    budget: float = 0.25,
    label: str,
    now: Callable[[], float] = time.monotonic,
    strict: bool = False,
    watchdog: ReadTxnWatchdog | None = None,
) -> Iterator[sqlite3.Connection]:
    """Time a read-only block, warning (or raising in strict mode) if it exceeds ``budget``.

    Every multi-row read outside the writer thread should wrap its query in
    this: a read transaction left open past its budget is exactly what
    blocks a ``TRUNCATE`` checkpoint from reclaiming WAL space.
    """
    active_watchdog = watchdog if watchdog is not None else _default_watchdog
    start = now()
    try:
        yield conn
    finally:
        elapsed = now() - start
        over_budget = elapsed > budget
        active_watchdog.record(elapsed * 1000.0, over_budget)
        if over_budget:
            message = (
                f"read_txn '{label}' took {elapsed * 1000:.1f}ms, "
                f"over its {budget * 1000:.1f}ms budget"
            )
            if strict:
                raise ReadTxnBudgetExceeded(message)
            logger.warning(message)


@dataclass(frozen=True)
class _Outcome:
    """The result of running one ``WriteOp.apply``: either a value or an exception."""

    value: Any = None
    exception: BaseException | None = None


def _check_transaction_control_sql(sql: str, op_name: str) -> None:
    if sql.strip().upper().startswith(_BLOCKED_SQL_PREFIXES):
        raise TransactionBoundaryViolation(
            f"write op {op_name!r} must not manage transaction boundaries itself: {sql!r}"
        )


class _GuardedCursor:
    """Wraps a real ``sqlite3.Cursor`` so it can't bypass ``_GuardedConnection``'s checks."""

    def __init__(self, cursor: sqlite3.Cursor, op_name: str) -> None:
        self._cursor = cursor
        self._op_name = op_name

    def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
        _check_transaction_control_sql(sql, self._op_name)
        return self._cursor.execute(sql, parameters)

    def executemany(self, sql: str, parameters: Any) -> sqlite3.Cursor:
        _check_transaction_control_sql(sql, self._op_name)
        return self._cursor.executemany(sql, parameters)

    def executescript(self, sql_script: str) -> sqlite3.Cursor:
        raise TransactionBoundaryViolation(
            f"write op {self._op_name!r} must not call executescript(): it commits "
            "implicitly and cannot be scoped to the op's SAVEPOINT"
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class _GuardedConnection:
    """Proxies ``conn`` for use inside ``WriteOp.apply``.

    ``commit``, ``rollback``, and ``executescript`` always raise;
    ``execute``/``executemany`` (including via ``cursor()``) raise on
    transaction-control SQL. Cooperative guard only -- ``._conn`` reaches
    the real connection directly.
    """

    def __init__(self, conn: sqlite3.Connection, op_name: str) -> None:
        self._conn = conn
        self._op_name = op_name

    def commit(self) -> None:
        raise TransactionBoundaryViolation(
            f"write op {self._op_name!r} must not call conn.commit(); "
            "the writer owns transaction boundaries"
        )

    def rollback(self) -> None:
        raise TransactionBoundaryViolation(
            f"write op {self._op_name!r} must not call conn.rollback(); "
            "the writer owns transaction boundaries"
        )

    def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
        self._check_sql(sql)
        return self._conn.execute(sql, parameters)

    def executemany(self, sql: str, parameters: Any) -> sqlite3.Cursor:
        self._check_sql(sql)
        return self._conn.executemany(sql, parameters)

    def executescript(self, sql_script: str) -> sqlite3.Cursor:
        raise TransactionBoundaryViolation(
            f"write op {self._op_name!r} must not call executescript(): it commits "
            "implicitly and cannot be scoped to the op's SAVEPOINT"
        )

    def cursor(self) -> _GuardedCursor:
        return _GuardedCursor(self._conn.cursor(), self._op_name)

    def _check_sql(self, sql: str) -> None:
        _check_transaction_control_sql(sql, self._op_name)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)
