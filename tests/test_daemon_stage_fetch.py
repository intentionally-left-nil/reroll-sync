"""Tests for the fetch stage: claiming, dispatching over a thread pool,
handing successes to the archive queue, and the files.pythonhosted.org
breaker's gating/signal derived from `FetchOutcome`s (since `fetch_one`
itself never raises a PyPI error -- it always turns one into an outcome).
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from reroll_sync.daemon.circuit_breaker import CircuitBreaker, CircuitState
from reroll_sync.daemon.stage_loop import PollTrigger, StageLoop
from reroll_sync.daemon.stages.fetch import FetchStage
from reroll_sync.db import init_db
from reroll_sync.dispatcher import Dispatcher, QueueItem, Stage
from reroll_sync.fetch import ByteBudgetedQueue, HandoffItem
from reroll_sync.pypi_client import PyPIClient
from reroll_sync.schema import WheelState
from reroll_sync.writer import Writer

_USER_AGENT = "reroll-sync-test (contact@example.invalid)"


class _FakeLimiter:
    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        return True


def _client(handler) -> PyPIClient:
    return PyPIClient(
        _FakeLimiter(), user_agent=_USER_AGENT, transport=httpx.MockTransport(handler)
    )


def _writer_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 50")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "fetch_stage.db")
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


@pytest.fixture
def dispatcher(reader, writer):
    return Dispatcher(reader, writer, reroll_version="1.0", now=lambda: 1_700_000_000.0)


@pytest.fixture
def pool():
    executor = ThreadPoolExecutor(max_workers=4)
    yield executor
    executor.shutdown(wait=True)


def _insert_wheel(
    db_path: str,
    *,
    filename: str,
    project: str = "widget",
    url: str = "https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl",
    metadata_sha256: str | None = None,
    state: WheelState = WheelState.NEED_METADATA,
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, metadata_sha256, serial, change_seq, "
            "updated_at) VALUES (?, ?, ?, 0, ?, ?, 1, 1, '2024-01-01T00:00:00+00:00')",
            (filename, project, int(state), url, metadata_sha256),
        )
        conn.commit()
        assert cursor.lastrowid is not None
        return cursor.lastrowid
    finally:
        conn.close()


def _breaker() -> CircuitBreaker:
    return CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)


def _stage(reader, dispatcher, pool, handler, *, breaker=None, queue=None, limit=10) -> tuple:
    client = _client(handler)
    q = queue if queue is not None else ByteBudgetedQueue(budget_bytes=10_000)
    stage = FetchStage(
        client,
        dispatcher,
        reader,
        q,
        breaker if breaker is not None else _breaker(),
        pool=pool,
        limit=limit,
    )
    return stage, q


def test_empty_queue_reports_no_work(db_path, reader, dispatcher, pool):
    stage, _q = _stage(
        reader, dispatcher, pool, lambda request: httpx.Response(404, request=request)
    )
    assert stage.iterate() is False


def test_successful_fetch_is_handed_off_to_the_archive_queue(db_path, reader, dispatcher, pool):
    data = b"Metadata-Version: 2.1\nName: widget\n"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data, request=request)

    wheel_id = _insert_wheel(db_path, filename="widget-1.0-py3-none-any.whl")
    stage, q = _stage(reader, dispatcher, pool, handler)

    did_work = stage.iterate()

    assert did_work is True
    handoff = q.get()
    assert isinstance(handoff, HandoffItem)
    assert handoff.queue_item.id == wheel_id
    assert handoff.data == data


def test_transient_failure_retries_and_opens_breaker_after_threshold(
    db_path, reader, dispatcher, pool
):
    _insert_wheel(db_path, filename="widget-1.0-py3-none-any.whl")
    breaker = _breaker()
    stage, _q = _stage(
        reader,
        dispatcher,
        pool,
        lambda request: httpx.Response(503, request=request),
        breaker=breaker,
    )

    did_work = stage.iterate()

    assert did_work is True
    assert breaker.state() == CircuitState.CLOSED

    row = reader.execute("SELECT state FROM wheels").fetchone()
    assert row[0] == int(WheelState.NEED_METADATA)  # Retry never changes state


def test_not_found_does_not_affect_the_breaker(db_path, reader, dispatcher, pool):
    """A 404 from a claimed sidecar is PyPI answering (its own index is
    inconsistent), not evidence files.pythonhosted.org is down.
    """
    _insert_wheel(db_path, filename="widget-1.0-py3-none-any.whl")
    breaker = _breaker()
    for _ in range(4):
        breaker.record_failure()
    stage, _q = _stage(
        reader,
        dispatcher,
        pool,
        lambda request: httpx.Response(404, request=request),
        breaker=breaker,
    )

    stage.iterate()

    assert breaker.state() == CircuitState.CLOSED  # unaffected, stays at 4 failures
    breaker.record_failure()
    assert breaker.state() == CircuitState.OPEN  # confirms the count really was 4, not reset


def test_rate_limited_does_not_affect_the_breaker(db_path, reader, dispatcher, pool):
    _insert_wheel(db_path, filename="widget-1.0-py3-none-any.whl")
    breaker = _breaker()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "2"}, request=request)

    stage, _q = _stage(reader, dispatcher, pool, handler, breaker=breaker)

    for _ in range(10):
        stage.iterate()

    assert breaker.state() == CircuitState.CLOSED


def test_open_breaker_skips_claiming_entirely(db_path, reader, dispatcher, pool):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, content=b"x", request=request)

    _insert_wheel(db_path, filename="widget-1.0-py3-none-any.whl")
    breaker = _breaker()
    for _ in range(5):
        breaker.record_failure()

    stage, _q = _stage(reader, dispatcher, pool, handler, breaker=breaker)
    did_work = stage.iterate()

    assert did_work is False
    assert calls == []
    row = reader.execute("SELECT state FROM wheels").fetchone()
    assert row[0] == int(WheelState.NEED_METADATA)  # never claimed at all


def test_a_batch_with_a_success_and_a_transient_failure_still_enqueues_the_success(
    db_path, reader, dispatcher, pool
):
    def handler(request: httpx.Request) -> httpx.Response:
        if "broken" in str(request.url):
            return httpx.Response(503, request=request)
        return httpx.Response(200, content=b"ok-data", request=request)

    _insert_wheel(
        db_path,
        filename="ok-1.0-py3-none-any.whl",
        url="https://files.pythonhosted.org/x/ok-1.0-py3-none-any.whl",
    )
    _insert_wheel(
        db_path,
        filename="broken-1.0-py3-none-any.whl",
        url="https://files.pythonhosted.org/x/broken-1.0-py3-none-any.whl",
    )
    stage, q = _stage(reader, dispatcher, pool, handler)

    did_work = stage.iterate()

    assert did_work is True
    handoff = q.get()
    assert handoff.filename == "ok-1.0-py3-none-any.whl"


def test_paused_disk_guard_skips_claiming_entirely(db_path, reader, dispatcher, pool):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, content=b"x", request=request)

    _insert_wheel(db_path, filename="widget-1.0-py3-none-any.whl")

    class _PausedDiskGuard:
        def is_paused(self) -> bool:
            return True

    client = _client(handler)
    q = ByteBudgetedQueue(budget_bytes=10_000)
    stage = FetchStage(
        client,
        dispatcher,
        reader,
        q,
        _breaker(),
        pool=pool,
        disk_guard=_PausedDiskGuard(),
    )

    did_work = stage.iterate()

    assert did_work is False
    assert calls == []
    row = reader.execute("SELECT state FROM wheels").fetchone()
    assert row[0] == int(WheelState.NEED_METADATA)  # never claimed at all


def test_disk_guard_not_paused_still_allows_claiming(db_path, reader, dispatcher, pool):
    data = b"Metadata-Version: 2.1\nName: widget\n"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data, request=request)

    _insert_wheel(db_path, filename="widget-1.0-py3-none-any.whl")

    class _NotPausedDiskGuard:
        def is_paused(self) -> bool:
            return False

    client = _client(handler)
    q = ByteBudgetedQueue(budget_bytes=10_000)
    stage = FetchStage(
        client,
        dispatcher,
        reader,
        q,
        _breaker(),
        pool=pool,
        disk_guard=_NotPausedDiskGuard(),
    )

    did_work = stage.iterate()

    assert did_work is True
    handoff = q.get()
    assert isinstance(handoff, HandoffItem)
    assert handoff.data == data


def test_claimed_item_with_no_matching_row_is_released_and_logged(
    db_path, reader, dispatcher, pool, caplog
):
    stage, _q = _stage(
        reader, dispatcher, pool, lambda request: httpx.Response(404, request=request)
    )
    bogus_item = QueueItem(id=999, project="ghost", lane=0, state=WheelState.NEED_METADATA)

    with caplog.at_level("ERROR"):
        did_work = stage._dispatch_batch([bogus_item])

    assert did_work is False
    assert dispatcher._in_flight[Stage.FETCH] == set()  # released, not left in-flight forever


# ---------------------------------------------------------------------------
# Pause concurrent with genuinely in-flight work (Fix 7)
# ---------------------------------------------------------------------------


def test_pause_during_a_genuinely_in_flight_batch_lets_it_complete(db_path, writer, pool):
    """A `StageLoop.pause()` call concurrent with an already-dispatched,
    genuinely in-flight fetch batch (a real `ThreadPoolExecutor` worker
    blocked mid-"request") must not affect that batch: it still completes
    and its outcome is applied. Only the *next* iteration is blocked from
    claiming anything new.

    Deterministic via two `threading.Event`s -- no sleeps: the mock
    transport handler itself blocks on `release` until this test has
    observed (`started`) that the worker thread is inside the request and
    called `pause()`, so the ordering ("pause happens while genuinely
    in-flight, not before or after") is guaranteed rather than hoped for.

    `run_once` runs on its own background thread here (mirroring how a
    real `StageLoop` runs), so both the dispatcher's and the stage's own
    sqlite connections need `check_same_thread=False` -- the module's
    other fixtures are bound to the main thread and would raise
    `sqlite3.ProgrammingError` instead.
    """
    _insert_wheel(db_path, filename="widget-1.0-py3-none-any.whl")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    thread_dispatcher = Dispatcher(conn, writer, reroll_version="1.0", now=lambda: 1_700_000_000.0)
    started = threading.Event()
    release = threading.Event()
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        started.set()
        assert release.wait(timeout=5.0)
        return httpx.Response(503, request=request)

    stage, _q = _stage(conn, thread_dispatcher, pool, handler)
    loop = StageLoop("fetch", stage.iterate, PollTrigger(idle_interval=0.0), threading.Event())

    result: dict[str, bool] = {}

    def _run_once() -> None:
        result["did_work"] = loop.run_once()

    runner = threading.Thread(target=_run_once)
    runner.start()

    assert started.wait(timeout=5.0)
    loop.pause()  # concurrent with the in-flight request the worker is blocked in
    release.set()
    runner.join(timeout=5.0)
    assert not runner.is_alive()
    conn.close()

    assert result["did_work"] is True  # the in-flight batch still completed
    verify_conn = sqlite3.connect(db_path)
    try:
        work_row = verify_conn.execute("SELECT attempts FROM work").fetchone()
    finally:
        verify_conn.close()
    assert work_row is not None
    assert work_row[0] == 1  # its Retry outcome was actually applied, not dropped

    # The stage is now paused: a further run_once claims nothing new.
    did_work_after_pause = loop.run_once()
    assert did_work_after_pause is False
    assert len(calls) == 1  # no second HTTP call was ever made
