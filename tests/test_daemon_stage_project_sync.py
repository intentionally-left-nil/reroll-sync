"""Tests for the project_sync stage: draining the project-name queue,
breaker gating, and deriving a breaker success/failure signal from
`IngestSummary` (since `sync_project` itself never raises a PyPI error --
it always turns one into an outcome).
"""

from __future__ import annotations

import sqlite3

import httpx
import pytest

from reroll_sync.daemon.circuit_breaker import CircuitBreaker, CircuitState
from reroll_sync.daemon.stages.project_sync import ProjectSyncStage
from reroll_sync.db import init_db
from reroll_sync.pypi_client import ACCEPT_HEADER, PyPIClient
from reroll_sync.writer import Writer

_USER_AGENT = "reroll-sync-test (contact@example.invalid)"


class _FakeLimiter:
    def __init__(self) -> None:
        self.penalized: list[tuple[str, float]] = []

    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        return True

    def penalize(self, child_name: str, seconds: float) -> None:
        self.penalized.append((child_name, seconds))


def _client(handler) -> PyPIClient:
    return PyPIClient(
        _FakeLimiter(), user_agent=_USER_AGENT, transport=httpx.MockTransport(handler)
    )


def _project_payload(files: list[str]) -> dict:
    return {
        "meta": {"_last-serial": 1},
        "files": [
            {"filename": f, "url": f"https://pypi.org/files/{f}", "hashes": {}} for f in files
        ],
    }


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "project_sync.db")
    init_db(path)
    return path


def _writer_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 50")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


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


def _breaker() -> CircuitBreaker:
    return CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)


def _stage(db_path, writer, handler, *, breaker=None, limiter=None, **kwargs) -> ProjectSyncStage:
    client = _client(handler)
    return ProjectSyncStage(
        client,
        lambda: sqlite3.connect(str(db_path)),
        writer,
        limiter if limiter is not None else _FakeLimiter(),
        breaker if breaker is not None else _breaker(),
        **kwargs,
    )


def test_empty_queue_reports_no_work(db_path, writer):
    stage = _stage(db_path, writer, lambda request: httpx.Response(404, request=request))
    assert stage.iterate() is False


def test_enqueued_project_is_synced(db_path, reader, writer):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER},
            json=_project_payload(["widget-1.0-py3-none-any.whl"]),
            request=request,
        )

    stage = _stage(db_path, writer, handler)
    stage.enqueue("widget")

    did_work = stage.iterate()

    assert did_work is True
    row = reader.execute("SELECT COUNT(*) FROM wheels WHERE project = ?", ("widget",)).fetchone()
    assert row[0] == 1


def test_open_breaker_skips_and_requeues(db_path, writer):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(
            200, headers={"content-type": ACCEPT_HEADER}, json=_project_payload([]), request=request
        )

    breaker = _breaker()
    for _ in range(5):
        breaker.record_failure()
    assert breaker.state() == CircuitState.OPEN

    stage = _stage(db_path, writer, handler, breaker=breaker)
    stage.enqueue("widget")

    did_work = stage.iterate()

    assert did_work is False
    assert calls == []

    # Requeued: once the breaker closes again, the same name is processed.
    breaker.record_success()
    did_work2 = stage.iterate()
    assert did_work2 is True
    assert calls == [1]


def test_all_transient_failures_records_a_breaker_failure(db_path, writer):
    """Repeated failures must each reach the server (not get filtered out by
    `ProjectBackoff`'s own eligibility window), so the clock is advanced well
    past any backoff delay between iterations.
    """
    breaker = _breaker()

    class _FakeClock:
        def __init__(self) -> None:
            self.value = 0.0

        def now(self) -> float:
            self.value += 100_000.0
            return self.value

    clock = _FakeClock()
    from reroll_sync.ingest import ProjectBackoff

    stage = _stage(
        db_path,
        writer,
        lambda request: httpx.Response(500, request=request),
        breaker=breaker,
        backoff=ProjectBackoff(now=clock.now),
        now=clock.now,
    )
    stage.enqueue("widget")

    stage.iterate()

    assert breaker.state() == CircuitState.CLOSED  # one failure, below threshold=5
    for _ in range(4):
        stage.enqueue("widget")
        stage.iterate()
    assert breaker.state() == CircuitState.OPEN


def test_a_successful_project_records_a_breaker_success(db_path, writer):
    breaker = _breaker()
    breaker.record_failure()
    breaker.record_failure()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": ACCEPT_HEADER}, json=_project_payload([]), request=request
        )

    stage = _stage(db_path, writer, handler, breaker=breaker)
    stage.enqueue("widget")
    stage.iterate()

    assert breaker.state() == CircuitState.CLOSED


def test_mixed_success_and_failure_records_a_breaker_success(db_path, writer):
    """One project succeeding is evidence pypi.org itself is up, even if a
    sibling project in the same batch hit a transient error.
    """
    breaker = _breaker()
    breaker.record_failure()
    breaker.record_failure()

    def handler(request: httpx.Request) -> httpx.Response:
        if "broken" in request.url.path:
            return httpx.Response(500, request=request)
        return httpx.Response(
            200, headers={"content-type": ACCEPT_HEADER}, json=_project_payload([]), request=request
        )

    stage = _stage(db_path, writer, handler, breaker=breaker, max_workers=2)
    stage.enqueue("ok")
    stage.enqueue("broken")
    stage.iterate()

    assert breaker.state() == CircuitState.CLOSED


def test_rate_limited_project_does_not_affect_breaker(db_path, writer):
    breaker = _breaker()
    limiter = _FakeLimiter()
    stage = _stage(
        db_path,
        writer,
        lambda request: httpx.Response(429, headers={"retry-after": "1"}, request=request),
        breaker=breaker,
        limiter=limiter,
    )
    stage.enqueue("widget")

    for _ in range(10):
        stage.enqueue("widget")
        stage.iterate()

    assert breaker.state() == CircuitState.CLOSED
    assert limiter.penalized


def test_rate_limited_project_does_not_reset_prior_failure_count(db_path, writer):
    breaker = _breaker()
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_failure()  # 4 failures, one short of threshold=5

    stage = _stage(
        db_path,
        writer,
        lambda request: httpx.Response(429, headers={"retry-after": "1"}, request=request),
        breaker=breaker,
    )
    stage.enqueue("widget")
    stage.iterate()

    assert breaker.state() == CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state() == CircuitState.OPEN  # confirms the count really was 4, not reset


def test_batch_limit_caps_names_drained_per_iteration(db_path, writer):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": ACCEPT_HEADER}, json=_project_payload([]), request=request
        )

    stage = _stage(db_path, writer, handler, batch_limit=2)
    stage.enqueue("a")
    stage.enqueue("b")
    stage.enqueue("c")

    stage.iterate()
    remaining = stage._drain(10)
    assert remaining == ["c"]
