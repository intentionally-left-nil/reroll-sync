"""Tests for the index_poll stage: conditional GET, breaker gating, and
handing off stale project names for project_sync to pick up.
"""

from __future__ import annotations

import sqlite3

import httpx
import pytest

from reroll_sync.daemon.circuit_breaker import CircuitBreaker, CircuitState
from reroll_sync.daemon.stages.index_poll import IndexPollStage
from reroll_sync.db import init_db
from reroll_sync.pypi_client import ACCEPT_HEADER, PyPIClient

_USER_AGENT = "reroll-sync-test (contact@example.invalid)"


class _FakeLimiter:
    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        return True


def _client(handler) -> PyPIClient:
    return PyPIClient(
        _FakeLimiter(), user_agent=_USER_AGENT, transport=httpx.MockTransport(handler)
    )


def _json_handler(payload: dict, *, status: int = 200, headers: dict | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            headers={"content-type": ACCEPT_HEADER, **(headers or {})},
            json=payload,
            request=request,
        )

    return handler


def _status_handler(status: int):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=b"", request=request)

    return handler


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "index_poll.db")
    init_db(path)
    return path


@pytest.fixture
def reader(db_path):
    conn = sqlite3.connect(str(db_path))
    yield conn
    conn.close()


def _breaker() -> CircuitBreaker:
    return CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)


def _index_payload(names: list[str]) -> dict:
    return {
        "meta": {"_last-serial": 1},
        "projects": [{"name": name, "_last-serial": 1} for name in names],
    }


def test_stale_projects_are_enqueued(reader):
    client = _client(_json_handler(_index_payload(["numpy", "scipy"])))
    enqueued: list[str] = []
    stage = IndexPollStage(client, reader, _breaker(), enqueue=enqueued.append)

    did_work = stage.iterate()

    assert did_work is True
    assert set(enqueued) == {"numpy", "scipy"}


def test_not_modified_reports_no_work(reader):
    client = _client(_status_handler(304))
    stage = IndexPollStage(client, reader, _breaker(), enqueue=lambda name: None)

    assert stage.iterate() is False


def test_no_stale_projects_reports_no_work(reader):
    client = _client(_json_handler(_index_payload([])))
    stage = IndexPollStage(client, reader, _breaker(), enqueue=lambda name: None)

    assert stage.iterate() is False


def test_etag_is_carried_forward_across_polls(reader):
    seen_etags: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_etags.append(request.headers.get("if-none-match"))
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER, "etag": '"abc"'},
            json=_index_payload([]),
            request=request,
        )

    client = _client(handler)
    stage = IndexPollStage(client, reader, _breaker(), enqueue=lambda name: None)

    stage.iterate()
    stage.iterate()

    assert seen_etags == [None, '"abc"']


def test_open_breaker_skips_the_poll_entirely(reader):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(
            200, headers={"content-type": ACCEPT_HEADER}, json=_index_payload([]), request=request
        )

    client = _client(handler)
    breaker = _breaker()
    for _ in range(5):
        breaker.record_failure()
    assert breaker.state() == CircuitState.OPEN

    stage = IndexPollStage(client, reader, breaker, enqueue=lambda name: None)
    did_work = stage.iterate()

    assert did_work is False
    assert calls == []


def test_transient_error_records_a_breaker_failure(reader):
    client = _client(_status_handler(503))
    breaker = _breaker()
    stage = IndexPollStage(client, reader, breaker, enqueue=lambda name: None)

    did_work = stage.iterate()

    assert did_work is False
    assert breaker.state() == CircuitState.CLOSED  # one failure, below threshold
    for _ in range(4):
        stage.iterate()
    assert breaker.state() == CircuitState.OPEN


def test_rate_limited_does_not_record_a_breaker_failure(reader):
    client = _client(_status_handler(429))
    breaker = _breaker()
    stage = IndexPollStage(client, reader, breaker, enqueue=lambda name: None)

    for _ in range(10):
        did_work = stage.iterate()
        assert did_work is False

    assert breaker.state() == CircuitState.CLOSED
