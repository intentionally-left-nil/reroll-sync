"""End-to-end acceptance tests for specs/10-daemon-and-control.md's own
acceptance criteria: no stage starves another (the real
`HierarchicalLimiter`'s reserves), and a dependency outage pauses only its
dependents (the "key isolation test").

These wire up a real `Daemon` against a real sqlite database and segment
directory, with a mocked PyPI transport (never touches the network) and
small, fast intervals. Waits on background stage threads are
`threading.Event.wait(timeout=...)` calls synchronized by test-only hooks
or by the mock transport handler itself (which runs synchronously on the
stage's own worker thread) -- never a fixed-duration sleep, and never a
sleep-in-a-loop poll.
"""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
import threading
from pathlib import Path
from typing import Any

import httpx
import pytest

from reroll_sync.daemon.config import Config
from reroll_sync.daemon.service import Daemon
from reroll_sync.pypi_client import ACCEPT_HEADER, PyPIClient
from reroll_sync.schema import WheelState

_USER_AGENT = "reroll-sync-test (contact@example.invalid)"


class _AlwaysGrantLimiter:
    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        return True

    def penalize(self, child_name: str, seconds: float) -> None:
        pass


@pytest.fixture
def work_dir(tmp_path):
    return tmp_path


@pytest.fixture
def socket_dir():
    path = Path(tempfile.mkdtemp(prefix="rs-accept-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


def _config(work_dir, socket_dir, **overrides: Any) -> Config:
    kwargs: dict[str, Any] = {
        "db_path": work_dir / "reroll_sync.db",
        "segments_dir": work_dir / "segments",
        "socket_path": socket_dir / "control.sock",
        "user_agent": _USER_AGENT,
        "checkpoint_interval": 100_000.0,
        "vacuum_interval": 100_000.0,
        "fetch_workers": 2,
        "project_workers": 2,
        "convert_workers": 1,
    }
    kwargs.update(overrides)
    return Config(**kwargs)


@pytest.fixture
def daemon_factory():
    created: list[Daemon] = []

    def _make(*args: Any, **kwargs: Any) -> Daemon:
        daemon = Daemon(*args, **kwargs)
        created.append(daemon)
        return daemon

    yield _make
    for daemon in created:
        if daemon.control_server is not None:
            daemon.shutdown()


def _fetchone(db_path: Path, sql: str, params: tuple[object, ...] = ()) -> tuple[Any, ...] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(sql, params).fetchone()
    finally:
        conn.close()


def _insert_wheel(
    db_path: Path,
    *,
    filename: str,
    project: str = "widget",
    url: str = "https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl",
    state: WheelState = WheelState.NEED_METADATA,
) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, serial, change_seq, updated_at) "
            "VALUES (?, ?, ?, 0, ?, 1, 1, '2024-01-01T00:00:00+00:00')",
            (filename, project, int(state), url),
        )
        conn.commit()
        assert cursor.lastrowid is not None
        return cursor.lastrowid
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# "No stage can starve another": the real HierarchicalLimiter's reserves
# ---------------------------------------------------------------------------


def test_metadata_backfill_saturating_files_pythonhosted_does_not_starve_index_poll(
    work_dir, socket_dir, daemon_factory
):
    """Proves it with the daemon's *actual* limiter instance and its real
    default rates (`files.pythonhosted.org` reserves 1800/min,
    `pypi.org` 200/min, out of a 2000/min global rate -- zero slack to
    borrow, so this only holds if each child's own reserve is untouchable).
    """
    daemon = daemon_factory(
        _config(work_dir, socket_dir),
        pypi_client=PyPIClient(_AlwaysGrantLimiter(), user_agent=_USER_AGENT),
    )
    daemon.start()

    limiter = daemon.limiter
    # Mark pypi.org as an active (not idle) contender first: HierarchicalLimiter
    # lets a child borrow the *entire* global bucket while every sibling has
    # gone unrequested for a while (by design, see ratelimit.py) -- a
    # dormant pypi.org would let files.pythonhosted.org legitimately borrow
    # its whole reserve too, which isn't the scenario this test is about.
    assert limiter.acquire("pypi.org", n=1, timeout=0.0) is True

    # Simulate a saturated metadata backfill: drain every token a sustained
    # run of fetch workers could plausibly acquire from files.pythonhosted.org,
    # immediately after pypi.org's own attempt above.
    drained = 0
    while limiter.acquire("files.pythonhosted.org", n=1, timeout=0.0):
        drained += 1
        if drained > 10_000:
            break
    assert drained > 0  # the drain loop actually did something

    # pypi.org's own reserve is untouched: index_poll can still acquire
    # immediately, with no wait at all.
    assert limiter.acquire("pypi.org", n=1, timeout=0.0) is True


# ---------------------------------------------------------------------------
# "A dependency outage pauses only its dependents": the key isolation test
# ---------------------------------------------------------------------------


def test_files_pythonhosted_outage_pauses_fetch_but_not_index_poll_or_bulk_convert(
    work_dir, socket_dir, daemon_factory
):
    index_poll_called = threading.Event()
    fetch_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "pypi.org":
            # Runs synchronously on index_poll's own stage thread: setting
            # the event here, rather than appending to a list a separate
            # thread polls, is itself the synchronization point.
            index_poll_called.set()
            return httpx.Response(
                200,
                headers={"content-type": ACCEPT_HEADER},
                json={"meta": {"_last-serial": 1}, "projects": []},
                request=request,
            )
        if host == "files.pythonhosted.org":
            fetch_calls.append(str(request.url))
            return httpx.Response(503, request=request)
        return httpx.Response(404, request=request)

    client = PyPIClient(
        _AlwaysGrantLimiter(), user_agent=_USER_AGENT, transport=httpx.MockTransport(handler)
    )
    daemon = daemon_factory(
        _config(work_dir, socket_dir, index_poll_interval=0.02), pypi_client=client
    )
    daemon.start()
    # Pause every stage immediately: nothing claims work until this test has
    # deterministically set up the outage and the seed data below.
    daemon.drain()

    breaker = daemon.breakers["files.pythonhosted.org"]
    for _ in range(5):
        breaker.record_failure()

    blocked_wheel_id = _insert_wheel(
        daemon.config.db_path,
        filename="blocked-1.0-py3-none-any.whl",
        state=WheelState.NEED_METADATA,
    )

    ready_wheel_id = _insert_wheel(
        daemon.config.db_path, filename="ready-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT
    )
    metadata = b"Metadata-Version: 2.1\nName: ready\nVersion: 1.0\n"
    location = daemon.archive_store.add(metadata)
    daemon.archive_store.seal_writer(daemon.archive_store.current_writer())
    conn = sqlite3.connect(str(daemon.config.db_path))
    conn.execute(
        "UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (location.sha256, ready_wheel_id)
    )
    conn.commit()
    conn.close()

    # Synchronize on convert actually applying *this* wheel's outcome,
    # instead of polling `wheels.state` from a separate thread: `on_applied`
    # fires only after `Dispatcher.apply_outcome` has already committed it.
    ready_wheel_applied = threading.Event()

    def _on_applied(queue_item, _outcome) -> None:
        if queue_item.id == ready_wheel_id:
            ready_wheel_applied.set()

    daemon._convert_stage._on_applied = _on_applied

    for name in daemon.stage_loops:
        daemon.resume_stage(name)

    # index_poll keeps running despite the files.pythonhosted.org outage.
    assert index_poll_called.wait(timeout=5.0)

    # bulk convert keeps running too: it never touches PyPI at all.
    assert ready_wheel_applied.wait(timeout=5.0)
    ready_row = _fetchone(
        daemon.config.db_path, "SELECT state FROM wheels WHERE id = ?", (ready_wheel_id,)
    )
    assert ready_row is not None
    assert ready_row[0] == int(WheelState.READY)

    # fetch, meanwhile, never even attempted a network call: the open
    # breaker gates it before dispatch, not after a failed request.
    assert fetch_calls == []
    blocked_row = _fetchone(
        daemon.config.db_path, "SELECT state FROM wheels WHERE id = ?", (blocked_wheel_id,)
    )
    assert blocked_row is not None
    assert blocked_row[0] == int(WheelState.NEED_METADATA)
