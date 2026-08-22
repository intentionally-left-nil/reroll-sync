"""Tests for the `Daemon` service: startup ordering, shutdown sequencing,
control-command wiring, and the acceptance-level isolation/starvation
tests specs/10-daemon-and-control.md calls for.

Real unix sockets are used for the control protocol (per this spec's own
guidance); everything else runs against a real (tmp-dir) sqlite database
and segment directory, but with injected clocks and small, fast intervals
-- no test sleeps.
"""

from __future__ import annotations

import json
import socket
import sqlite3
import tempfile
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from reroll_sync.daemon.config import Config
from reroll_sync.daemon.service import SHUTDOWN_GRACE_SECONDS, Daemon
from reroll_sync.db import SchemaMismatchError, init_db
from reroll_sync.dispatcher import QueueItem
from reroll_sync.fetch import ByteBudgetedQueue, HandoffItem
from reroll_sync.pypi_client import PyPIClient, PyPITransientError
from reroll_sync.schema import WheelState

_USER_AGENT = "reroll-sync-test (contact@example.invalid)"

_ASYNC_SHUTDOWN_WAIT_TIMEOUT = SHUTDOWN_GRACE_SECONDS + 5.0
"""Bound for waiting on a shutdown triggered on a background thread (the
control socket's `shutdown` command, or a signal handler) rather than
called directly: `Daemon.shutdown` itself has no upper bound tighter than
`SHUTDOWN_GRACE_SECONDS`, so a wait bound below that -- 5s, say -- can
flake under a loaded CI runner even though the daemon is behaving exactly
as documented.
"""


@pytest.fixture
def work_dir(tmp_path):
    return tmp_path


@pytest.fixture
def socket_dir():
    # macOS caps AF_UNIX paths well below pytest's default (deeply nested)
    # tmp_path.
    import shutil

    path = Path(tempfile.mkdtemp(prefix="rs-daemon-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


def _config(work_dir, socket_dir, **overrides: Any) -> Config:
    kwargs: dict[str, Any] = {
        "db_path": work_dir / "reroll_sync.db",
        "segments_dir": work_dir / "segments",
        "socket_path": socket_dir / "control.sock",
        "user_agent": _USER_AGENT,
        "index_poll_interval": 100_000.0,  # timer stages: never due mid-test
        "checkpoint_interval": 100_000.0,
        "vacuum_interval": 100_000.0,
        "fetch_workers": 2,
        "project_workers": 2,
        "convert_workers": 1,
    }
    kwargs.update(overrides)
    return Config(**kwargs)


def _offline_pypi_client() -> PyPIClient:
    """A `PyPIClient` that never touches the real network: every request
    404s. Real stage loops start running the moment `Daemon.start()`
    returns, so any test that leaves a wheel claimable would otherwise
    make a real network call against pypi.org/files.pythonhosted.org.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    class _AlwaysGrantLimiter:
        def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
            return True

        def penalize(self, child_name: str, seconds: float) -> None:
            pass

    return PyPIClient(
        _AlwaysGrantLimiter(), user_agent=_USER_AGENT, transport=httpx.MockTransport(handler)
    )


def _daemons():
    """Track daemons created by a test so they're always shut down."""
    created: list[Daemon] = []

    def _make(*args, **kwargs) -> Daemon:
        kwargs.setdefault("pypi_client", _offline_pypi_client())
        daemon = Daemon(*args, **kwargs)
        created.append(daemon)
        return daemon

    return created, _make


@pytest.fixture(autouse=True)
def _fast_convert_pool(monkeypatch):
    """`Daemon._build_convert` normally spins up a real `ProcessPoolExecutor`
    (a subprocess fork); every test in this module cares about wiring and
    lifecycle, not actual conversion, so a thread pool is a much faster
    stand-in. `worker_init` is a no-op here since nothing in this module
    calls `convert_in_worker`.
    """
    from concurrent.futures import ThreadPoolExecutor

    def _fake_process_pool(max_workers=None, initializer=None, initargs=()):
        del initializer, initargs  # would call reroll.default_mappers() over the network
        return ThreadPoolExecutor(max_workers=max_workers)

    monkeypatch.setattr("reroll_sync.daemon.service.ProcessPoolExecutor", _fake_process_pool)


@pytest.fixture
def daemon_factory():
    created, make = _daemons()
    yield make
    for daemon in created:
        if daemon.control_server is not None:
            daemon.shutdown()


def _request(socket_path: Path, payload: dict) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(5.0)
        sock.connect(str(socket_path))
        sock.sendall(json.dumps(payload).encode() + b"\n")
        buf = bytearray()
        while b"\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf.extend(chunk)
        line, _, _rest = bytes(buf).partition(b"\n")
        return json.loads(line)


def _fetchone(db_path, sql, params=()):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(sql, params).fetchone()
    finally:
        conn.close()


def _insert_wheel(
    db_path,
    *,
    filename: str,
    project: str = "widget",
    url: str = "https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl",
    metadata_sha256: str | None = None,
    blob_sha256: str | None = None,
    state: WheelState = WheelState.NEED_METADATA,
) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, metadata_sha256, blob_sha256, "
            "serial, change_seq, updated_at) "
            "VALUES (?, ?, ?, 0, ?, ?, ?, 1, 1, '2024-01-01T00:00:00+00:00')",
            (filename, project, int(state), url, metadata_sha256, blob_sha256),
        )
        conn.commit()
        assert cursor.lastrowid is not None
        return cursor.lastrowid
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Startup ordering
# ---------------------------------------------------------------------------


def test_schema_mismatch_aborts_startup_before_any_stage_starts(
    work_dir, socket_dir, daemon_factory
):
    db_path = work_dir / "reroll_sync.db"
    init_db(db_path)
    # Corrupt the schema: drop a column the declared schema expects.
    conn = sqlite3.connect(str(db_path))
    conn.execute("DROP TABLE wheels")
    conn.execute("CREATE TABLE wheels (id INTEGER PRIMARY KEY, filename TEXT NOT NULL UNIQUE)")
    conn.commit()
    conn.close()

    daemon = daemon_factory(_config(work_dir, socket_dir))

    with pytest.raises(SchemaMismatchError):
        daemon.start()

    assert daemon.stage_loops == {}
    assert daemon.control_server is None


def test_archive_recovery_runs_before_any_stage_claims_work(work_dir, socket_dir, daemon_factory):
    """A stale `.open` segment from a previous crash must be recovered
    before fetch/convert can claim anything -- proven here by a wheel whose
    blob lived in that segment ending up back at NEED_METADATA, never
    claimed out from under recovery.
    """
    init_db(work_dir / "reroll_sync.db")
    segments_dir = work_dir / "segments"
    segments_dir.mkdir()

    # Simulate a crash: a segment file with data but no sealed footer/row.
    (segments_dir / "000000.open").write_bytes(b"partial, unsealed data")

    wheel_id = _insert_wheel(
        work_dir / "reroll_sync.db",
        filename="widget-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
    )
    conn = sqlite3.connect(str(work_dir / "reroll_sync.db"))
    conn.execute("INSERT INTO segments (id) VALUES (0)")
    conn.execute(
        "INSERT INTO blobs (sha256, segment_id, block_no, offset, length) VALUES (?, 0, 0, 0, 4)",
        ("a" * 64,),
    )
    conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", ("a" * 64, wheel_id))
    conn.commit()
    conn.close()

    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    assert daemon.recovered_segment_ids == [0]
    row = _fetchone(
        work_dir / "reroll_sync.db",
        "SELECT state, blob_sha256 FROM wheels WHERE id = ?",
        (wheel_id,),
    )
    assert row == (int(WheelState.NEED_METADATA), None)
    blob_row = _fetchone(
        work_dir / "reroll_sync.db", "SELECT 1 FROM blobs WHERE sha256 = ?", ("a" * 64,)
    )
    assert blob_row is None


def test_socket_answers_status_even_when_a_stage_fails_to_start(
    work_dir, socket_dir, daemon_factory, monkeypatch
):
    daemon = daemon_factory(_config(work_dir, socket_dir))

    def _broken_convert():
        raise RuntimeError("simulated convert-stage construction failure")

    monkeypatch.setattr(daemon, "_build_convert", _broken_convert)
    daemon.start()

    assert "convert" in daemon.stage_start_errors
    # fetch depends on convert's on_archived callback, so it cascades --
    # but gc, an unrelated stage, must still have started.
    assert "gc" in daemon.stage_loops

    response = _request(daemon.config.socket_path, {"command": "status"})
    assert response["ok"] is True
    assert "convert" in response["result"]["stage_start_errors"]


def test_a_sealed_segment_is_never_recovered(work_dir, socket_dir, daemon_factory):
    init_db(work_dir / "reroll_sync.db")
    segments_dir = work_dir / "segments"
    segments_dir.mkdir()
    conn = sqlite3.connect(str(work_dir / "reroll_sync.db"))
    conn.execute(
        "INSERT INTO segments (id, sealed_at, bytes, records, footer_sha) "
        "VALUES (0, '2024-01-01T00:00:00+00:00', 100, 1, 'x')"
    )
    conn.commit()
    conn.close()

    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    assert daemon.recovered_segment_ids == []


def test_two_simultaneous_stale_open_segments_are_both_recovered(
    work_dir, socket_dir, daemon_factory
):
    """A hypothetical double-crash (or two segments open across one crash)
    must recover *both* stale `.open` segments, not just the single-segment
    case the tests above cover.
    """
    init_db(work_dir / "reroll_sync.db")
    segments_dir = work_dir / "segments"
    segments_dir.mkdir()

    (segments_dir / "000000.open").write_bytes(b"partial, unsealed data 0")
    (segments_dir / "000001.open").write_bytes(b"partial, unsealed data 1")

    wheel_id_0 = _insert_wheel(
        work_dir / "reroll_sync.db",
        filename="widget-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
    )
    wheel_id_1 = _insert_wheel(
        work_dir / "reroll_sync.db",
        filename="gadget-1.0-py3-none-any.whl",
        project="gadget",
        state=WheelState.NEED_CONVERT,
    )
    conn = sqlite3.connect(str(work_dir / "reroll_sync.db"))
    conn.execute("INSERT INTO segments (id) VALUES (0)")
    conn.execute("INSERT INTO segments (id) VALUES (1)")
    conn.execute(
        "INSERT INTO blobs (sha256, segment_id, block_no, offset, length) VALUES (?, 0, 0, 0, 4)",
        ("a" * 64,),
    )
    conn.execute(
        "INSERT INTO blobs (sha256, segment_id, block_no, offset, length) VALUES (?, 1, 0, 0, 4)",
        ("b" * 64,),
    )
    conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", ("a" * 64, wheel_id_0))
    conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", ("b" * 64, wheel_id_1))
    conn.commit()
    conn.close()

    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    assert daemon.recovered_segment_ids == [0, 1]
    for wheel_id in (wheel_id_0, wheel_id_1):
        row = _fetchone(
            work_dir / "reroll_sync.db",
            "SELECT state, blob_sha256 FROM wheels WHERE id = ?",
            (wheel_id,),
        )
        assert row == (int(WheelState.NEED_METADATA), None)
    for sha256 in ("a" * 64, "b" * 64):
        blob_row = _fetchone(
            work_dir / "reroll_sync.db", "SELECT 1 FROM blobs WHERE sha256 = ?", (sha256,)
        )
        assert blob_row is None


# ---------------------------------------------------------------------------
# Disk guard (Fix 1): wired into a periodic timer, gates fetch claiming, and
# defers (never drops) an archive append while paused.
# ---------------------------------------------------------------------------


def test_disk_guard_stage_checks_on_its_own_timer(work_dir, socket_dir, daemon_factory):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    assert "disk_guard" in daemon.stage_loops
    checked = []
    daemon.disk_guard.check = lambda: checked.append(1)

    did_work = daemon.stage_loops["disk_guard"].run_once()

    assert did_work is False  # nothing to report; it's a pure side-effecting timer
    assert checked == [1]


def test_disk_guard_pause_blocks_fetch_defers_archive_append_then_resumes(
    work_dir, socket_dir, daemon_factory
):
    """The end-to-end scenario Fix 1 requires: a real `Daemon`, an injected
    fake disk-space-check function reporting free space below the floor,
    confirming `fetch` claims nothing and a queued archive append is
    deferred (never processed) while paused, then space recovering above
    `floor * hysteresis` and confirming both resume.

    Synchronized with a test-only hook (`_on_archive_disk_pause`), not a
    sleep or a poll: it fires every time the archive thread observes the
    pause, so waiting on its `Event` proves the archive thread has *seen*
    the paused state (and therefore cannot have raced ahead and processed
    the queued item) before this test inspects the queue.
    """
    daemon = daemon_factory(_config(work_dir, socket_dir, disk_free_floor_bytes=1000))
    daemon.start()

    paused_observed = threading.Event()
    daemon._on_archive_disk_pause = paused_observed.set
    daemon.disk_guard._disk_usage = lambda _path: (0, 0, 500)  # below the 1000-byte floor
    assert daemon.disk_guard.check() is True

    # fetch claims nothing while paused.
    wheel_id = _insert_wheel(daemon.config.db_path, filename="widget-1.0-py3-none-any.whl")
    assert daemon._fetch_stage.iterate() is False
    row = _fetchone(daemon.config.db_path, "SELECT state FROM wheels WHERE id = ?", (wheel_id,))
    assert row[0] == int(WheelState.NEED_METADATA)  # never claimed

    # The archive thread defers a queued append rather than processing it:
    # wait until it has actually observed the pause before checking.
    assert paused_observed.wait(timeout=5.0)
    data = b"Metadata-Version: 2.1\nName: widget\nVersion: 1.0\n"
    handoff = HandoffItem(
        queue_item=QueueItem(id=wheel_id, project="widget", lane=0, state=WheelState.NEED_METADATA),
        filename="widget-1.0-py3-none-any.whl",
        data=data,
        sha256="unused-in-this-test",
    )
    daemon.handoff_queue.put(handoff, size=len(data))
    # Give the (still-paused) archive thread several more chances to have
    # wrongly claimed the item if it were going to; it must not.
    for _ in range(5):
        assert paused_observed.wait(timeout=5.0)
        paused_observed.clear()
    assert len(daemon.handoff_queue) == 1  # still queued, never popped

    # Space recovers above floor * hysteresis: fetch and archive resume.
    processed = threading.Event()
    daemon._on_archive_item_processed = processed.set
    daemon.disk_guard._disk_usage = lambda _path: (0, 0, 10_000)
    assert daemon.disk_guard.check() is False

    assert processed.wait(timeout=5.0)
    assert len(daemon.handoff_queue) == 0  # the deferred item was processed once resumed
    row = _fetchone(daemon.config.db_path, "SELECT state FROM wheels WHERE id = ?", (wheel_id,))
    assert row[0] == int(WheelState.NEED_CONVERT)  # its Ok outcome was applied normally


def test_archive_thread_exits_when_handoff_queue_closes_independently_of_shutdown(
    work_dir, socket_dir, daemon_factory
):
    """`Daemon.shutdown`'s own ordering always sets `shutdown_event` before
    closing `handoff_queue`, so the archive thread normally exits via the
    outer `while not shutdown_event.is_set()` check. This is the loop's
    other, independent exit condition -- closing the queue without
    signaling shutdown at all -- covering any caller that closes the queue
    on its own.
    """
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    assert daemon.archive_thread is not None
    assert daemon.archive_thread.is_alive()

    daemon.handoff_queue.close()

    daemon.archive_thread.join(timeout=5.0)
    assert not daemon.archive_thread.is_alive()
    assert not daemon.shutdown_event.is_set()  # confirms this was not the shutdown path


def test_archive_thread_step_paused_with_no_hook_installed_just_waits(
    work_dir, socket_dir, daemon_factory
):
    """`_on_archive_disk_pause` is `None` in production; this drives
    `_archive_thread_step` directly (after stopping the real background
    archive thread, so nothing else is concurrently calling it) to
    exercise that branch deterministically, with no test hook involved at
    all.
    """
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    daemon.handoff_queue.close()
    daemon.archive_thread.join(timeout=5.0)
    assert not daemon.archive_thread.is_alive()

    daemon.disk_guard._disk_usage = lambda _path: (0, 0, 0)
    assert daemon.disk_guard.check() is True
    assert daemon._on_archive_disk_pause is None

    result = daemon._archive_thread_step()

    assert result is True  # paused: told to keep looping, after a bounded wait


def test_archive_thread_step_stops_once_shutdown_is_signaled_while_disk_stays_paused(
    work_dir, socket_dir, daemon_factory
):
    """A paused `disk_guard` never lets the archive thread drain (it must
    never write to disk while paused); once shutdown has also been
    signaled, waiting for space to free up would just hang the shutdown
    sequence forever, so the loop gives up instead of looping again.
    """
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    daemon.handoff_queue.close()
    daemon.archive_thread.join(timeout=5.0)
    assert not daemon.archive_thread.is_alive()

    daemon.disk_guard._disk_usage = lambda _path: (0, 0, 0)
    assert daemon.disk_guard.check() is True
    daemon.shutdown_event.set()

    result = daemon._archive_thread_step()

    assert result is False  # gives up rather than waiting for space that may never free up


def test_archive_thread_processes_a_queued_item_with_no_test_hook_installed(
    work_dir, socket_dir, daemon_factory
):
    """The normal-operation path -- no disk-guard pressure, and neither
    test hook installed -- needs its own coverage too: `_on_archive_*`
    hooks are always `None` in production. Synchronized via a wrapper
    around `ArchiveHandoff`'s own (pre-existing) `on_archived` callback
    instead of this module's test-only hooks, so the hooks genuinely stay
    unset for the duration of the assertion.
    """
    daemon = daemon_factory(_config(work_dir, socket_dir, disk_free_floor_bytes=1))
    daemon.start()
    assert daemon._on_archive_disk_pause is None
    assert daemon._on_archive_item_processed is None
    wheel_id = _insert_wheel(daemon.config.db_path, filename="widget-1.0-py3-none-any.whl")

    done = threading.Event()
    original_on_archived = daemon.archive_handoff._on_archived

    def _wrapped(*args):
        original_on_archived(*args)
        done.set()

    daemon.archive_handoff._on_archived = _wrapped

    data = b"Metadata-Version: 2.1\nName: widget\nVersion: 1.0\n"
    handoff = HandoffItem(
        queue_item=QueueItem(id=wheel_id, project="widget", lane=0, state=WheelState.NEED_METADATA),
        filename="widget-1.0-py3-none-any.whl",
        data=data,
        sha256="unused-in-this-test",
    )
    daemon.handoff_queue.put(handoff, size=len(data))

    assert done.wait(timeout=5.0)
    row = _fetchone(daemon.config.db_path, "SELECT state FROM wheels WHERE id = ?", (wheel_id,))
    assert row[0] == int(WheelState.NEED_CONVERT)


# ---------------------------------------------------------------------------
# fatal_exceptions wiring (Fix 2): every real stage crashes loudly on a
# programming/configuration error but keeps looping on an ordinary,
# operational failure.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stage_name", ["project_sync", "index_poll", "convert", "fetch", "gc"])
def test_fatal_exception_propagates_for_every_real_stage(
    work_dir, socket_dir, daemon_factory, stage_name
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    loop = daemon.stage_loops[stage_name]
    loop._iterate = lambda: (_ for _ in ()).throw(TypeError("simulated programming error"))

    with pytest.raises(TypeError, match="simulated programming error"):
        loop.run_once()


@pytest.mark.parametrize("stage_name", ["project_sync", "index_poll", "convert", "fetch", "gc"])
def test_ordinary_pypi_transient_error_is_still_caught_for_every_real_stage(
    work_dir, socket_dir, daemon_factory, stage_name
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    loop = daemon.stage_loops[stage_name]
    loop._iterate = lambda: (_ for _ in ()).throw(PyPITransientError("simulated network blip"))

    did_work = loop.run_once()

    assert did_work is False
    assert loop.stats().consecutive_failures == 1


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


def test_shutdown_removes_the_socket_file(work_dir, socket_dir, daemon_factory):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    socket_path = daemon.config.socket_path
    assert socket_path.exists()

    daemon.shutdown()

    assert not socket_path.exists()


def test_shutdown_stops_the_writer_and_drains_queued_ops(work_dir, socket_dir, daemon_factory):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    from reroll_sync.writer import WriteOp

    done = threading.Event()

    def _apply(conn):
        conn.execute(
            "INSERT INTO errors (error_category, reroll_version, created_at) "
            "VALUES ('x', '1.0', '2024-01-01T00:00:00+00:00')"
        )
        done.set()

    daemon.writer.submit(WriteOp(name="test-op", apply=_apply))
    daemon.shutdown()

    assert done.is_set()
    (count,) = _fetchone(daemon.config.db_path, "SELECT COUNT(*) FROM errors")
    assert count == 1


def test_shutdown_does_not_seal_the_open_segment(work_dir, socket_dir, daemon_factory):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    daemon.archive_store.current_writer()  # ensure a segment is open

    daemon.shutdown()

    open_files = list((work_dir / "segments").glob("*.open"))
    assert len(open_files) == 1


def test_shutdown_does_not_lose_or_error_on_an_in_flight_successful_fetch(
    work_dir, socket_dir, daemon_factory, capsys
):
    """A fetch batch already in flight (a real thread-pool worker blocked
    mid-HTTP-response) when `shutdown` runs must still be handed off to
    `handoff_queue` successfully: `shutdown` has to join the fetch stage's
    thread -- the only producer onto that queue -- before closing it, or
    the fetch stage's own `put()` call raises `QueueClosed`, silently
    dropping the already-downloaded bytes and logging what looks like an
    unhandled exception for a perfectly ordinary shutdown race.

    `capsys`, not `caplog`: `configure_logging` disables propagation to the
    root logger (see `logging_setup.py`), so pytest's `caplog` never sees
    daemon log records once a `Daemon` has started.

    Deterministic via `threading.Event`s, not sleeps: `started` confirms
    the worker thread is genuinely inside the request before shutdown
    begins; `closed` (a monkeypatched `handoff_queue.close`) is given a
    short bounded window to fire *before* the response is released --
    long enough for the pre-fix implementation's unconditional early
    `close()` to always win that race, but otherwise elapsing harmlessly
    once `close()` is correctly deferred.
    """
    started = threading.Event()
    closed = threading.Event()
    release = threading.Event()
    data = b"Metadata-Version: 2.1\nName: widget\nVersion: 1.0\n"

    def handler(request: httpx.Request) -> httpx.Response:
        if "files.pythonhosted.org" not in str(request.url):
            return httpx.Response(404, request=request)  # e.g. index_poll's own pypi.org probe
        started.set()
        assert release.wait(timeout=5.0)
        return httpx.Response(200, content=data, request=request)

    class _AlwaysGrantLimiter:
        def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
            return True

        def penalize(self, child_name: str, seconds: float) -> None:
            pass

    client = PyPIClient(
        _AlwaysGrantLimiter(), user_agent=_USER_AGENT, transport=httpx.MockTransport(handler)
    )

    daemon = daemon_factory(_config(work_dir, socket_dir), pypi_client=client)
    daemon.start()
    wheel_id = _insert_wheel(daemon.config.db_path, filename="widget-1.0-py3-none-any.whl")

    real_close = daemon.handoff_queue.close

    def _close() -> None:
        real_close()
        closed.set()

    daemon.handoff_queue.close = _close  # type: ignore[method-assign]

    assert started.wait(timeout=5.0)  # the fetch worker is genuinely mid-request

    shutdown_thread = threading.Thread(target=daemon.shutdown)
    shutdown_thread.start()
    closed.wait(timeout=0.5)  # give a premature close() every chance to happen first
    release.set()
    shutdown_thread.join(timeout=10.0)

    assert not shutdown_thread.is_alive()
    out = capsys.readouterr().out
    assert "QueueClosed" not in out
    assert "unhandled exception in iteration" not in out

    row = _fetchone(
        daemon.config.db_path, "SELECT state, blob_sha256 FROM wheels WHERE id = ?", (wheel_id,)
    )
    assert row[0] == int(WheelState.NEED_CONVERT)  # archived, not silently dropped
    assert row[1] is not None


def test_shutdown_signaled_skips_seal_even_if_threshold_crossed_during_drain(
    work_dir, socket_dir, daemon_factory
):
    """`ArchiveHandoff._maybe_seal` still runs during the shutdown drain --
    the archive thread keeps processing already-queued items while
    `Daemon.shutdown` joins it -- but must never actually seal: spec 10's
    shutdown step 4 is "seal nothing", not "seal only if there's time".

    Forces `should_seal()` to report `True` (as if the segment had just
    crossed its size/time threshold) and confirms no seal happens once
    `shutdown_event` is set: the segment stays `.open`, and no `segments`
    row update is ever submitted -- while the item's own blob/state writes
    still complete normally.
    """
    daemon = daemon_factory(_config(work_dir, socket_dir, disk_free_floor_bytes=1))
    daemon.start()
    wheel_id = _insert_wheel(daemon.config.db_path, filename="widget-1.0-py3-none-any.whl")

    real_writer = daemon.archive_store.current_writer()
    segment_id = real_writer.segment_id
    real_writer.should_seal = lambda: True  # simulate the threshold having been crossed

    daemon.shutdown_event.set()  # simulate: mid-shutdown drain, before joining the thread

    data = b"Metadata-Version: 2.1\nName: widget\nVersion: 1.0\n"
    handoff = HandoffItem(
        queue_item=QueueItem(id=wheel_id, project="widget", lane=0, state=WheelState.NEED_METADATA),
        filename="widget-1.0-py3-none-any.whl",
        data=data,
        sha256="unused-in-this-test",
    )
    daemon.handoff_queue.put(handoff, size=len(data))

    did_work = daemon.archive_handoff.process_one()

    assert did_work is True  # the item's own blob/state writes still completed normally
    row = _fetchone(
        daemon.config.db_path, "SELECT state, blob_sha256 FROM wheels WHERE id = ?", (wheel_id,)
    )
    assert row[0] == int(WheelState.NEED_CONVERT)
    assert row[1] is not None

    sealed_row = _fetchone(
        daemon.config.db_path, "SELECT sealed_at FROM segments WHERE id = ?", (segment_id,)
    )
    assert sealed_row == (None,)  # never sealed
    open_files = list((work_dir / "segments").glob(f"{segment_id:06d}.open"))
    assert len(open_files) == 1  # the segment file itself stays `.open`


def test_shutdown_is_idempotent(work_dir, socket_dir, daemon_factory):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    daemon.shutdown()
    daemon.shutdown()  # must not raise or hang


def test_stale_socket_file_from_a_previous_crash_is_replaced(work_dir, socket_dir, daemon_factory):
    socket_path = socket_dir / "control.sock"
    socket_path.write_text("stale")

    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    response = _request(socket_path, {"command": "status"})
    assert response["ok"] is True


def test_second_signal_during_shutdown_calls_os_exit(
    work_dir, socket_dir, daemon_factory, monkeypatch
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    daemon._shutting_down = True  # simulate: already mid-shutdown

    exit_calls = []
    monkeypatch.setattr("os._exit", lambda code: exit_calls.append(code))
    daemon.install_signal_handlers()

    import signal as signal_module

    handler = cast("Callable[[int, object], None]", signal_module.getsignal(signal_module.SIGTERM))
    handler(signal_module.SIGTERM, None)

    assert exit_calls == [1]
    daemon._shutting_down = False  # let the fixture's real shutdown() run


# ---------------------------------------------------------------------------
# Control commands, end to end
# ---------------------------------------------------------------------------


def test_pause_resume_via_control_socket_changes_stage_behaviour(
    work_dir, socket_dir, daemon_factory
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    response = _request(daemon.config.socket_path, {"command": "pause", "args": {"stage": "fetch"}})
    assert response["ok"] is True
    assert daemon.stage_loops["fetch"].is_paused() is True

    response = _request(
        daemon.config.socket_path, {"command": "resume", "args": {"stage": "fetch"}}
    )
    assert response["ok"] is True
    assert daemon.stage_loops["fetch"].is_paused() is False


def test_drain_via_control_socket_pauses_every_stage(work_dir, socket_dir, daemon_factory):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    response = _request(daemon.config.socket_path, {"command": "drain"})
    assert response["ok"] is True
    assert all(loop.is_paused() for loop in daemon.stage_loops.values())


def test_reprocess_via_control_socket_returns_committed_affected_count(
    work_dir, socket_dir, daemon_factory
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    wheel_id = _insert_wheel(
        daemon.config.db_path, filename="widget-1.0-py3-none-any.whl", state=WheelState.SKIPPED
    )

    response = _request(
        daemon.config.socket_path, {"command": "reprocess", "args": {"type": "skipped_only"}}
    )

    assert response == {"ok": True, "result": {"affected": 1}}
    row = _fetchone(daemon.config.db_path, "SELECT state FROM wheels WHERE id = ?", (wheel_id,))
    assert row[0] == int(WheelState.NEED_CONVERT)


def test_unquarantine_via_control_socket_returns_committed_affected_count(
    work_dir, socket_dir, daemon_factory
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    wheel_id = _insert_wheel(
        daemon.config.db_path, filename="widget-1.0-py3-none-any.whl", state=WheelState.QUARANTINED
    )

    response = _request(
        daemon.config.socket_path,
        {"command": "unquarantine", "args": {"type": "state", "state": "QUARANTINED"}},
    )

    assert response == {"ok": True, "result": {"affected": 1}}
    row = _fetchone(daemon.config.db_path, "SELECT state FROM wheels WHERE id = ?", (wheel_id,))
    assert row[0] == int(WheelState.NEED_METADATA)


def test_shutdown_via_control_socket_eventually_stops_the_daemon(
    work_dir, socket_dir, daemon_factory
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    response = _request(daemon.config.socket_path, {"command": "shutdown"})
    assert response["ok"] is True

    assert daemon._stopped_event.wait(timeout=_ASYNC_SHUTDOWN_WAIT_TIMEOUT)
    assert not daemon.config.socket_path.exists()


def test_index_poll_cascades_when_project_sync_fails_to_start(
    work_dir, socket_dir, daemon_factory, monkeypatch
):
    daemon = daemon_factory(_config(work_dir, socket_dir))

    def _broken_project_sync():
        raise RuntimeError("simulated project_sync-stage construction failure")

    monkeypatch.setattr(daemon, "_build_project_sync", _broken_project_sync)
    daemon.start()

    assert "project_sync" in daemon.stage_start_errors
    assert "index_poll" in daemon.stage_start_errors
    assert "project_sync stage did not start" in daemon.stage_start_errors["index_poll"]
    assert "gc" in daemon.stage_loops


def test_archive_thread_crash_is_logged_and_reraised(work_dir, socket_dir, daemon_factory, capsys):
    """`configure_logging` disables propagation to the root logger (see
    `logging_setup.py`), so pytest's `caplog` never sees daemon log
    records once a `Daemon` has started; captured stdout (this module's
    JSON lines) is the only way to assert on them here.

    `_run_archive_thread` calls `archive_handoff.process_one` directly
    (gating each pop on `disk_guard.is_paused()` and the queue actually
    holding something), not `.run()`, so that's what's monkeypatched to
    simulate the crash -- with an item queued first, since the loop never
    calls `process_one` at all while the queue is empty.

    The real background archive thread is stopped first and given a fresh
    queue: otherwise it would race this test's own direct
    `_run_archive_thread()` call to pop the one queued item, and might
    call the (already-broken) `process_one` itself, crashing on its own
    thread instead of (or in addition to) the one this test asserts on.
    """
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    daemon.handoff_queue.close()
    daemon.archive_thread.join(timeout=5.0)
    assert not daemon.archive_thread.is_alive()

    daemon.handoff_queue = ByteBudgetedQueue(budget_bytes=10_000)
    daemon.handoff_queue.put(
        HandoffItem(
            queue_item=QueueItem(id=1, project="widget", lane=0, state=WheelState.NEED_METADATA),
            filename="widget-1.0-py3-none-any.whl",
            data=b"x",
            sha256="a" * 64,
        ),
        size=1,
    )

    def _broken_process_one():
        raise RuntimeError("disk exploded")

    daemon.archive_handoff.process_one = _broken_process_one

    with pytest.raises(RuntimeError, match="disk exploded"):
        daemon._run_archive_thread()

    assert "archive thread crashed" in capsys.readouterr().out


def test_first_signal_triggers_shutdown_on_a_background_thread(
    work_dir, socket_dir, daemon_factory
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    daemon.install_signal_handlers()

    import signal as signal_module

    handler = cast("Callable[[int, object], None]", signal_module.getsignal(signal_module.SIGTERM))
    handler(signal_module.SIGTERM, None)

    assert daemon._stopped_event.wait(timeout=_ASYNC_SHUTDOWN_WAIT_TIMEOUT)


def test_run_forever_blocks_until_shutdown_completes(work_dir, socket_dir, daemon_factory):
    daemon = daemon_factory(_config(work_dir, socket_dir))

    thread = threading.Thread(target=daemon.run_forever)
    thread.start()
    try:
        # Wait for start() (run inside run_forever) to actually complete --
        # `Event.wait(timeout=...)` returns the instant it's set, not a poll.
        assert daemon._started_event.wait(timeout=5.0)
        assert daemon.control_server is not None

        daemon.shutdown()
        thread.join(timeout=5.0)
        assert not thread.is_alive()
    finally:
        if thread.is_alive():
            daemon.shutdown_event.set()
            thread.join(timeout=5.0)


def test_shutdown_when_control_server_never_started_does_not_crash(
    work_dir, socket_dir, daemon_factory, monkeypatch
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    monkeypatch.setattr(
        "reroll_sync.daemon.service.ControlServer.start",
        lambda self: (_ for _ in ()).throw(OSError("bind failed")),
    )

    with pytest.raises(OSError, match="bind failed"):
        daemon.start()

    assert daemon.control_server is None
    daemon.shutdown()  # must not raise even though the control server never started


def test_disk_breaker_guarded_store_gates_add_through_the_breaker():
    from reroll_sync.archive.location import BlobLocation
    from reroll_sync.daemon.circuit_breaker import CircuitBreaker, CircuitState
    from reroll_sync.daemon.service import _DiskBreakerGuardedStore

    _LOCATION = BlobLocation(sha256="a" * 64, segment_id=0, block_no=0, offset=0, length=4)

    class _FakeStore:
        def __init__(self) -> None:
            self.added: list[bytes] = []
            self.other_calls: list[str] = []

        def add(self, data: bytes) -> BlobLocation:
            self.added.append(data)
            return _LOCATION

        def current_writer(self) -> str:
            self.other_calls.append("current_writer")
            return "writer"

    store = _FakeStore()
    breaker = CircuitBreaker(failure_threshold=1)
    guarded = _DiskBreakerGuardedStore(store, breaker)

    assert guarded.add(b"data") == _LOCATION
    assert store.added == [b"data"]
    assert breaker.state() == CircuitState.CLOSED

    # Passthrough for everything else, via __getattr__.
    assert guarded.current_writer() == "writer"
    assert store.other_calls == ["current_writer"]


def test_disk_breaker_guarded_store_records_failures_on_write_errors():
    from reroll_sync.archive.location import BlobLocation
    from reroll_sync.daemon.circuit_breaker import CircuitBreaker, CircuitState
    from reroll_sync.daemon.service import _DiskBreakerGuardedStore

    class _BrokenStore:
        def add(self, data: bytes) -> BlobLocation:
            raise OSError("disk full")

        def current_writer(self) -> None:
            raise NotImplementedError

    breaker = CircuitBreaker(failure_threshold=1)
    guarded = _DiskBreakerGuardedStore(_BrokenStore(), breaker)

    with pytest.raises(OSError, match="disk full"):
        guarded.add(b"data")

    assert breaker.state() == CircuitState.OPEN


def test_disk_breaker_guarded_store_skips_add_when_disk_guard_is_paused():
    from reroll_sync.archive.location import BlobLocation
    from reroll_sync.daemon.circuit_breaker import CircuitBreaker, CircuitState
    from reroll_sync.daemon.disk_guard import DiskGuardPausedError
    from reroll_sync.daemon.service import _DiskBreakerGuardedStore

    class _FakeStore:
        def __init__(self) -> None:
            self.added: list[bytes] = []

        def add(self, data: bytes) -> BlobLocation:
            self.added.append(data)
            return BlobLocation(sha256="a" * 64, segment_id=0, block_no=0, offset=0, length=4)

        def current_writer(self) -> None:
            raise NotImplementedError

    class _PausedDiskGuard:
        def is_paused(self) -> bool:
            return True

    store = _FakeStore()
    breaker = CircuitBreaker(failure_threshold=1)
    guarded = _DiskBreakerGuardedStore(store, breaker, disk_guard=_PausedDiskGuard())

    with pytest.raises(DiskGuardPausedError):
        guarded.add(b"data")

    assert store.added == []  # the real store was never touched
    assert breaker.state() == CircuitState.CLOSED  # not counted as a breaker failure


def test_disk_breaker_guarded_store_add_proceeds_when_disk_guard_is_not_paused():
    from reroll_sync.archive.location import BlobLocation
    from reroll_sync.daemon.circuit_breaker import CircuitBreaker
    from reroll_sync.daemon.service import _DiskBreakerGuardedStore

    _LOCATION = BlobLocation(sha256="a" * 64, segment_id=0, block_no=0, offset=0, length=4)

    class _FakeStore:
        def add(self, data: bytes) -> BlobLocation:
            return _LOCATION

        def current_writer(self) -> None:
            raise NotImplementedError

    class _NotPausedDiskGuard:
        def is_paused(self) -> bool:
            return False

    guarded = _DiskBreakerGuardedStore(
        _FakeStore(), CircuitBreaker(failure_threshold=1), disk_guard=_NotPausedDiskGuard()
    )

    assert guarded.add(b"data") == _LOCATION


def test_disk_breaker_guarded_store_passes_through_other_attributes():
    from reroll_sync.archive.location import BlobLocation
    from reroll_sync.daemon.circuit_breaker import CircuitBreaker
    from reroll_sync.daemon.service import _DiskBreakerGuardedStore

    class _FakeStore:
        reader = "the-real-reader"

        def add(self, data: bytes) -> BlobLocation:
            raise NotImplementedError

        def current_writer(self) -> None:
            raise NotImplementedError

    guarded = _DiskBreakerGuardedStore(_FakeStore(), CircuitBreaker())

    assert guarded.reader == "the-real-reader"  # via __getattr__, not add/current_writer


def test_disk_breaker_guarded_store_current_writer_wraps_when_shutdown_event_given():
    import threading

    from reroll_sync.archive.location import BlobLocation
    from reroll_sync.daemon.circuit_breaker import CircuitBreaker
    from reroll_sync.daemon.service import _DiskBreakerGuardedStore, _NoSealDuringShutdownWriter

    class _FakeWriter:
        def should_seal(self) -> bool:
            return True

    class _FakeStore:
        def current_writer(self) -> _FakeWriter:
            return _FakeWriter()

        def add(self, data: bytes) -> BlobLocation:
            raise NotImplementedError

    shutdown_event = threading.Event()
    guarded = _DiskBreakerGuardedStore(
        _FakeStore(), CircuitBreaker(), shutdown_event=shutdown_event
    )

    wrapped = guarded.current_writer()

    assert isinstance(wrapped, _NoSealDuringShutdownWriter)
    assert wrapped.should_seal() is True  # not shut down yet: delegates to the real writer
    shutdown_event.set()
    assert wrapped.should_seal() is False  # shut down: never seals, regardless of the real writer


def test_no_seal_during_shutdown_writer_passes_through_other_attributes():
    import threading

    from reroll_sync.daemon.service import _NoSealDuringShutdownWriter

    class _FakeWriter:
        segment_id = 42

        def seal(self) -> str:
            return "sealed"

    wrapped = _NoSealDuringShutdownWriter(_FakeWriter(), threading.Event())

    assert wrapped.segment_id == 42
    assert wrapped.seal() == "sealed"


def test_unknown_stage_name_is_reported_as_an_error(work_dir, socket_dir, daemon_factory):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()

    response = _request(daemon.config.socket_path, {"command": "pause", "args": {"stage": "nope"}})

    assert response["ok"] is False
    assert "nope" in response["error"]


# ---------------------------------------------------------------------------
# /metrics HTTP endpoint (spec 11)
# ---------------------------------------------------------------------------


def test_metrics_server_is_not_started_when_metrics_port_is_unset(
    work_dir, socket_dir, daemon_factory
):
    daemon = daemon_factory(_config(work_dir, socket_dir))
    daemon.start()
    assert daemon.metrics_server is None


def test_metrics_endpoint_serves_prometheus_text(work_dir, socket_dir, daemon_factory):
    import http.client

    daemon = daemon_factory(_config(work_dir, socket_dir, metrics_port=0))
    daemon.start()
    assert daemon.metrics_server is not None

    conn = http.client.HTTPConnection("127.0.0.1", daemon.metrics_server.port(), timeout=5)
    try:
        conn.request("GET", "/metrics")
        response = conn.getresponse()
        body = response.read().decode("utf-8")
    finally:
        conn.close()

    assert response.status == 200
    assert "reroll_sync_wal_bytes" in body
    assert "reroll_sync_index_lag" in body


def test_metrics_endpoint_reflects_dispatcher_and_writer_state(
    work_dir, socket_dir, daemon_factory
):
    import http.client

    daemon = daemon_factory(_config(work_dir, socket_dir, metrics_port=0))
    daemon.start()
    _insert_wheel(
        daemon.config.db_path, filename="widget-1.0-py3-none-any.whl", state=WheelState.QUARANTINED
    )

    conn = http.client.HTTPConnection("127.0.0.1", daemon.metrics_server.port(), timeout=5)
    try:
        conn.request("GET", "/metrics")
        response = conn.getresponse()
        body = response.read().decode("utf-8")
    finally:
        conn.close()

    assert 'reroll_sync_state_counts{state="QUARANTINED"} 1' in body


def test_shutdown_stops_the_metrics_server(work_dir, socket_dir, daemon_factory):
    import http.client

    daemon = daemon_factory(_config(work_dir, socket_dir, metrics_port=0))
    daemon.start()
    port = daemon.metrics_server.port()

    daemon.shutdown()

    def _get() -> None:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
        conn.request("GET", "/metrics")
        conn.getresponse()

    with pytest.raises(ConnectionRefusedError):
        _get()
