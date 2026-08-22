"""Tests for the unix-socket control protocol: real sockets, fake handlers.

Per specs/10-daemon-and-control.md, sockets (not sleeps) are the one place
a real OS resource is appropriate in this spec's test suite.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import stat
import tempfile
import threading

import pytest

from reroll_sync.control import ControlHandlers, ControlProtocolError, ControlServer
from reroll_sync.dispatcher import ProjectSelector, RerollVersionBelow, SkippedOnly, StateSelector
from reroll_sync.schema import WheelState


class _FakeHandlers:
    def __init__(self) -> None:
        self.paused: list[str] = []
        self.resumed: list[str] = []
        self.drained = False
        self.shutdown_called = False
        self.reprocess_calls: list[object] = []
        self.unquarantine_calls: list[object] = []
        self.status_result: dict = {"stages": {}}
        self.reprocess_result = 0
        self.unquarantine_result = 0
        self.raise_on_pause: Exception | None = None

    def status(self):
        return self.status_result

    def pause(self, stage: str) -> None:
        if self.raise_on_pause is not None:
            raise self.raise_on_pause
        self.paused.append(stage)

    def resume(self, stage: str) -> None:
        self.resumed.append(stage)

    def drain(self) -> None:
        self.drained = True

    def reprocess(self, selector) -> int:
        self.reprocess_calls.append(selector)
        return self.reprocess_result

    def unquarantine(self, selector) -> int:
        self.unquarantine_calls.append(selector)
        return self.unquarantine_result

    def shutdown(self) -> None:
        self.shutdown_called = True


def _handlers(fake: _FakeHandlers) -> ControlHandlers:
    return ControlHandlers(
        status=fake.status,
        pause=fake.pause,
        resume=fake.resume,
        drain=fake.drain,
        reprocess=fake.reprocess,
        unquarantine=fake.unquarantine,
        shutdown=fake.shutdown,
    )


@pytest.fixture
def fake() -> _FakeHandlers:
    return _FakeHandlers()


@pytest.fixture
def socket_dir():
    # macOS caps AF_UNIX paths well below pytest's default (deeply nested)
    # tmp_path, so control sockets get their own short-lived directory
    # directly under /tmp instead.
    path = tempfile.mkdtemp(prefix="rs-ctl-")
    yield __import__("pathlib").Path(path)
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def server(socket_dir, fake):
    socket_path = socket_dir / "control.sock"
    srv = ControlServer(socket_path, _handlers(fake))
    srv.start()
    yield srv
    srv.stop()


def _request(socket_path, payload: dict, *, raw: bytes | None = None, read: bool = True):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(5.0)
        sock.connect(str(socket_path))
        data = raw if raw is not None else (json.dumps(payload).encode() + b"\n")
        sock.sendall(data)
        if not read:
            return None
        response = _read_response(sock)
        return response


def _read_response(sock: socket.socket) -> dict:
    buf = bytearray()
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf.extend(chunk)
    line, _, _rest = bytes(buf).partition(b"\n")
    return json.loads(line)


# ---------------------------------------------------------------------------
# Socket setup
# ---------------------------------------------------------------------------


def test_socket_is_created_with_mode_0600(server, socket_dir):
    socket_path = socket_dir / "control.sock"
    mode = stat.S_IMODE(os.stat(socket_path).st_mode)
    assert mode == 0o600


def test_stale_socket_file_is_replaced_not_fatal(socket_dir, fake):
    socket_path = socket_dir / "control.sock"
    socket_path.write_text("stale")
    srv = ControlServer(socket_path, _handlers(fake))
    srv.start()
    try:
        response = _request(socket_path, {"command": "status"})
        assert response["ok"] is True
    finally:
        srv.stop()


def test_stop_removes_the_socket_file(socket_dir, fake):
    socket_path = socket_dir / "control.sock"
    srv = ControlServer(socket_path, _handlers(fake))
    srv.start()
    assert socket_path.exists()
    srv.stop()
    assert not socket_path.exists()


def test_stop_without_start_is_a_noop(socket_dir, fake):
    srv = ControlServer(socket_dir / "control.sock", _handlers(fake))
    srv.stop()  # must not raise even though start() was never called


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def test_status_round_trips(server, socket_dir, fake):
    fake.status_result = {"stages": {"fetch": {"paused": False}}}
    response = _request(socket_dir / "control.sock", {"command": "status"})
    assert response == {"ok": True, "result": {"stages": {"fetch": {"paused": False}}}}


def test_pause_calls_handler_with_stage_name(server, socket_dir, fake):
    response = _request(
        socket_dir / "control.sock", {"command": "pause", "args": {"stage": "fetch"}}
    )
    assert response["ok"] is True
    assert fake.paused == ["fetch"]


def test_resume_calls_handler_with_stage_name(server, socket_dir, fake):
    response = _request(
        socket_dir / "control.sock", {"command": "resume", "args": {"stage": "fetch"}}
    )
    assert response["ok"] is True
    assert fake.resumed == ["fetch"]


def test_pause_and_resume_observably_change_behaviour(server, socket_dir, fake):
    """The control protocol layer itself just forwards to the handler; this
    confirms the round trip actually reaches it and back for both directions.
    """
    _request(socket_dir / "control.sock", {"command": "pause", "args": {"stage": "fetch"}})
    _request(socket_dir / "control.sock", {"command": "resume", "args": {"stage": "fetch"}})
    assert fake.paused == ["fetch"]
    assert fake.resumed == ["fetch"]


def test_pause_missing_stage_arg_is_an_error(server, socket_dir, fake):
    response = _request(socket_dir / "control.sock", {"command": "pause", "args": {}})
    assert response["ok"] is False
    assert fake.paused == []


def test_drain_calls_handler(server, socket_dir, fake):
    response = _request(socket_dir / "control.sock", {"command": "drain"})
    assert response["ok"] is True
    assert fake.drained is True


def test_shutdown_calls_handler(server, socket_dir, fake):
    response = _request(socket_dir / "control.sock", {"command": "shutdown"})
    assert response["ok"] is True
    assert fake.shutdown_called is True


def test_reprocess_returns_affected_count(server, socket_dir, fake):
    fake.reprocess_result = 42
    response = _request(
        socket_dir / "control.sock",
        {"command": "reprocess", "args": {"type": "project", "project": "numpy"}},
    )
    assert response == {"ok": True, "result": {"affected": 42}}
    assert fake.reprocess_calls == [ProjectSelector(project="numpy")]


def test_reprocess_parses_reroll_version_below_selector(server, socket_dir, fake):
    _request(
        socket_dir / "control.sock",
        {"command": "reprocess", "args": {"type": "reroll_version_below", "version": "1.2.3"}},
    )
    assert fake.reprocess_calls == [RerollVersionBelow(version="1.2.3")]


def test_reprocess_parses_state_selector(server, socket_dir, fake):
    _request(
        socket_dir / "control.sock",
        {"command": "reprocess", "args": {"type": "state", "state": "SKIPPED"}},
    )
    assert fake.reprocess_calls == [StateSelector(state=WheelState.SKIPPED)]


def test_reprocess_parses_skipped_only_selector(server, socket_dir, fake):
    _request(
        socket_dir / "control.sock", {"command": "reprocess", "args": {"type": "skipped_only"}}
    )
    assert fake.reprocess_calls == [SkippedOnly()]


def test_reprocess_unknown_selector_type_is_an_error(server, socket_dir, fake):
    response = _request(
        socket_dir / "control.sock", {"command": "reprocess", "args": {"type": "bogus"}}
    )
    assert response["ok"] is False
    assert fake.reprocess_calls == []


def test_reprocess_unknown_state_name_is_an_error(server, socket_dir, fake):
    response = _request(
        socket_dir / "control.sock",
        {"command": "reprocess", "args": {"type": "state", "state": "NOT_A_REAL_STATE"}},
    )
    assert response["ok"] is False


def test_unquarantine_returns_affected_count(server, socket_dir, fake):
    fake.unquarantine_result = 3
    response = _request(
        socket_dir / "control.sock",
        {"command": "unquarantine", "args": {"type": "state", "state": "QUARANTINED"}},
    )
    assert response == {"ok": True, "result": {"affected": 3}}
    assert fake.unquarantine_calls == [StateSelector(state=WheelState.QUARANTINED)]


def test_unknown_command_returns_error_listing_valid_commands(server, socket_dir):
    response = _request(socket_dir / "control.sock", {"command": "not-a-real-command"})
    assert response["ok"] is False
    assert "status" in response["error"]
    assert "pause" in response["error"]


def test_a_handler_exception_is_reported_not_crashed(server, socket_dir, fake):
    fake.raise_on_pause = RuntimeError("boom")
    response = _request(
        socket_dir / "control.sock", {"command": "pause", "args": {"stage": "fetch"}}
    )
    assert response["ok"] is False
    assert "boom" in response["error"]
    # The server must still be alive afterward.
    response2 = _request(socket_dir / "control.sock", {"command": "status"})
    assert response2["ok"] is True


# ---------------------------------------------------------------------------
# Malformed / oversized / disconnecting requests
# ---------------------------------------------------------------------------


def test_malformed_json_is_handled_without_crashing(server, socket_dir):
    response = _request(socket_dir / "control.sock", {}, raw=b"not json at all\n")
    assert response["ok"] is False
    # Server still answers a subsequent well-formed request.
    response2 = _request(socket_dir / "control.sock", {"command": "status"})
    assert response2["ok"] is True


def test_request_missing_command_field_is_an_error(server, socket_dir):
    response = _request(socket_dir / "control.sock", {}, raw=b'{"args": {}}\n')
    assert response["ok"] is False


def test_non_object_args_is_an_error(server, socket_dir):
    response = _request(socket_dir / "control.sock", {}, raw=b'{"command": "status", "args": 5}\n')
    assert response["ok"] is False


def test_oversized_request_is_rejected_without_crashing(socket_dir, fake):
    socket_path = socket_dir / "control.sock"
    srv = ControlServer(socket_path, _handlers(fake), max_request_bytes=64)
    srv.start()
    try:
        huge_payload = json.dumps({"command": "status", "args": {"x": "y" * 1000}}).encode()
        response = _request(socket_path, {}, raw=huge_payload + b"\n")
        assert response is not None
        assert response["ok"] is False
        # Server survives; a normal request still works.
        response2 = _request(socket_path, {"command": "status"})
        assert response2["ok"] is True
    finally:
        srv.stop()


class _RecvRaisesOSError:
    """A minimal socket-like object whose `recv` raises `OSError`.

    Exercises `_handle_connection`'s defensive read-error branch, which a
    real reset connection can trigger but not reliably on demand.
    """

    def recv(self, _bufsize: int) -> bytes:
        raise OSError("connection reset by peer")

    def close(self) -> None:
        pass

    def __enter__(self) -> _RecvRaisesOSError:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def test_read_error_during_a_connection_does_not_crash_the_server(server):
    server._handle_connection(_RecvRaisesOSError())  # must not raise


class _OversizedRequestThenDisconnectedSend:
    """A minimal socket-like object that deterministically reproduces the
    race Fix 5 targets: an oversized request (so `_read_line` raises
    `ControlProtocolError`) immediately followed by the client vanishing
    before the error reply can be sent (so `sendall` raises `OSError`).
    """

    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes

    def recv(self, _bufsize: int) -> bytes:
        return b"x" * (self._max_bytes + 1)

    def sendall(self, _data: bytes) -> int:
        raise OSError("client already disconnected")

    def close(self) -> None:
        pass

    def __enter__(self) -> _OversizedRequestThenDisconnectedSend:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def test_oversized_request_send_error_does_not_crash_the_server(server):
    """The `ControlProtocolError` branch's error reply must be wrapped in
    `contextlib.suppress(OSError)` just like the other `_send` call in
    `_handle_connection`, so a client that disconnects between an oversized
    request and the error reply doesn't raise out of the request-handling
    thread.
    """
    conn = _OversizedRequestThenDisconnectedSend(server._max_request_bytes)
    server._handle_connection(conn)  # must not raise


def test_client_disconnect_mid_write_does_not_affect_other_clients(server, socket_dir, fake):
    socket_path = socket_dir / "control.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(str(socket_path))
    sock.sendall(b'{"command": "sta')  # no trailing newline, then vanish
    sock.close()

    response = _request(socket_path, {"command": "status"})
    assert response["ok"] is True


def test_client_that_sends_nothing_and_disconnects_is_handled(server, socket_dir):
    socket_path = socket_dir / "control.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(str(socket_path))
    sock.close()

    response = _request(socket_path, {"command": "status"})
    assert response["ok"] is True


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_clients_are_all_served(server, socket_dir):
    socket_path = socket_dir / "control.sock"
    results: list[dict] = []
    lock = threading.Lock()

    def _worker():
        response = _request(socket_path, {"command": "status"})
        with lock:
            results.append(response)

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    assert len(results) == 10
    assert all(r["ok"] is True for r in results)


# ---------------------------------------------------------------------------
# Shutdown restricting to status-only
# ---------------------------------------------------------------------------


def test_restrict_to_status_only_rejects_other_commands(server, socket_dir, fake):
    socket_path = socket_dir / "control.sock"
    server.restrict_to_status_only()

    response = _request(socket_path, {"command": "pause", "args": {"stage": "fetch"}})
    assert response["ok"] is False
    assert fake.paused == []

    response2 = _request(socket_path, {"command": "status"})
    assert response2["ok"] is True


def test_control_protocol_error_message_is_informative():
    with pytest.raises(ControlProtocolError, match="bad selector"):
        raise ControlProtocolError("bad selector")
