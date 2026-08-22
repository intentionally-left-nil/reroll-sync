"""Tests for the control protocol's CLIENT side: a real unix socket against
a real `ControlServer` (mirroring `test_control.py`'s style), plus raw
sockets for the failure modes a live daemon can't produce on demand.
"""

from __future__ import annotations

import shutil
import socket
import tempfile
from pathlib import Path

import pytest

from reroll_sync.control import ControlHandlers, ControlServer
from reroll_sync.control_client import ControlClientError, send_control_command


class _FakeHandlers:
    def __init__(self) -> None:
        self.status_result: dict = {"stages": {}}
        self.paused: list[str] = []
        self.raise_on_pause: Exception | None = None

    def status(self):
        return self.status_result

    def pause(self, stage: str) -> None:
        if self.raise_on_pause is not None:
            raise self.raise_on_pause
        self.paused.append(stage)

    def resume(self, stage: str) -> None:
        pass

    def drain(self) -> None:
        pass

    def reprocess(self, selector) -> int:
        return 0

    def unquarantine(self, selector) -> int:
        return 0

    def shutdown(self) -> None:
        pass


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
    # directly under /tmp instead (mirrors test_control.py).
    path = tempfile.mkdtemp(prefix="rs-cli-ctl-")
    yield Path(path)
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def server(socket_dir, fake):
    socket_path = socket_dir / "control.sock"
    srv = ControlServer(socket_path, _handlers(fake))
    srv.start()
    yield srv
    srv.stop()


# ---------------------------------------------------------------------------
# Success round trip
# ---------------------------------------------------------------------------


def test_status_round_trips(server, socket_dir, fake):
    fake.status_result = {"stages": {"fetch": {"paused": False}}}
    result = send_control_command(socket_dir / "control.sock", "status")
    assert result == {"stages": {"fetch": {"paused": False}}}


def test_args_are_forwarded_to_the_handler(server, socket_dir, fake):
    result = send_control_command(socket_dir / "control.sock", "pause", {"stage": "fetch"})
    assert result == {"stage": "fetch", "paused": True}
    assert fake.paused == ["fetch"]


def test_no_args_defaults_to_an_empty_object(server, socket_dir, fake):
    result = send_control_command(socket_dir / "control.sock", "drain")
    assert result == {"draining": True}


# ---------------------------------------------------------------------------
# The daemon's error reply is surfaced verbatim
# ---------------------------------------------------------------------------


def test_daemon_error_reply_is_surfaced_verbatim(server, socket_dir, fake):
    fake.raise_on_pause = RuntimeError("boom, stage does not exist")
    with pytest.raises(ControlClientError, match="boom, stage does not exist"):
        send_control_command(socket_dir / "control.sock", "pause", {"stage": "bogus"})


def test_unknown_command_error_is_surfaced(server, socket_dir):
    with pytest.raises(ControlClientError, match="unknown command"):
        send_control_command(socket_dir / "control.sock", "not-a-real-command")


# ---------------------------------------------------------------------------
# No daemon at all
# ---------------------------------------------------------------------------


def test_no_socket_file_at_all_raises_naming_the_path(socket_dir):
    missing = socket_dir / "control.sock"
    with pytest.raises(ControlClientError, match=str(missing)):
        send_control_command(missing, "status")


def test_stale_socket_file_left_behind_by_a_crashed_daemon_raises(socket_dir):
    stale = socket_dir / "control.sock"
    stale.write_text("not actually a socket")
    with pytest.raises(ControlClientError, match=str(stale)):
        send_control_command(stale, "status")


def test_other_connect_oserror_raises_naming_the_path(socket_dir):
    # A path longer than AF_UNIX's ``sun_path`` buffer (104-108 bytes,
    # depending on platform) makes the kernel-level `connect` fail with a
    # plain `OSError` -- neither `FileNotFoundError` nor
    # `ConnectionRefusedError` -- exercising the catch-all branch.
    too_long = socket_dir / ("a" * 200)
    with pytest.raises(ControlClientError, match="could not connect"):
        send_control_command(too_long, "status")


# ---------------------------------------------------------------------------
# Socket present but not accepting: bounded, never hangs
# ---------------------------------------------------------------------------


def test_socket_listening_but_never_accepting_times_out_quickly(socket_dir):
    socket_path = socket_dir / "control.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    listener.listen(1)
    try:
        with pytest.raises(ControlClientError, match=str(socket_path)):
            send_control_command(socket_path, "status", connect_timeout=0.2, read_timeout=0.2)
    finally:
        listener.close()


# ---------------------------------------------------------------------------
# Malformed / truncated responses
# ---------------------------------------------------------------------------


def test_malformed_response_raises(socket_dir):
    socket_path = socket_dir / "control.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    listener.listen(1)
    try:

        def _serve_once():
            conn, _addr = listener.accept()
            with conn:
                conn.recv(4096)
                conn.sendall(b"not json at all\n")

        import threading

        thread = threading.Thread(target=_serve_once, daemon=True)
        thread.start()
        with pytest.raises(ControlClientError, match="malformed"):
            send_control_command(socket_path, "status", read_timeout=5.0)
        thread.join(timeout=5.0)
    finally:
        listener.close()


def test_connection_closed_with_no_reply_raises(socket_dir):
    socket_path = socket_dir / "control.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    listener.listen(1)
    try:

        def _serve_once():
            conn, _addr = listener.accept()
            with conn:
                # Read the whole request first so the client's own `sendall`
                # always succeeds -- deterministically exercising the "no
                # reply at all" path in `_read_line`/`send_control_command`,
                # rather than racing it against a broken-pipe `sendall`.
                conn.recv(4096)

        import threading

        thread = threading.Thread(target=_serve_once, daemon=True)
        thread.start()
        with pytest.raises(ControlClientError, match="closed the connection"):
            send_control_command(socket_path, "status", read_timeout=5.0)
        thread.join(timeout=5.0)
    finally:
        listener.close()
