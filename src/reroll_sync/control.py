"""Unix-domain-socket control plane for the daemon.

Newline-delimited JSON, one request per connection:
``{"command": str, "args": {...}}`` in, ``{"ok": bool, "result": ...}`` or
``{"ok": bool, "error": ...}`` out.

**Permissions**: the socket is created with mode ``0600`` and no other
authentication. This is a deliberate Phase 1 decision, not an oversight --
the threat model is "another unprivileged user on this host", which
filesystem permissions already cover. See specs/10-daemon-and-control.md's
"Deferred" section for what a stronger threat model would need.

Mutating commands (``pause``/``resume``/``drain``/``reprocess``/
``unquarantine``/``shutdown``) are expected to be backed by handlers that go
through the daemon's single writer thread, so a reply reflects committed
state -- see ``daemon/service.py``. This module itself never touches
sqlite; it only parses requests and dispatches to whatever
:class:`ControlHandlers` the daemon supplies.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .dispatcher import ProjectSelector, RerollVersionBelow, Selector, SkippedOnly, StateSelector
from .schema import WheelState

logger = logging.getLogger("reroll_sync.control")

DEFAULT_MAX_REQUEST_BYTES = 64 * 1024
DEFAULT_MAX_WORKERS = 8
_SOCKET_MODE = 0o600


class ControlProtocolError(Exception):
    """Raised for a malformed request. Always caught before it can affect another connection."""


@dataclass(frozen=True)
class ControlHandlers:
    """Everything one control command handler needs, supplied by the daemon.

    ``pause``/``resume`` take a stage name; ``reprocess``/``unquarantine``
    take an already-parsed :data:`~reroll_sync.dispatcher.Selector` and
    return the affected count.
    """

    status: Callable[[], Mapping[str, Any]]
    pause: Callable[[str], None]
    resume: Callable[[str], None]
    drain: Callable[[], None]
    reprocess: Callable[[Selector], int]
    unquarantine: Callable[[Selector], int]
    shutdown: Callable[[], None]


class ControlServer:
    """Serves the control protocol over a unix domain socket at ``socket_path``.

    Each accepted connection is handled on its own thread from a bounded
    pool and serves exactly one request. A stale socket file left behind by
    a crashed previous run is replaced, not fatal.
    """

    def __init__(
        self,
        socket_path: Path,
        handlers: ControlHandlers,
        *,
        max_workers: int = DEFAULT_MAX_WORKERS,
        max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES,
    ) -> None:
        self._socket_path = Path(socket_path)
        self._handlers = handlers
        self._max_request_bytes = max_request_bytes
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="reroll-sync-control"
        )
        self._server: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._status_only = threading.Event()

    def start(self) -> None:
        """Bind and listen on ``socket_path`` (mode ``0600``), replacing any stale file."""
        with contextlib.suppress(FileNotFoundError):
            self._socket_path.unlink()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(self._socket_path))
        os.chmod(self._socket_path, _SOCKET_MODE)
        server.listen(16)
        self._server = server
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name="reroll-sync-control-accept", daemon=True
        )
        self._accept_thread.start()

    def stop(self) -> None:
        """Stop accepting connections, wait for in-flight ones, and remove the socket file.

        Safe to call even if `start` was never called (e.g. daemon startup
        failed before the control socket came up): a `None` server or
        accept thread is simply skipped.
        """
        if self._server is not None:
            with contextlib.suppress(OSError):
                self._server.close()
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=5.0)
        self._pool.shutdown(wait=True)
        with contextlib.suppress(FileNotFoundError):
            self._socket_path.unlink()

    def restrict_to_status_only(self) -> None:
        """Refuse every command but ``status``, per the shutdown sequence's first step."""
        self._status_only.set()

    def _accept_loop(self) -> None:
        """Accept connections until `stop` closes the listening socket.

        `socket.accept` on a closed socket raises `OSError`, which is this
        loop's only exit -- there's no separate "closing" flag to check.
        """
        assert self._server is not None
        while True:
            try:
                conn, _addr = self._server.accept()
            except OSError:
                return
            self._pool.submit(self._handle_connection, conn)

    def _handle_connection(self, conn: socket.socket) -> None:
        with conn:
            try:
                line = _read_line(conn, self._max_request_bytes)
            except ControlProtocolError as exc:
                with contextlib.suppress(OSError):
                    _send(conn, {"ok": False, "error": str(exc)})
                return
            except OSError:
                return
            if line is None:
                return
            response = self._dispatch(line)
            with contextlib.suppress(OSError):
                _send(conn, response)

    def _dispatch(self, line: bytes) -> dict[str, Any]:
        try:
            request = json.loads(line)
        except ValueError as exc:
            return {"ok": False, "error": f"malformed JSON request: {exc}"}
        if not isinstance(request, dict) or "command" not in request:
            return {"ok": False, "error": "request must be a JSON object with a 'command' field"}

        command = request["command"]
        args = request.get("args") or {}
        if not isinstance(args, dict):
            return {"ok": False, "error": "'args' must be a JSON object"}

        if self._status_only.is_set() and command != "status":
            return {"ok": False, "error": "daemon is shutting down; only 'status' is accepted"}

        handler = _COMMANDS.get(command)
        if handler is None:
            return {
                "ok": False,
                "error": f"unknown command {command!r}; valid commands: {sorted(_COMMANDS)}",
            }
        try:
            result = handler(self._handlers, args)
        except Exception as exc:
            logger.error("control command %r failed", command, exc_info=True)
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "result": result}


def _cmd_status(handlers: ControlHandlers, args: Mapping[str, Any]) -> Any:
    del args
    return handlers.status()


def _cmd_pause(handlers: ControlHandlers, args: Mapping[str, Any]) -> Any:
    stage = _require_str(args, "stage")
    handlers.pause(stage)
    return {"stage": stage, "paused": True}


def _cmd_resume(handlers: ControlHandlers, args: Mapping[str, Any]) -> Any:
    stage = _require_str(args, "stage")
    handlers.resume(stage)
    return {"stage": stage, "paused": False}


def _cmd_drain(handlers: ControlHandlers, args: Mapping[str, Any]) -> Any:
    del args
    handlers.drain()
    return {"draining": True}


def _cmd_reprocess(handlers: ControlHandlers, args: Mapping[str, Any]) -> Any:
    selector = _parse_selector(args)
    affected = handlers.reprocess(selector)
    return {"affected": affected}


def _cmd_unquarantine(handlers: ControlHandlers, args: Mapping[str, Any]) -> Any:
    selector = _parse_selector(args)
    affected = handlers.unquarantine(selector)
    return {"affected": affected}


def _cmd_shutdown(handlers: ControlHandlers, args: Mapping[str, Any]) -> Any:
    del args
    handlers.shutdown()
    return {"shutting_down": True}


_COMMANDS: dict[str, Callable[[ControlHandlers, Mapping[str, Any]], Any]] = {
    "status": _cmd_status,
    "pause": _cmd_pause,
    "resume": _cmd_resume,
    "drain": _cmd_drain,
    "reprocess": _cmd_reprocess,
    "unquarantine": _cmd_unquarantine,
    "shutdown": _cmd_shutdown,
}


def _parse_selector(args: Mapping[str, Any]) -> Selector:
    selector_type = _require_str(args, "type")
    if selector_type == "reroll_version_below":
        return RerollVersionBelow(version=_require_str(args, "version"))
    if selector_type == "project":
        return ProjectSelector(project=_require_str(args, "project"))
    if selector_type == "state":
        state_name = _require_str(args, "state")
        try:
            return StateSelector(state=WheelState[state_name])
        except KeyError as exc:
            raise ControlProtocolError(f"unknown state {state_name!r}") from exc
    if selector_type == "skipped_only":
        return SkippedOnly()
    raise ControlProtocolError(f"unknown selector type {selector_type!r}")


def _require_str(args: Mapping[str, Any], key: str) -> str:
    value = args.get(key)
    if not isinstance(value, str) or not value:
        raise ControlProtocolError(f"'{key}' is required and must be a non-empty string")
    return value


def _read_line(conn: socket.socket, max_bytes: int) -> bytes | None:
    """Read up to and including the first newline, or ``None`` if the peer sent nothing."""
    buf = bytearray()
    while b"\n" not in buf:
        chunk = conn.recv(4096)
        if not chunk:
            break
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise ControlProtocolError(f"request exceeded {max_bytes} bytes")
    if not buf:
        return None
    line, _sep, _rest = bytes(buf).partition(b"\n")
    return line


def _send(conn: socket.socket, response: dict[str, Any]) -> None:
    conn.sendall(json.dumps(response).encode("utf-8") + b"\n")
