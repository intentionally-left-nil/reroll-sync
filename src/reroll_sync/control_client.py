"""The control protocol's client side: one request, one reply, over a unix socket.

The counterpart to ``control.py``'s server (``ControlServer``): this module
only ever connects out, sends one JSON line, and reads one back. Every
failure mode -- no daemon, a socket present but not accepting, a timed-out
or malformed reply -- raises :class:`ControlClientError` naming the socket
path, rather than hanging or crashing.
"""

from __future__ import annotations

import json
import socket
from collections.abc import Mapping
from pathlib import Path
from typing import Any

DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 30.0


class ControlClientError(Exception):
    """Raised for every way a control-socket request can fail to complete."""


def send_control_command(
    socket_path: Path,
    command: str,
    args: Mapping[str, Any] | None = None,
    *,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
) -> Any:
    """Send one ``{"command": ..., "args": ...}`` request and return its ``result``.

    Connecting is bounded by ``connect_timeout``; the round trip after that
    (send + one line of reply) is bounded by ``read_timeout`` -- a socket
    that is listening but whose accept loop never runs still lets
    ``connect`` succeed at the kernel level, so the read side needs its own
    bound to avoid hanging forever. The daemon's own ``{"ok": false, ...}``
    reply is raised verbatim as a :class:`ControlClientError`.
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(connect_timeout)
        try:
            sock.connect(str(socket_path))
        except (FileNotFoundError, ConnectionRefusedError) as exc:
            raise ControlClientError(
                f"no daemon is listening on control socket '{socket_path}': {exc}"
            ) from exc
        except OSError as exc:
            raise ControlClientError(
                f"could not connect to control socket '{socket_path}': {exc}"
            ) from exc

        sock.settimeout(read_timeout)
        payload = json.dumps({"command": command, "args": dict(args or {})}).encode("utf-8")
        try:
            sock.sendall(payload + b"\n")
            line = _read_line(sock)
        except OSError as exc:
            raise ControlClientError(
                f"lost connection to control socket '{socket_path}': {exc}"
            ) from exc
    finally:
        sock.close()

    if line is None:
        raise ControlClientError(f"daemon at '{socket_path}' closed the connection with no reply")
    try:
        response = json.loads(line)
    except ValueError as exc:
        raise ControlClientError(f"malformed response from '{socket_path}': {exc}") from exc
    if not response.get("ok", False):
        raise ControlClientError(str(response.get("error", "unknown error")))
    return response.get("result")


def _read_line(sock: socket.socket) -> bytes | None:
    """Read up to and including the first newline, or ``None`` if the peer sent nothing."""
    buf = bytearray()
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf.extend(chunk)
    if not buf:
        return None
    line, _sep, _rest = bytes(buf).partition(b"\n")
    return line
