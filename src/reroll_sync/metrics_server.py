"""A minimal, localhost-only HTTP server exposing `/metrics`.

The daemon's control plane (``control.py``) is a unix-domain-socket JSON
protocol, not HTTP -- this module is the one exception, since Prometheus
scrapes plain HTTP text. Bound to ``127.0.0.1`` only; it never listens on
any other interface.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger("reroll_sync.metrics_server")

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_METRICS_PATH = "/metrics"


class MetricsServer:
    """Serves ``GET /metrics`` on ``127.0.0.1:port``, calling ``render`` per request.

    Every other path returns ``404``. ``render`` runs fresh on every
    request -- no caching -- so a scrape always reflects a current
    ``health.snapshot()``. Pass ``port=0`` to let the OS pick a free port
    (useful for tests); read it back afterward via :meth:`port`.

    Per-request defensive, like ``control.py``'s own handler: a ``render``
    exception is caught, logged, and answered with a plain ``500`` body --
    it never crashes the server thread or affects a later request.
    """

    def __init__(self, port: int, render: Callable[[], str]) -> None:
        self._port = port
        self._render = render
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Bind and start serving on a background thread."""
        render = self._render

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                logger.debug("metrics_server: " + format, *args)

            def do_GET(self) -> None:
                if self.path != _METRICS_PATH:
                    self.send_response(404)
                    self.end_headers()
                    return
                try:
                    body = render().encode("utf-8")
                except Exception:
                    logger.error("metrics_server: render() raised", exc_info=True)
                    self._send_error_body()
                    return
                self.send_response(200)
                self.send_header("Content-Type", _CONTENT_TYPE)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _send_error_body(self) -> None:
                body = b"internal error rendering metrics\n"
                self.send_response(500)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", self._port), _Handler)
        self._server = server
        self._thread = threading.Thread(
            target=server.serve_forever, name="reroll-sync-metrics", daemon=True
        )
        self._thread.start()

    def port(self) -> int:
        """Return the bound port -- the OS-assigned one, if constructed with ``port=0``."""
        assert self._server is not None, "start() has not been called"
        return self._server.server_address[1]

    def stop(self) -> None:
        """Stop serving and join the background thread. Safe to call without ``start()``."""
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
