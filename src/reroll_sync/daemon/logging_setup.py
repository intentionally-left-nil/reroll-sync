"""Structured JSON logging for the daemon.

Every log line goes to stdout as one JSON object; the process supervisor
(systemd, docker, etc.) captures it from there. Correlation keys
(``wheel_id``/``filename``/``project``) are pulled from ``LogRecord``
extras when a stage supplies them.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import IO

NOISY_REROLL_LOGGERS: tuple[str, ...] = (
    "reroll.scope",
    "reroll.invalid",
    "reroll.unconvertable",
    "reroll.runtime",
)
"""reroll's own per-wheel loggers. Every skip is already recorded in the
database (``skips``/``errors``), so logging it again at 12M-wheel scale is
pure noise; silenced to ``ERROR`` by :func:`configure_logging`.
"""

_CORRELATION_KEYS = {
    "wheel_id": "wheel_id",
    "wheel_filename": "filename",
    "wheel_project": "project",
}
"""Maps a caller's `extra` key to its output JSON field name.

``filename``/``project`` collide with reserved `logging.LogRecord`
attributes (`filename` is always the source file of the log call), so
callers pass ``wheel_filename``/``wheel_project`` instead; the JSON output
still uses the plain, spec-mandated field names.
"""

_ROOT_LOGGER_NAME = "reroll_sync"


class JsonLogFormatter(logging.Formatter):
    """Renders one `logging.LogRecord` as one JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for extra_key, output_key in _CORRELATION_KEYS.items():
            value = getattr(record, extra_key, None)
            if value is not None:
                payload[output_key] = value
        if record.exc_info is not None:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(*, level: int = logging.INFO, stream: IO[str] | None = None) -> None:
    """Install a single JSON-lines handler on the ``reroll_sync`` root logger.

    Idempotent: calling this again replaces the handler rather than
    stacking a second one. Also silences :data:`NOISY_REROLL_LOGGERS` to
    ``ERROR``, per this module's docstring. ``stream`` defaults to the
    *current* ``sys.stdout`` at call time, not whatever it was when this
    module was imported -- a plain ``sys.stdout`` default would bind the
    stream that existed at import time and miss any later replacement
    (e.g. pytest's per-test capture).
    """
    root = logging.getLogger(_ROOT_LOGGER_NAME)
    root.setLevel(level)
    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    root.handlers = [handler]
    root.propagate = False

    silence_noisy_reroll_loggers()


def silence_noisy_reroll_loggers() -> None:
    """Sets each of :data:`NOISY_REROLL_LOGGERS` to ``ERROR``.

    Callers that run outside the process ``configure_logging`` was called
    in -- e.g. a ``ProcessPoolExecutor`` worker started via ``spawn``, which
    gets a fresh, unconfigured logging module rather than inheriting the
    parent process's levels -- must call this themselves.
    """
    for name in NOISY_REROLL_LOGGERS:
        logging.getLogger(name).setLevel(logging.ERROR)
