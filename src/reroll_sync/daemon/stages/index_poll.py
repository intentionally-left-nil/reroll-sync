"""The `index_poll` stage: conditionally re-fetches the PyPI simple index
and forwards each stale project name to `project_sync`.

Guarded by the `pypi.org` circuit breaker: `PyPIRateLimited` never counts
as a breaker failure (throttling is expected, not an outage), while a
transient/protocol error does.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable

from ...ingest import poll_index
from ...pypi_client import (
    PyPIClient,
    PyPINotFound,
    PyPIProtocolError,
    PyPIRateLimited,
    PyPITransientError,
)
from ..circuit_breaker import CircuitBreaker

logger = logging.getLogger("reroll_sync.index_poll")


class IndexPollStage:
    """Owns the simple index's etag across polls and feeds `project_sync`'s queue."""

    def __init__(
        self,
        client: PyPIClient,
        reader_conn: sqlite3.Connection,
        breaker: CircuitBreaker,
        *,
        enqueue: Callable[[str], None],
    ) -> None:
        self._client = client
        self._reader_conn = reader_conn
        self._breaker = breaker
        self._enqueue = enqueue
        self._etag: str | None = None

    def iterate(self) -> bool:
        """Poll once if the breaker allows it. Returns whether any project was enqueued."""
        if not self._breaker.allow():
            return False
        try:
            result = poll_index(self._client, self._reader_conn, etag=self._etag)
        except PyPIRateLimited:
            return False
        except (PyPITransientError, PyPIProtocolError, PyPINotFound) as exc:
            self._breaker.record_failure()
            logger.error("index poll failed: %s", exc)
            return False

        self._breaker.record_success()
        if result.etag is not None:
            self._etag = result.etag
        if result.not_modified:
            return False
        for name in result.stale_projects:
            self._enqueue(name)
        return bool(result.stale_projects)
