"""The `index_poll` stage: conditionally re-fetches the PyPI simple index
and forwards each stale project name to `project_sync`.

Guarded by the `pypi.org` circuit breaker: `PyPIRateLimited` never counts
as a breaker failure (throttling is expected, not an outage), while a
transient/protocol error does.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass

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


@dataclass(frozen=True)
class IndexPollSnapshot:
    """The last poll's observed freshness, for `health.snapshot()`.

    All three are ``None`` until the first poll completes.
    ``last_remote_serial`` is the simple index's own ``_last-serial`` as of
    the most recent non-``304`` response; ``last_change_at`` only advances
    on a poll that actually found stale projects, so it answers "when did
    the index last change" rather than "when did we last ask".
    """

    last_remote_serial: int | None
    last_poll_at: float | None
    last_change_at: float | None


class IndexPollStage:
    """Owns the simple index's etag across polls and feeds `project_sync`'s queue."""

    def __init__(
        self,
        client: PyPIClient,
        reader_conn: sqlite3.Connection,
        breaker: CircuitBreaker,
        *,
        enqueue: Callable[[str], None],
        now: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._reader_conn = reader_conn
        self._breaker = breaker
        self._enqueue = enqueue
        self._now = now
        self._etag: str | None = None
        self._last_remote_serial: int | None = None
        self._last_poll_at: float | None = None
        self._last_change_at: float | None = None

    def snapshot(self) -> IndexPollSnapshot:
        """Return the last poll's observed remote serial and timing."""
        return IndexPollSnapshot(
            last_remote_serial=self._last_remote_serial,
            last_poll_at=self._last_poll_at,
            last_change_at=self._last_change_at,
        )

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
        self._last_poll_at = self._now()
        if result.etag is not None:
            self._etag = result.etag
        if result.not_modified:
            return False
        # poll_index only leaves remote_global_serial unset on the
        # not_modified path, already handled above, so it is always an
        # int here.
        self._last_remote_serial = result.remote_global_serial
        for name in result.stale_projects:
            self._enqueue(name)
        if result.stale_projects:
            self._last_change_at = self._last_poll_at
        return bool(result.stale_projects)
