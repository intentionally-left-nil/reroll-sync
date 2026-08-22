"""The `project_sync` stage: drains project names `index_poll` (or a manual
`reprocess`) enqueues and reconciles each against PyPI via `ingest.py`.

`sync_project` never raises a PyPI error -- it always turns one into a
`ProjectSyncOutcome` -- so the `pypi.org` breaker's success/failure signal
here is derived from the batch's `IngestSummary` rather than from a caught
exception: any real success (insert/update/tombstone/gone) means the
dependency answered, even if a sibling project in the same batch hit a
transient error.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable

from ...ingest import Penalizer, ProjectBackoff, ingest_stale_projects
from ...pypi_client import PyPIClient
from ...writer import Writer
from ..circuit_breaker import CircuitBreaker

DEFAULT_MAX_WORKERS = 32


class ProjectSyncStage:
    """Owns the pending-project-name queue and reconciles a batch per iteration."""

    def __init__(
        self,
        client: PyPIClient,
        reader_conn_factory: Callable[[], sqlite3.Connection],
        writer: Writer,
        limiter: Penalizer | None,
        breaker: CircuitBreaker,
        *,
        max_workers: int = DEFAULT_MAX_WORKERS,
        batch_limit: int | None = None,
        backoff: ProjectBackoff | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._reader_conn_factory = reader_conn_factory
        self._writer = writer
        self._limiter = limiter
        self._breaker = breaker
        self._max_workers = max_workers
        self._batch_limit = batch_limit if batch_limit is not None else max_workers
        self._backoff = backoff if backoff is not None else ProjectBackoff(now=now)
        self._now = now
        self._lock = threading.Lock()
        self._pending: dict[str, None] = {}

    def enqueue(self, name: str) -> None:
        """Add `name` to the pending set (a no-op if it's already queued)."""
        with self._lock:
            self._pending[name] = None

    def iterate(self) -> bool:
        """Reconcile up to `batch_limit` pending projects. Returns whether any were attempted."""
        names = self._drain(self._batch_limit)
        if not names:
            return False
        if not self._breaker.allow():
            self._requeue(names)
            return False

        summary = ingest_stale_projects(
            self._client,
            self._reader_conn_factory,
            self._writer,
            self._limiter,
            names,
            now=self._now,
            max_workers=self._max_workers,
            backoff=self._backoff,
        )
        succeeded_anything = bool(
            summary.inserted or summary.updated or summary.tombstoned or summary.projects_gone
        )
        # A batch that was purely rate-limited (or entirely eligibility-
        # filtered by `backoff`) says nothing about whether pypi.org is up,
        # so it leaves the breaker untouched.
        if summary.retried and not succeeded_anything:
            self._breaker.record_failure()
        elif succeeded_anything:
            self._breaker.record_success()
        return True

    def _drain(self, limit: int) -> list[str]:
        with self._lock:
            names = list(self._pending)[:limit]
            for name in names:
                del self._pending[name]
            return names

    def _requeue(self, names: list[str]) -> None:
        with self._lock:
            for name in names:
                self._pending.setdefault(name, None)
