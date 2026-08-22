"""The `fetch` stage: claims `NEED_METADATA` wheels, downloads each
sidecar over a thread pool, and hands successes to the archive queue.

`fetch_one` never raises a PyPI error -- it always turns one into a
`FetchOutcome` -- so the `files.pythonhosted.org` breaker's success/failure
signal here is derived from the batch's outcomes rather than a caught
exception: a raw `FetchRetry` (a transient network/protocol error) counts
against it; `FetchRateLimited` (throttling, expected) and `FetchSkip` (PyPI's
own index answered, just inconsistently) do not.

Claiming and outcome application happen on this stage's own single thread,
never from a worker thread: `Dispatcher.claim`/`apply_outcome`/`release`
mutate plain, unlocked dicts/sets, so only one thread may ever call them for
a given `Dispatcher` instance.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from concurrent.futures import Executor, Future, as_completed
from dataclasses import dataclass

from ...dispatcher import Dispatcher, QueueItem, Stage
from ...fetch import (
    ByteBudgetedQueue,
    FetchItem,
    FetchOk,
    FetchOutcome,
    FetchRetry,
    HandoffItem,
    adapt_fetch_outcome,
    fetch_one,
)
from ...pypi_client import PyPIClient
from ...writer import read_txn
from ..circuit_breaker import CircuitBreaker
from ..disk_guard import DiskGuardLike

logger = logging.getLogger("reroll_sync.fetch")

DEFAULT_LIMIT = 256
DEFAULT_READ_BUDGET = 0.25


@dataclass(frozen=True)
class _ClaimedRow:
    filename: str
    url: str
    metadata_sha256: str | None


class FetchStage:
    """Owns one iteration's worth of fetch claiming/dispatch/handoff."""

    def __init__(
        self,
        client: PyPIClient,
        dispatcher: Dispatcher,
        reader_conn: sqlite3.Connection,
        handoff_queue: ByteBudgetedQueue,
        breaker: CircuitBreaker,
        *,
        pool: Executor,
        limit: int = DEFAULT_LIMIT,
        now: Callable[[], float] = time.time,
        read_budget: float = DEFAULT_READ_BUDGET,
        disk_guard: DiskGuardLike | None = None,
        shutdown_event: threading.Event | None = None,
    ) -> None:
        self._client = client
        self._dispatcher = dispatcher
        self._reader_conn = reader_conn
        self._handoff_queue = handoff_queue
        self._breaker = breaker
        self._pool = pool
        self._limit = limit
        self._now = now
        self._read_budget = read_budget
        self._disk_guard = disk_guard
        self._shutdown_event = shutdown_event

    def iterate(self) -> bool:
        """Claim and dispatch one batch, unless the breaker or `disk_guard` says not to.

        `disk_guard` is checked first: a full disk is a reason to stop
        fetching new sidecars regardless of whether files.pythonhosted.org
        itself is healthy (spec 10: "pause the fetch and archive stages").
        """
        if self._disk_guard is not None and self._disk_guard.is_paused():
            return False
        if not self._breaker.allow():
            return False
        items = self._dispatcher.claim(Stage.FETCH, self._limit)
        if not items:
            return False
        return self._dispatch_batch(items)

    def _dispatch_batch(self, items: list[QueueItem]) -> bool:
        rows = self._load_rows([item.id for item in items])
        fetch_pairs: list[tuple[QueueItem, FetchItem]] = []
        for item in items:
            row = rows.get(item.id)
            if row is None:
                logger.error(
                    "wheel id=%d claimed for fetch with no matching wheels row; releasing",
                    item.id,
                )
                self._dispatcher.release(Stage.FETCH, item.id)
                continue
            fetch_pairs.append(
                (
                    item,
                    FetchItem(
                        id=item.id,
                        project=item.project,
                        lane=item.lane,
                        state=item.state,
                        filename=row.filename,
                        url=row.url,
                        metadata_sha256=row.metadata_sha256,
                    ),
                )
            )
        if not fetch_pairs:
            return False

        futures = {
            self._pool.submit(fetch_one, self._client, fetch_item, now=self._now): (
                item,
                fetch_item,
            )
            for item, fetch_item in fetch_pairs
        }
        saw_ok = False
        saw_transient_failure = False
        pending = set(futures)
        for future in as_completed(futures):
            pending.discard(future)
            item, fetch_item = futures[future]
            outcome = future.result()
            if isinstance(outcome, FetchOk):
                saw_ok = True
                handoff = HandoffItem(
                    queue_item=item,
                    filename=fetch_item.filename,
                    data=outcome.data,
                    sha256=outcome.sha256,
                )
                self._handoff_queue.put(handoff, size=len(outcome.data))
            else:
                if isinstance(outcome, FetchRetry):
                    saw_transient_failure = True
                self._dispatcher.apply_outcome(Stage.FETCH, item, adapt_fetch_outcome(outcome))
            if self._shutdown_event is not None and self._shutdown_event.is_set():
                break

        self._abandon_pending(pending, futures)

        # A pure FetchSkip/FetchRateLimited batch (PyPI's index itself
        # answered, or simply throttled us) says nothing about whether
        # files.pythonhosted.org is up, so it leaves the breaker untouched.
        if saw_transient_failure:
            self._breaker.record_failure()
        elif saw_ok:
            self._breaker.record_success()
        return True

    def _abandon_pending(
        self,
        pending: set[Future[FetchOutcome]],
        futures: dict[Future[FetchOutcome], tuple[QueueItem, FetchItem]],
    ) -> None:
        """Release every item in `pending` without waiting for or applying its outcome.

        Called once shutdown has been signaled mid-batch: spec 10 requires
        a stage loop to exit promptly on a shutdown event, not block for
        however long the rest of a large, rate-limited batch takes to
        finish. A still-running fetch keeps running in the pool -- there is
        no way to interrupt it -- but a not-yet-started one is cancelled
        outright; either way, its item goes back to `NEED_METADATA` to be
        re-claimed on the next run rather than risk handing off to a
        `handoff_queue`/`Writer` that `Daemon.shutdown` may already be
        tearing down.
        """
        for future in pending:
            future.cancel()
            item, _fetch_item = futures[future]
            self._dispatcher.release(Stage.FETCH, item.id)

    def _load_rows(self, ids: list[int]) -> dict[int, _ClaimedRow]:
        placeholders = ", ".join("?" for _ in ids)
        sql = f"SELECT id, filename, url, metadata_sha256 FROM wheels WHERE id IN ({placeholders})"
        with read_txn(self._reader_conn, budget=self._read_budget, label="daemon.fetch.rows"):
            rows = self._reader_conn.execute(sql, ids).fetchall()
        return {
            row[0]: _ClaimedRow(filename=row[1], url=row[2], metadata_sha256=row[3]) for row in rows
        }
