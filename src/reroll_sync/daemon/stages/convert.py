"""The `convert` stage: fed by both the fetch/archive handoff (freshly
archived bytes, via `on_archived`) and bulk reads of already-archived but
unconverted wheels (`BulkConvertSource`), over a process pool.

`ArchiveHandoff.on_archived` hands back the *fetch* claim's `QueueItem`,
whose `state` field is stale (`NEED_METADATA`): `ArchiveHandoff` only
updates `Dispatcher`'s `Stage.FETCH` bookkeeping, and the wheel's real
current state is already `NEED_CONVERT` by the time this callback runs.
Applying a convert outcome with that stale state would violate
`schema.ALLOWED_TRANSITIONS` (no `NEED_METADATA -> READY`/`SKIPPED` edge),
so `on_archived` rebuilds a fresh `QueueItem` with the correct state
instead of passing the stale one through.
"""

from __future__ import annotations

import queue
from collections.abc import Callable
from concurrent.futures import Executor, as_completed
from dataclasses import dataclass

from ...convert import convert_in_worker
from ...dispatcher import Dispatcher, Outcome, QueueItem, Stage, adapt_convert_outcome
from ...fetch import BulkConvertSource
from ...schema import WheelState

DEFAULT_LIMIT = 256
DEFAULT_QUEUE_MAXSIZE = 10_000


@dataclass(frozen=True)
class _ConvertQueueItem:
    queue_item: QueueItem
    filename: str
    data: bytes


class ConvertStage:
    """Owns the fed-queue/bulk-claim split for one convert iteration."""

    def __init__(
        self,
        dispatcher: Dispatcher,
        bulk_source: BulkConvertSource,
        pool: Executor,
        *,
        reroll_version: str,
        limit: int = DEFAULT_LIMIT,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
        on_applied: Callable[[QueueItem, Outcome], None] | None = None,
    ) -> None:
        self._dispatcher = dispatcher
        self._bulk_source = bulk_source
        self._pool = pool
        self._reroll_version = reroll_version
        self._limit = limit
        self._fed_queue: queue.Queue[_ConvertQueueItem] = queue.Queue(maxsize=queue_maxsize)
        self._on_applied = on_applied
        """Test-only hook: called with `(queue_item, outcome)` right after
        each converted item's outcome is applied (and thus committed --
        `Dispatcher.apply_outcome` blocks on `Writer.submit_and_wait`).
        `None` in production; a test wires this to synchronize
        deterministically instead of polling for a DB state change.
        """

    def on_archived(self, queue_item: QueueItem, filename: str, data: bytes) -> None:
        """Feed one just-archived wheel straight into convert, bypassing bulk reclaim.

        Matches `fetch.ArchiveHandoff`'s `on_archived` callback signature.
        """
        fresh_item = QueueItem(
            id=queue_item.id,
            project=queue_item.project,
            lane=queue_item.lane,
            state=WheelState.NEED_CONVERT,
        )
        self._fed_queue.put(_ConvertQueueItem(fresh_item, filename, data))

    def iterate(self) -> bool:
        """Convert one batch, fed queue first, bulk-claimed archive reads otherwise."""
        batch = self._drain_fed_queue(self._limit)
        if not batch:
            batch = [
                _ConvertQueueItem(ready.queue_item, ready.filename, ready.data)
                for ready in self._bulk_source.claim_batch(self._limit)
            ]
        if not batch:
            return False

        futures = {
            self._pool.submit(convert_in_worker, item.data, item.filename): item for item in batch
        }
        for future in as_completed(futures):
            item = futures[future]
            outcome = future.result()
            adapted = adapt_convert_outcome(outcome, reroll_version=self._reroll_version)
            self._dispatcher.apply_outcome(Stage.CONVERT, item.queue_item, adapted)
            if self._on_applied is not None:
                self._on_applied(item.queue_item, adapted)
        return True

    def _drain_fed_queue(self, limit: int) -> list[_ConvertQueueItem]:
        items: list[_ConvertQueueItem] = []
        for _ in range(limit):
            try:
                items.append(self._fed_queue.get_nowait())
            except queue.Empty:
                break
        return items
