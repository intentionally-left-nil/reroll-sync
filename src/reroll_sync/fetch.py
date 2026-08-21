"""Fetches each wheel's ``.metadata`` sidecar, archives it, and hands the
same in-memory bytes to convert. Replaces ``metadata_sync.py``'s R2 round
trip; see ``specs/09-metadata-fetch-stage.md``.

Deviation from spec 09: ``PyPINotFound`` and ``MetadataHashMismatch`` fetch
failures both adapt onto ``dispatcher.Retry``, not the ``Skip`` (targeting
``wheels.state = NO_METADATA``) the spec's prose describes.
``schema.ALLOWED_TRANSITIONS`` has no ``NEED_METADATA -> SKIPPED`` edge, and
routing either failure onto ``NO_METADATA`` conflates "PyPI publishes no
sidecar at all" with "a sidecar exists but is corrupt" -- and, because
``ingest.py``'s ``NO_METADATA -> NEED_METADATA`` transition checks the
*current* value of ``has_metadata`` rather than whether it just flipped,
lets ordinary, unrelated project churn silently re-queue (and thus
un-skip) a wheel whose actual problem was never fixed. ``Retry`` never
touches ``wheels.state`` (dispatcher.py's own invariant), so the wheel
stays ``NEED_METADATA`` and is retried through the existing backoff/
quarantine machinery (spec 07): exponential backoff up to ``max_attempts``
(8 by default, ~2h total), then quarantine over the
``NEED_METADATA -> QUARANTINED`` edge ``ALLOWED_TRANSITIONS`` already has.
For ``MetadataHashMismatch``, both the expected and actual sha256 digests
are carried in ``Retry.details``, which ``dispatcher._retry_op`` persists
to ``work.last_error`` on every attempt and to ``errors.details`` once the
wheel is quarantined -- no separate ``errors`` write is needed here.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from .archive.errors import CorruptSegmentError
from .archive.location import BlobLocation
from .archive.reader import SegmentReader
from .archive.store import ArchiveStore
from .archive.writer import SegmentStats
from .dispatcher import (
    Dispatcher,
    Ok,
    Outcome,
    QueueItem,
    RateLimited,
    Retry,
    Skip,
    Stage,
    StagePayloadWriter,
)
from .pypi_client import (
    MetadataHashMismatch,
    PyPIClient,
    PyPINotFound,
    PyPIProtocolError,
    PyPIRateLimited,
    PyPITransientError,
)
from .schema import WheelState
from .writer import WriteOp, Writer, read_txn

logger = logging.getLogger(__name__)

_NOT_A_REROLL_ERROR = "reroll-sync"
"""``FetchSkip.reroll_version`` marker for a check that is fixed and does
not vary with reroll -- ported from the deleted ``metadata_sync.py``.
"""

DEFAULT_QUEUE_BUDGET_BYTES = 256 * 1024 * 1024
"""Default byte budget for :class:`ByteBudgetedQueue`, well above the PyPI
client's 32 MB single-response cap so one maximum response never deadlocks.
"""

_PROGRAMMING_ERROR_REASON = "missing_metadata_hash_and_flag"
_METADATA_MISSING_REASON = "metadata_missing"
_METADATA_HASH_MISMATCH_REASON = "metadata_hash_mismatch"
_TRANSIENT_REASON = "pypi_transient_error"
_CORRUPT_BLOB_REASON = "corrupt_blob"
_MISSING_SEGMENT_REASON = "archive_segment_missing"
_MISSING_BLOB_REASON = "archive_blob_missing"
_RECOVERY_CHUNK_SIZE = 500


# ---------------------------------------------------------------------------
# fetch_one
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchItem:
    """One claimable wheel plus the columns :func:`fetch_one` needs beyond
    :class:`~reroll_sync.dispatcher.QueueItem`.

    ``has_metadata`` is never persisted on ``wheels`` (see ``schema.py``):
    a row can only reach ``NEED_METADATA`` -- the state fetch claims from
    -- because ingestion already observed ``has_metadata=True``. Real
    daemon wiring therefore always populates it as ``True``; ``False`` is
    reachable only by a test constructing this dataclass directly, to
    exercise the "should never happen" guard in :func:`fetch_one`.
    """

    id: int
    project: str
    lane: int
    state: WheelState
    filename: str
    url: str
    metadata_sha256: str | None
    has_metadata: bool = True


@dataclass(frozen=True)
class FetchOk:
    """A wheel's ``.metadata`` sidecar was downloaded and verified."""

    data: bytes
    sha256: str


@dataclass(frozen=True)
class FetchSkip:
    """A fetch failure attributable to PyPI's own published index/sidecar
    state: a claimed sidecar doesn't exist, or its bytes don't match the
    published hash.

    :func:`adapt_fetch_outcome` maps this onto ``dispatcher.Retry``, not
    ``dispatcher.Skip`` or ``Ok(NO_METADATA, ...)`` -- see this module's
    docstring for why. ``permanent`` and ``reroll_version`` describe the
    failure the way spec 09 itself conceives of it and are asserted on
    directly by ``fetch_one``'s own tests; the adapter does not consume
    them.
    """

    reason: str
    subcategory: str
    details: str
    permanent: bool
    reroll_version: str | None


@dataclass(frozen=True)
class FetchRetry:
    """A failure that says nothing about the wheel: try again later."""

    reason: str
    details: str


@dataclass(frozen=True)
class FetchRateLimited:
    """The fetch was throttled; not an attempt against the wheel at all."""

    child: str
    retry_after: float | None


FetchOutcome = FetchOk | FetchSkip | FetchRetry | FetchRateLimited


def fetch_one(client: PyPIClient, wheel: FetchItem, *, now: Callable[[], float]) -> FetchOutcome:
    """Download one wheel's ``.metadata`` sidecar. Performs no database writes.

    ``now`` is accepted for parity with every other stage's signature but
    is not otherwise used: this function makes no retry/backoff decision
    of its own, and structured logging already timestamps itself.
    """
    if wheel.metadata_sha256 is None and not wheel.has_metadata:
        logger.error(
            "wheel %r (id=%d) was claimed for fetch with no metadata hash and "
            "has_metadata=False; ingestion should have set NO_METADATA instead",
            wheel.filename,
            wheel.id,
        )
        return FetchRetry(
            reason=_PROGRAMMING_ERROR_REASON,
            details=f"wheel {wheel.filename!r} (id={wheel.id}) should have been NO_METADATA",
        )

    url = f"{wheel.url}.metadata"
    try:
        data = client.fetch_metadata(url, wheel.metadata_sha256)
    except PyPINotFound as exc:
        return FetchSkip(
            reason=_METADATA_MISSING_REASON,
            subcategory=type(exc).__name__,
            details=str(exc),
            permanent=True,
            reroll_version=None,
        )
    except MetadataHashMismatch as exc:
        return FetchSkip(
            reason=_METADATA_HASH_MISMATCH_REASON,
            subcategory=type(exc).__name__,
            details=f"expected sha256={exc.expected}, actual sha256={exc.actual}",
            permanent=False,
            reroll_version=_NOT_A_REROLL_ERROR,
        )
    except (PyPITransientError, PyPIProtocolError) as exc:
        return FetchRetry(reason=_TRANSIENT_REASON, details=str(exc))
    except PyPIRateLimited as exc:
        return FetchRateLimited(child=_host_of(url), retry_after=exc.retry_after)

    return FetchOk(data=data, sha256=hashlib.sha256(data).hexdigest())


def adapt_fetch_outcome(outcome: FetchSkip | FetchRetry | FetchRateLimited) -> Outcome:
    """Map a non-``Ok`` :data:`FetchOutcome` onto the generic dispatcher :data:`Outcome` union.

    ``FetchOk`` has no adapter here: it needs an archive location that only
    exists once the archive thread has written the bytes to a segment, so
    its ``Ok`` is built by :class:`ArchiveHandoff`, not by this function.

    ``FetchSkip`` maps onto ``Retry``, exactly like ``FetchRetry`` does --
    see this module's docstring for why a fetch failure that spec 09 itself
    calls a "Skip" is deliberately routed through the retry/quarantine path
    instead of ``Ok(NO_METADATA, ...)``.
    """
    if isinstance(outcome, (FetchSkip, FetchRetry)):
        return Retry(reason=outcome.reason, details=outcome.details)
    if isinstance(outcome, FetchRateLimited):
        return RateLimited(child=outcome.child, seconds=outcome.retry_after or 0.0)
    raise TypeError(f"unsupported fetch outcome: {outcome!r}")


def dispatch_fetch_item(
    client: PyPIClient,
    item: FetchItem,
    *,
    dispatcher: Dispatcher,
    enqueue: Callable[[FetchItem, bytes, str], None],
    now: Callable[[], float],
) -> FetchOutcome:
    """Fetch one wheel and route its outcome: ``enqueue`` on success, or an
    immediate :meth:`Dispatcher.apply_outcome` for every other outcome.

    Returns the raw :class:`FetchOutcome`, mainly so callers/tests can
    assert on it without re-deriving it from side effects.
    """
    outcome = fetch_one(client, item, now=now)
    if isinstance(outcome, FetchOk):
        enqueue(item, outcome.data, outcome.sha256)
    else:
        adapted = adapt_fetch_outcome(outcome)
        dispatcher.apply_outcome(Stage.FETCH, _as_queue_item(item), adapted)
    return outcome


# ---------------------------------------------------------------------------
# The byte-budgeted handoff queue
# ---------------------------------------------------------------------------


class QueueClosed(RuntimeError):
    """Raised by :meth:`ByteBudgetedQueue.put` once the queue has been closed."""


class ByteBudgetedQueue:
    """A FIFO queue bounded by the cumulative byte size of its items, not their count.

    ``budget_bytes`` must exceed the largest single item this queue will
    ever hold: :meth:`put` always accepts an item into an otherwise-empty
    (and nothing in flight) queue regardless of its size, so one
    maximum-size response can never deadlock a producer. It blocks only
    while the queue already holds -- queued *or* popped-but-not-yet-
    :meth:`release`-d -- enough bytes that adding more would exceed budget.

    A consumer's bytes stay charged against the budget after :meth:`get`
    until it calls :meth:`release`: the memory an item occupies isn't
    freed just because it left the deque, only once the consumer is
    actually done with it (e.g. after handing it to a convert pool).
    """

    def __init__(self, budget_bytes: int = DEFAULT_QUEUE_BUDGET_BYTES) -> None:
        if budget_bytes <= 0:
            raise ValueError("budget_bytes must be positive")
        self._budget_bytes = budget_bytes
        self._items: deque[object] = deque()
        self._current_bytes = 0
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)
        self._closed = False

    @property
    def budget_bytes(self) -> int:
        return self._budget_bytes

    def put(self, item: object, size: int) -> None:
        """Enqueue ``item``, blocking while the budget is exceeded and non-empty."""
        with self._not_full:
            while (
                not self._closed
                and self._current_bytes > 0
                and self._current_bytes + size > self._budget_bytes
            ):
                self._not_full.wait()
            if self._closed:
                raise QueueClosed("cannot put onto a closed ByteBudgetedQueue")
            self._items.append(item)
            self._current_bytes += size
            self._not_empty.notify()

    def get(self) -> object | None:
        """Pop the next item, blocking until one is available, or return ``None``
        once the queue has been closed and fully drained. The popped item's
        bytes remain charged against the budget until :meth:`release`.
        """
        with self._not_empty:
            while not self._items and not self._closed:
                self._not_empty.wait()
            if not self._items:
                return None
            return self._items.popleft()

    def release(self, size: int) -> None:
        """Free ``size`` bytes previously charged by a matching :meth:`put`,
        once a consumer that popped it via :meth:`get` is done with it.
        """
        with self._not_full:
            self._current_bytes -= size
            self._not_full.notify()

    def close(self) -> None:
        """Mark the queue closed: no further ``put`` succeeds, and ``get`` returns
        ``None`` once already-queued items are drained.
        """
        with self._lock:
            self._closed = True
            self._not_empty.notify_all()
            self._not_full.notify_all()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def current_bytes(self) -> int:
        with self._lock:
            return self._current_bytes


# ---------------------------------------------------------------------------
# The archive/convert handoff
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HandoffItem:
    """One successfully-fetched wheel, queued for the archive thread."""

    queue_item: QueueItem
    filename: str
    data: bytes
    sha256: str


class ArchiveHandoff:
    """The single archive thread's job: drain the handoff queue, archive each
    item's bytes, apply the wheel's ``Ok`` outcome, and forward the same
    bytes to convert.

    ``store`` performs its own immediate commits for segment allocation
    and the ``blobs`` row (:meth:`ArchiveStore.add`/``open_writer``) on its
    own dedicated connection -- crash recovery (:func:`recover_unsealed_segment`)
    is what makes that safe without waiting for a seal. Sealing itself is
    different: the ``segments``-row update is submitted through ``writer``
    (the single runtime writer thread) as an explicit :class:`WriteOp`,
    per the module's design -- see ``specs/09-metadata-fetch-stage.md``.

    Only the archive thread may ever call :meth:`process_one`/:meth:`run`
    on a given instance: neither ``store`` nor this class synchronizes
    against concurrent callers.
    """

    def __init__(
        self,
        queue: ByteBudgetedQueue,
        store: ArchiveStore,
        dispatcher: Dispatcher,
        writer: Writer,
        on_archived: Callable[[QueueItem, str, bytes], None],
        *,
        wall_clock: Callable[[], str] = lambda: datetime.now(UTC).isoformat(),
    ) -> None:
        self._queue = queue
        self._store = store
        self._dispatcher = dispatcher
        self._writer = writer
        self._on_archived = on_archived
        self._wall_clock = wall_clock

    def process_one(self) -> bool:
        """Pop and fully process one item. Returns ``False`` once the queue is
        closed and drained (the signal to stop the archive loop).
        """
        popped = self._queue.get()
        if popped is None:
            return False
        item = popped
        assert isinstance(item, HandoffItem)

        try:
            location = self._store.add(item.data)
            self._maybe_seal()

            ok = Ok(next_state=WheelState.NEED_CONVERT, write=_blob_link_writer(location.sha256))
            self._dispatcher.apply_outcome(Stage.FETCH, item.queue_item, ok)
            self._on_archived(item.queue_item, item.filename, item.data)
        finally:
            self._queue.release(len(item.data))
        return True

    def run(self) -> None:
        """Process items until the queue is closed and drained."""
        while self.process_one():
            pass

    def _maybe_seal(self) -> None:
        current = self._store.current_writer()
        if not current.should_seal():
            return
        stats = current.seal()
        self._writer.submit_and_wait(_seal_write_op(stats, wall_clock=self._wall_clock))
        self._store.open_writer()


# ---------------------------------------------------------------------------
# Crash recovery for an unsealed segment
# ---------------------------------------------------------------------------


def recover_unsealed_segment(
    store: ArchiveStore, writer: Writer, segment_id: int, *, chunk_size: int = _RECOVERY_CHUNK_SIZE
) -> int:
    """Reset every wheel whose ``blob_sha256`` lived in the truncated, unsealed
    ``segment_id``: clears ``blob_sha256`` and resets ``state`` to
    ``NEED_METADATA``, and removes the segment's now-stale ``blobs`` rows.

    Called by daemon startup after :class:`ArchiveStore`'s own constructor
    truncates a stale ``.open`` file with no usable footer (spec 10's
    responsibility to invoke; this function only implements the cleanup
    itself). Chunked so no single ``WriteOp`` handles an unbounded number
    of wheels. Returns the total number of wheels reset.
    """
    blob_rows = store.blob_rows_for_segment(segment_id)
    sha256s = [row[0] for row in blob_rows]
    total = 0
    for start in range(0, len(sha256s), chunk_size):
        chunk = sha256s[start : start + chunk_size]
        total += writer.submit_and_wait(_recover_chunk_write_op(chunk, segment_id))
    return total


# ---------------------------------------------------------------------------
# Bulk convert from the archive
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BulkConvertReady:
    """One record read back out of the archive, ready for convert."""

    queue_item: QueueItem
    filename: str
    data: bytes


class BulkConvertSource:
    """Feeds the convert pool from already-archived blobs instead of a fresh fetch.

    Claims ``NEED_CONVERT`` items with ``blob_sha256`` set, groups them by
    ``(segment_id, block_no)`` before reading so each block is decompressed
    at most once per :meth:`claim_batch` call regardless of claim order.
    """

    def __init__(
        self,
        store: ArchiveStore,
        dispatcher: Dispatcher,
        conn: sqlite3.Connection,
        *,
        reader: SegmentReader | None = None,
        read_budget: float = 0.25,
    ) -> None:
        self._store = store
        self._dispatcher = dispatcher
        self._conn = conn
        self._reader = reader if reader is not None else store.reader
        self._read_budget = read_budget

    def claim_batch(self, limit: int) -> list[BulkConvertReady]:
        """Claim up to ``limit`` items and return the ones successfully read
        back from the archive, ready for convert.

        A claimed item whose blob can't be located or read is resolved
        directly (``Retry`` for a missing/unreadable segment, ``Skip`` for
        a blob that fails its sha256 check) rather than included in the
        returned batch, so a single bad blob never poisons the rest.
        """
        items = self._dispatcher.claim(Stage.CONVERT, limit)
        if not items:
            return []

        rows = self._load_rows([item.id for item in items])
        by_sha256: dict[str, list[tuple[QueueItem, str]]] = {}
        for item in items:
            row = rows.get(item.id)
            filename, blob_sha256 = row if row is not None else (None, None)
            if filename is None or blob_sha256 is None:
                logger.error(
                    "wheel id=%d claimed for bulk convert with no blob_sha256; releasing", item.id
                )
                self._dispatcher.release(Stage.CONVERT, item.id)
                continue
            by_sha256.setdefault(blob_sha256, []).append((item, filename))

        locations = self._load_locations(list(by_sha256))
        grouped: dict[tuple[int, int], list[tuple[str, QueueItem, str]]] = {}
        for sha256, entries in by_sha256.items():
            location = locations.get(sha256)
            if location is None:
                for item, _filename in entries:
                    self._retry(
                        item, reason=_MISSING_BLOB_REASON, details=f"no blobs row for {sha256}"
                    )
                continue
            key = (location.segment_id, location.block_no)
            for item, filename in entries:
                grouped.setdefault(key, []).append((sha256, item, filename))

        ready: list[BulkConvertReady] = []
        for key in sorted(grouped):
            segment_id = key[0]
            for sha256, item, filename in grouped[key]:
                location = locations[sha256]
                try:
                    data = self._reader.read(location)
                except FileNotFoundError:
                    self._retry(
                        item,
                        reason=_MISSING_SEGMENT_REASON,
                        details=f"segment {segment_id:06d} missing on disk",
                    )
                    continue
                except CorruptSegmentError as exc:
                    self._skip_corrupt(item, sha256=sha256, details=str(exc))
                    continue
                ready.append(BulkConvertReady(queue_item=item, filename=filename, data=data))
        return ready

    def _retry(self, item: QueueItem, *, reason: str, details: str) -> None:
        self._dispatcher.apply_outcome(Stage.CONVERT, item, Retry(reason=reason, details=details))

    def _skip_corrupt(self, item: QueueItem, *, sha256: str, details: str) -> None:
        outcome = Skip(
            reason=_CORRUPT_BLOB_REASON,
            subcategory="CorruptSegmentError",
            details=f"blob {sha256}: {details}",
            permanent=False,
            reroll_version=None,
        )
        self._dispatcher.apply_outcome(Stage.CONVERT, item, outcome)

    def _load_rows(self, ids: list[int]) -> dict[int, tuple[str, str | None]]:
        placeholders = ", ".join("?" for _ in ids)
        sql = f"SELECT id, filename, blob_sha256 FROM wheels WHERE id IN ({placeholders})"
        with read_txn(self._conn, budget=self._read_budget, label="fetch.bulk_convert.rows"):
            rows = self._conn.execute(sql, ids).fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}

    def _load_locations(self, sha256s: list[str]) -> dict[str, BlobLocation]:
        if not sha256s:
            return {}
        placeholders = ", ".join("?" for _ in sha256s)
        sql = (
            "SELECT sha256, segment_id, block_no, offset, length FROM blobs "
            f"WHERE sha256 IN ({placeholders})"
        )
        with read_txn(self._conn, budget=self._read_budget, label="fetch.bulk_convert.locations"):
            rows = self._conn.execute(sql, sha256s).fetchall()
        return {row[0]: BlobLocation(*row) for row in rows}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _as_queue_item(item: FetchItem) -> QueueItem:
    return QueueItem(id=item.id, project=item.project, lane=item.lane, state=item.state)


def _host_of(url: str) -> str:
    """Return ``url``'s host, mirroring ``pypi_client``'s own rate-limiter key.

    ``PyPIRateLimited`` doesn't carry which host was limited (spec 04), so
    this is recomputed here from the same ``.metadata`` URL ``fetch_one``
    requested, the same way ``ingest.py``'s ``_limiter_child_for`` does for
    project pages.
    """
    return httpx.URL(url).host


def _blob_link_writer(blob_sha256: str) -> StagePayloadWriter:
    def _write(conn: sqlite3.Connection, wheel_id: int) -> None:
        conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (blob_sha256, wheel_id))

    return _write


def _seal_write_op(stats: SegmentStats, *, wall_clock: Callable[[], str]) -> WriteOp:
    def _apply(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE segments SET sealed_at = ?, bytes = ?, records = ?, footer_sha = ? "
            "WHERE id = ?",
            (wall_clock(), stats.bytes, stats.records, stats.footer_sha256, stats.segment_id),
        )

    return WriteOp(name="fetch.seal_segment", apply=_apply)


def _recover_chunk_write_op(chunk: list[str], segment_id: int) -> WriteOp:
    placeholders = ", ".join("?" for _ in chunk)

    def _apply(conn: sqlite3.Connection) -> int:
        cursor = conn.execute(
            f"UPDATE wheels SET state = ?, blob_sha256 = NULL "
            f"WHERE blob_sha256 IN ({placeholders}) AND state != ?",
            (int(WheelState.NEED_METADATA), *chunk, int(WheelState.DELETED)),
        )
        affected = cursor.rowcount
        conn.execute(f"DELETE FROM blobs WHERE sha256 IN ({placeholders})", chunk)
        return affected

    return WriteOp(name=f"fetch.recover_segment.{segment_id}", apply=_apply)
