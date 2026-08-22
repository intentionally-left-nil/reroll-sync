"""Selects work from the derived queues and turns stage outcomes into writes.

The only place retry policy, quarantine, and skip attribution live. A stage
returns one of a small outcome union (``Ok``/``Skip``/``Retry``/
``RateLimited``); an adapter (e.g. :func:`adapt_convert_outcome`) maps a
stage's own outcome type onto this union so this module never needs to know
about a stage's specific result type. See ``specs/07-dispatcher-and-queues.md``
and ``docs/pipeline.md``.

Two invariants a later change must not violate:

* ``Ok`` and ``Skip`` always delete the ``work`` row for the item -- leaving
  one behind after a wheel finally succeeds would skew retry/quarantine
  bookkeeping the next time it fails.
* ``Retry`` never changes ``wheels.state``: the derived queue (state + the
  absence of a deferring ``work`` row) is what makes an item eligible again.
"""

from __future__ import annotations

import enum
import json
import random
import sqlite3
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import zstandard

from .convert import ConvertOk, ConvertOutcome, ConvertRetry, ConvertSkip
from .ratelimit import HierarchicalLimiter
from .schema import ALLOWED_TRANSITIONS, WheelState
from .writer import WriteOp, Writer, read_txn

ZSTD_LEVEL = 10
"""Shared zstd compression level for every stage payload this module stores."""

BASE_BACKOFF_SECONDS = 30.0
MAX_BACKOFF_SECONDS = 6 * 3600.0
DEFAULT_MAX_ATTEMPTS = 8
_JITTER_LOW = 0.5
_JITTER_HIGH = 1.5


class Stage(enum.StrEnum):
    """A pipeline stage with a derived queue, matching ``work.stage`` values."""

    FETCH = "fetch"
    CONVERT = "convert"


_QUEUE_STATE: Mapping[Stage, WheelState] = {
    Stage.FETCH: WheelState.NEED_METADATA,
    Stage.CONVERT: WheelState.NEED_CONVERT,
}


class IllegalTransitionError(RuntimeError):
    """Raised when an outcome would move a wheel along an edge ``ALLOWED_TRANSITIONS`` forbids."""

    def __init__(self, current: WheelState, next_state: WheelState) -> None:
        self.current = current
        self.next_state = next_state
        super().__init__(f"illegal transition: {current.name} -> {next_state.name}")


@dataclass(frozen=True)
class QueueItem:
    """One claimable row from a stage's derived queue."""

    id: int
    project: str
    lane: int
    state: WheelState


StagePayloadWriter = Callable[[sqlite3.Connection, int], None]
"""Writes a successful outcome's stage-specific columns for ``wheel_id``.

Runs inside the same ``WriteOp`` as the generic ``state``/``change_seq``
update and ``work`` row deletion, so it must not touch either of those.
"""


@dataclass(frozen=True)
class Ok:
    """A stage succeeded: advance to ``next_state`` and persist its payload."""

    next_state: WheelState
    write: StagePayloadWriter


@dataclass(frozen=True)
class Skip:
    """A wheel-attributable failure: this wheel will never succeed as-is."""

    reason: str
    subcategory: str
    details: str
    permanent: bool
    reroll_version: str | None


@dataclass(frozen=True)
class Retry:
    """A failure that says nothing about the wheel: try again later."""

    reason: str
    details: str


@dataclass(frozen=True)
class RateLimited:
    """The stage was throttled; not an attempt against the wheel at all."""

    child: str
    seconds: float


Outcome = Ok | Skip | Retry | RateLimited
"""The generic three-shape (plus ``RateLimited``) outcome union every stage adapts into.

``ConvertOutcome`` (``convert.py``) is one stage's own outcome type;
:func:`adapt_convert_outcome` maps it onto this union. A later stage (e.g.
spec 09's fetch stage) defines its own outcome type and its own adapter
function without this module changing at all.
"""


def adapt_convert_outcome(outcome: ConvertOutcome, *, reroll_version: str) -> Outcome:
    """Map a ``convert.py`` outcome onto the generic :data:`Outcome` union.

    ``reroll_version`` tags the ``wheel_repodata`` row a ``ConvertOk`` writes
    (``ConvertOk`` itself carries no version -- ``convert()`` is a pure
    function with no notion of "this database's current version").
    """
    if isinstance(outcome, ConvertOk):
        return Ok(next_state=WheelState.READY, write=_convert_ok_writer(outcome, reroll_version))
    if isinstance(outcome, ConvertSkip):
        return Skip(
            reason=outcome.reason,
            subcategory=outcome.subcategory,
            details=outcome.details,
            permanent=outcome.permanent,
            reroll_version=outcome.reroll_version,
        )
    if isinstance(outcome, ConvertRetry):
        return Retry(reason=outcome.reason, details=outcome.details)
    raise TypeError(f"unsupported convert outcome: {outcome!r}")


def compress_json(obj: Any) -> bytes:
    """Serialize ``obj`` to JSON, then zstd-compress it at :data:`ZSTD_LEVEL`."""
    payload = json.dumps(obj).encode("utf-8")
    return zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(payload)


def decompress_json(blob: bytes) -> Any:
    """Invert :func:`compress_json`."""
    payload = zstandard.ZstdDecompressor().decompress(blob)
    return json.loads(payload.decode("utf-8"))


def compute_backoff(
    attempts: int,
    *,
    base: float = BASE_BACKOFF_SECONDS,
    cap: float = MAX_BACKOFF_SECONDS,
    rng: random.Random,
) -> float:
    """Return the jittered backoff delay, in seconds, for the ``attempts``-th failure.

    ``30 * 2 ** (attempts - 1)``, capped at ``cap``, times a jitter factor
    drawn uniformly from ``[0.5, 1.5)``. The jitter is what keeps a batch of
    items that failed together from retrying together forever.
    """
    delay = min(base * 2 ** (attempts - 1), cap)
    return delay * rng.uniform(_JITTER_LOW, _JITTER_HIGH)


@dataclass(frozen=True)
class RerollVersionBelow:
    """Reprocess wheels whose stored ``reroll_version`` predates ``version``."""

    version: str


@dataclass(frozen=True)
class StateSelector:
    """Reprocess every wheel currently in ``state``."""

    state: WheelState


@dataclass(frozen=True)
class ProjectSelector:
    """Reprocess every wheel belonging to ``project``."""

    project: str


@dataclass(frozen=True)
class SkippedOnly:
    """Reprocess every currently-``SKIPPED`` wheel."""


Selector = RerollVersionBelow | StateSelector | ProjectSelector | SkippedOnly


@dataclass(frozen=True)
class StageMetrics:
    """A point-in-time snapshot of one stage's queue and outcome counters."""

    queue_depth: int
    in_flight: int
    oldest_pending_age: float | None
    throughput_ema: float
    outcome_counts: Mapping[str, int]
    retry_count: int
    quarantine_count: int


@dataclass(frozen=True)
class SelectorPreview:
    """A read-only count of what ``Dispatcher.reprocess(selector)`` would affect.

    ``skips_to_clear_count`` is nonzero only for :class:`RerollVersionBelow`:
    it is the only selector whose reprocess campaign deletes ``skips`` rows
    (see ``_reprocess_chunk_op``).
    """

    wheel_count: int
    skips_to_clear_count: int


def preview_selector(
    conn: sqlite3.Connection, selector: Selector, *, read_budget: float = 0.25
) -> SelectorPreview:
    """Count how many wheels ``Dispatcher.reprocess(selector)`` would touch, without writing.

    ``conn`` is a read-only connection. Mirrors each selector's own chunk
    query from ``reprocess()`` (never a table scan), just without the
    ``LIMIT``/keyset pagination, so this always reflects exactly what a real
    campaign would match.
    """
    if isinstance(selector, ProjectSelector):
        sql, params = _project_count_query(selector.project)
        return SelectorPreview(
            wheel_count=_run_count(conn, sql, params, budget=read_budget),
            skips_to_clear_count=0,
        )
    if isinstance(selector, StateSelector):
        sql, params = _state_count_query(selector.state)
        return SelectorPreview(
            wheel_count=_run_count(conn, sql, params, budget=read_budget),
            skips_to_clear_count=0,
        )
    if isinstance(selector, SkippedOnly):
        sql, params = _state_count_query(WheelState.SKIPPED)
        return SelectorPreview(
            wheel_count=_run_count(conn, sql, params, budget=read_budget),
            skips_to_clear_count=0,
        )
    if isinstance(selector, RerollVersionBelow):
        return _preview_reroll_version_below(conn, selector.version, read_budget=read_budget)
    raise TypeError(f"unsupported selector: {selector!r}")


class Dispatcher:
    """Owns queue selection, outcome application, backoff, and reprocess campaigns.

    ``conn`` is a read-only connection (e.g. from ``db.connect_reader``) used
    for every queue/metrics query; ``writer`` is where every mutation is
    submitted as a :class:`WriteOp`. This module itself never calls
    ``commit()``/``rollback()``/``BEGIN``.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        writer: Writer,
        *,
        reroll_version: str,
        limiter: HierarchicalLimiter | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        now: Callable[[], float] = time.time,
        rng: random.Random | None = None,
        read_budget: float = 0.25,
        ema_alpha: float = 0.2,
    ) -> None:
        self._conn = conn
        self._writer = writer
        self._reroll_version = reroll_version
        self._limiter = limiter
        self._max_attempts = max_attempts
        self._now = now
        self._rng = rng if rng is not None else random.Random()
        self._read_budget = read_budget
        self._ema_alpha = ema_alpha

        self._in_flight: dict[Stage, set[int]] = {stage: set() for stage in Stage}
        self._cursors: dict[Stage, tuple[int, str, int] | None] = dict.fromkeys(Stage)
        self._outcome_counts: dict[Stage, dict[str, int]] = {stage: {} for stage in Stage}
        self._retry_count: dict[Stage, int] = dict.fromkeys(Stage, 0)
        self._quarantine_count: dict[Stage, int] = dict.fromkeys(Stage, 0)
        self._last_completion: dict[Stage, float] = {}
        self._throughput_ema: dict[Stage, float] = dict.fromkeys(Stage, 0.0)

    def claim(self, stage: Stage, limit: int) -> list[QueueItem]:
        """Return up to ``limit`` claimable items for ``stage``, advancing its cursor.

        A single indexed query per call: filters to the stage's queue state,
        excludes in-flight and deferred (future ``next_attempt_at`` or
        quarantined) items, and orders by ``(lane, project, id)`` starting
        just past this stage's keyset cursor. A short page (fewer than
        ``limit`` rows) resets the cursor to the start for the next call.
        """
        predicate_state = _QUEUE_STATE[stage]
        cursor = self._cursors[stage]
        in_flight = self._in_flight[stage]
        sql, params = _claim_query(
            predicate_state, stage, cursor, in_flight, self._iso_now(), limit
        )
        with read_txn(
            self._conn, budget=self._read_budget, label=f"dispatcher.claim.{stage.value}"
        ):
            rows = self._conn.execute(sql, params).fetchall()

        items = [
            QueueItem(id=row[0], lane=row[1], project=row[2], state=predicate_state) for row in rows
        ]
        if len(items) < limit:
            self._cursors[stage] = None
        else:
            last = items[-1]
            self._cursors[stage] = (last.lane, last.project, last.id)
        in_flight.update(item.id for item in items)
        return items

    def release(self, stage: Stage, wheel_id: int) -> None:
        """Remove ``wheel_id`` from ``stage``'s in-flight set without applying an outcome."""
        self._in_flight[stage].discard(wheel_id)

    def apply_outcome(self, stage: Stage, item: QueueItem, outcome: Outcome) -> Any:
        """Turn ``outcome`` into the appropriate write(s), then release ``item``.

        Returns the applied ``WriteOp``'s result (``None`` for ``RateLimited``,
        which performs no database write at all).
        """
        try:
            if isinstance(outcome, RateLimited):
                result = self._apply_rate_limited(outcome)
            else:
                op = self._build_write_op(stage, item, outcome)
                result = self._writer.submit_and_wait(op)
            self._record_outcome(stage, outcome, result)
            return result
        finally:
            self.release(stage, item.id)

    def reprocess(self, selector: Selector, *, chunk_size: int = 5000) -> int:
        """Reset every wheel matching ``selector`` back to ``NEED_CONVERT``/lane 1.

        Chunked into one ``WriteOp`` per up-to-``chunk_size`` matching wheels
        (never one transaction for the whole campaign). A ``DELETED`` wheel
        never matches any selector -- it is a permanent tombstone. Returns
        the total number of ``wheels`` rows updated.
        """
        total = 0
        for chunk_ids in self._selector_chunks(selector, chunk_size):
            op = _reprocess_chunk_op(chunk_ids, selector, self._writer.next_seq, self._iso_now)
            total += self._writer.submit_and_wait(op)
        return total

    def _selector_chunks(self, selector: Selector, chunk_size: int) -> Iterator[list[int]]:
        """Dispatch ``selector`` to its own index-driven chunk generator.

        Every generator excludes ``state = DELETED``: a tombstoned wheel
        must never be dragged back into the live queue by any selector.
        """
        if isinstance(selector, RerollVersionBelow):
            yield from self._reroll_version_below_chunks(selector.version, chunk_size)
        elif isinstance(selector, ProjectSelector):
            yield from self._project_chunks(selector.project, chunk_size)
        elif isinstance(selector, StateSelector):
            yield from self._state_chunks(selector.state, chunk_size)
        elif isinstance(selector, SkippedOnly):
            yield from self._state_chunks(WheelState.SKIPPED, chunk_size)
        else:
            raise TypeError(f"unsupported selector: {selector!r}")

    def _project_chunks(self, project: str, chunk_size: int) -> Iterator[list[int]]:
        last_id = 0
        while True:
            sql, params = _project_chunk_query(project, last_id, chunk_size)
            with read_txn(
                self._conn, budget=self._read_budget, label="dispatcher.reprocess.select"
            ):
                rows = self._conn.execute(sql, params).fetchall()
            if not rows:
                return
            ids = [row[0] for row in rows]
            yield ids
            last_id = ids[-1]
            if len(rows) < chunk_size:
                return

    def _state_chunks(self, state: WheelState, chunk_size: int) -> Iterator[list[int]]:
        cursor: tuple[int, str, int] | None = None
        while True:
            sql, params = _state_chunk_query(state, cursor, chunk_size)
            with read_txn(
                self._conn, budget=self._read_budget, label="dispatcher.reprocess.select"
            ):
                rows = self._conn.execute(sql, params).fetchall()
            if not rows:
                return
            yield [row[0] for row in rows]
            last_id, last_lane, last_project = rows[-1]
            cursor = (last_lane, last_project, last_id)
            if len(rows) < chunk_size:
                return

    def _reroll_version_below_chunks(self, version: str, chunk_size: int) -> Iterator[list[int]]:
        """Reprocess ids from ``wheel_repodata``, then from ``skips``, as two phases.

        Neither phase needs a keyset cursor: :func:`_reprocess_chunk_op`
        deletes every row it matches from the source table, so re-running
        the *same* bounded query after each committed chunk always returns
        the next remaining batch.
        """
        while True:
            sql, params = _reroll_version_wheel_repodata_chunk_query(version, chunk_size)
            with read_txn(
                self._conn, budget=self._read_budget, label="dispatcher.reprocess.select"
            ):
                rows = self._conn.execute(sql, params).fetchall()
            if not rows:
                break
            yield [row[0] for row in rows]
            if len(rows) < chunk_size:
                break

        while True:
            sql, params = _reroll_version_skips_chunk_query(version, chunk_size)
            with read_txn(
                self._conn, budget=self._read_budget, label="dispatcher.reprocess.select"
            ):
                rows = self._conn.execute(sql, params).fetchall()
            if not rows:
                return
            yield list(dict.fromkeys(row[0] for row in rows))
            if len(rows) < chunk_size:
                return

    def metrics(self, stage: Stage) -> StageMetrics:
        """Return a snapshot of ``stage``'s queue depth, in-flight count, and outcome counters."""
        predicate_state = _QUEUE_STATE[stage]
        with read_txn(
            self._conn, budget=self._read_budget, label=f"dispatcher.metrics.{stage.value}"
        ):
            (queue_depth,) = self._conn.execute(
                "SELECT COUNT(*) FROM wheels WHERE state = ?", (int(predicate_state),)
            ).fetchone()
            (oldest_updated_at,) = self._conn.execute(
                "SELECT MIN(updated_at) FROM wheels WHERE state = ?", (int(predicate_state),)
            ).fetchone()
        oldest_pending_age = (
            None if oldest_updated_at is None else self._now() - _parse_iso(oldest_updated_at)
        )
        return StageMetrics(
            queue_depth=queue_depth,
            in_flight=len(self._in_flight[stage]),
            oldest_pending_age=oldest_pending_age,
            throughput_ema=self._throughput_ema[stage],
            outcome_counts=dict(self._outcome_counts[stage]),
            retry_count=self._retry_count[stage],
            quarantine_count=self._quarantine_count[stage],
        )

    def _apply_rate_limited(self, outcome: RateLimited) -> None:
        if self._limiter is not None:
            self._limiter.penalize(outcome.child, outcome.seconds)
        return None

    def _build_write_op(self, stage: Stage, item: QueueItem, outcome: Ok | Skip | Retry) -> WriteOp:
        if isinstance(outcome, Ok):
            return self._ok_op(stage, item, outcome)
        if isinstance(outcome, Skip):
            return self._skip_op(stage, item, outcome)
        return self._retry_op(stage, item, outcome)

    def _ok_op(self, stage: Stage, item: QueueItem, outcome: Ok) -> WriteOp:
        _validate_transition(item.state, outcome.next_state)
        now_iso = self._iso_now()
        next_state = outcome.next_state
        write = outcome.write
        wheel_id = item.id
        stage_value = stage.value
        writer = self._writer

        def _apply(conn: sqlite3.Connection) -> None:
            seq = writer.next_seq()
            conn.execute(
                "UPDATE wheels SET state = ?, change_seq = ?, updated_at = ? WHERE id = ?",
                (int(next_state), seq, now_iso, wheel_id),
            )
            write(conn, wheel_id)
            conn.execute(
                "DELETE FROM work WHERE wheel_id = ? AND stage = ?", (wheel_id, stage_value)
            )

        return WriteOp(name=f"{stage_value}.ok", apply=_apply)

    def _skip_op(self, stage: Stage, item: QueueItem, outcome: Skip) -> WriteOp:
        _validate_transition(item.state, WheelState.SKIPPED)
        now_iso = self._iso_now()
        reroll_version = self._reroll_version
        wheel_id = item.id
        stage_value = stage.value
        writer = self._writer

        def _apply(conn: sqlite3.Connection) -> None:
            seq = writer.next_seq()
            conn.execute(
                "UPDATE wheels SET state = ?, change_seq = ?, updated_at = ? WHERE id = ?",
                (int(WheelState.SKIPPED), seq, now_iso, wheel_id),
            )
            conn.execute(
                "INSERT INTO skips "
                "(wheel_id, stage, reason, permanent, reroll_version, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(wheel_id, stage) DO UPDATE SET "
                "reason = excluded.reason, permanent = excluded.permanent, "
                "reroll_version = excluded.reroll_version, created_at = excluded.created_at",
                (
                    wheel_id,
                    stage_value,
                    outcome.reason,
                    int(outcome.permanent),
                    None if outcome.permanent else outcome.reroll_version,
                    now_iso,
                ),
            )
            conn.execute(
                "INSERT INTO errors "
                "(wheel_id, error_category, error_subcat, details, reroll_version, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    wheel_id,
                    outcome.reason,
                    outcome.subcategory,
                    outcome.details,
                    reroll_version,
                    now_iso,
                ),
            )
            conn.execute(
                "DELETE FROM work WHERE wheel_id = ? AND stage = ?", (wheel_id, stage_value)
            )

        return WriteOp(name=f"{stage_value}.skip", apply=_apply)

    def _retry_op(self, stage: Stage, item: QueueItem, outcome: Retry) -> WriteOp:
        now_value = self._now()
        now_iso = _iso(now_value)
        max_attempts = self._max_attempts
        reroll_version = self._reroll_version
        rng = self._rng
        current_state = item.state
        wheel_id = item.id
        stage_value = stage.value
        details = outcome.details
        reason = outcome.reason
        writer = self._writer

        def _apply(conn: sqlite3.Connection) -> tuple[str, int]:
            row = conn.execute(
                "SELECT attempts FROM work WHERE wheel_id = ? AND stage = ?",
                (wheel_id, stage_value),
            ).fetchone()
            attempts = (row[0] if row is not None else 0) + 1

            if attempts > max_attempts:
                _validate_transition(current_state, WheelState.QUARANTINED)
                seq = writer.next_seq()
                conn.execute(
                    "UPDATE wheels SET state = ?, change_seq = ?, updated_at = ? WHERE id = ?",
                    (int(WheelState.QUARANTINED), seq, now_iso, wheel_id),
                )
                conn.execute(
                    "INSERT INTO work "
                    "(wheel_id, stage, attempts, next_attempt_at, last_error, quarantined_at) "
                    "VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(wheel_id, stage) DO UPDATE SET "
                    "attempts = excluded.attempts, next_attempt_at = excluded.next_attempt_at, "
                    "last_error = excluded.last_error, quarantined_at = excluded.quarantined_at",
                    (wheel_id, stage_value, attempts, now_iso, details, now_iso),
                )
                conn.execute(
                    "INSERT INTO errors "
                    "(wheel_id, error_category, error_subcat, details, reroll_version, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (wheel_id, reason, None, details, reroll_version, now_iso),
                )
                return ("quarantined", attempts)

            delay = compute_backoff(attempts, rng=rng)
            next_attempt_at = _iso(now_value + delay)
            conn.execute(
                "INSERT INTO work (wheel_id, stage, attempts, next_attempt_at, last_error) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(wheel_id, stage) DO UPDATE SET "
                "attempts = excluded.attempts, next_attempt_at = excluded.next_attempt_at, "
                "last_error = excluded.last_error",
                (wheel_id, stage_value, attempts, next_attempt_at, details),
            )
            return ("retried", attempts)

        return WriteOp(name=f"{stage_value}.retry", apply=_apply)

    def _record_outcome(self, stage: Stage, outcome: Outcome, result: Any) -> None:
        counts = self._outcome_counts[stage]
        if isinstance(outcome, Ok):
            counts["ok"] = counts.get("ok", 0) + 1
        elif isinstance(outcome, Skip):
            counts["skip"] = counts.get("skip", 0) + 1
        elif isinstance(outcome, Retry):
            counts["retry"] = counts.get("retry", 0) + 1
            kind, _attempts = result
            if kind == "quarantined":
                self._quarantine_count[stage] += 1
            else:
                self._retry_count[stage] += 1
        else:
            counts["rate_limited"] = counts.get("rate_limited", 0) + 1
        self._record_throughput(stage)

    def _record_throughput(self, stage: Stage) -> None:
        now = self._now()
        last = self._last_completion.get(stage)
        self._last_completion[stage] = now
        if last is None:
            return
        elapsed = now - last
        if elapsed <= 0:
            return
        instantaneous = 1.0 / elapsed
        previous = self._throughput_ema[stage]
        self._throughput_ema[stage] = (
            self._ema_alpha * instantaneous + (1 - self._ema_alpha) * previous
        )

    def _iso_now(self) -> str:
        return _iso(self._now())


def _validate_transition(current: WheelState, next_state: WheelState) -> None:
    if next_state not in ALLOWED_TRANSITIONS[current]:
        raise IllegalTransitionError(current, next_state)


def _convert_ok_writer(outcome: ConvertOk, reroll_version: str) -> StagePayloadWriter:
    repodata_zst = compress_json([record.model_dump(mode="json") for record in outcome.records])
    name_conv_zst = compress_json(
        [resolution.model_dump(mode="json") for resolution in outcome.resolutions]
    )
    requires_prerelease = int(outcome.requires_prerelease)
    conda_name = outcome.conda_name

    def _write(conn: sqlite3.Connection, wheel_id: int) -> None:
        conn.execute(
            "INSERT INTO wheel_repodata "
            "(wheel_id, repodata_zst, name_conv_zst, requires_prerelease, reroll_version) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(wheel_id) DO UPDATE SET "
            "repodata_zst = excluded.repodata_zst, name_conv_zst = excluded.name_conv_zst, "
            "requires_prerelease = excluded.requires_prerelease, "
            "reroll_version = excluded.reroll_version",
            (wheel_id, repodata_zst, name_conv_zst, requires_prerelease, reroll_version),
        )
        conn.execute("UPDATE wheels SET conda_name = ? WHERE id = ?", (conda_name, wheel_id))

    return _write


def _lane_project_id_keyset(
    cursor: tuple[int, str, int] | None, *, alias: str = ""
) -> tuple[str | None, list[Any]]:
    """The ``(lane, project, id)`` OR-chain keyset predicate shared by ``claim()``
    and reprocess()'s state-based selectors, both seeking ``ix_wheels_queue``.

    Returns ``(None, [])`` for ``cursor is None`` (first page, no lower bound).
    """
    if cursor is None:
        return None, []
    prefix = f"{alias}." if alias else ""
    last_lane, last_project, last_id = cursor
    condition = (
        f"({prefix}lane > ? OR ({prefix}lane = ? AND {prefix}project > ?) "
        f"OR ({prefix}lane = ? AND {prefix}project = ? AND {prefix}id > ?))"
    )
    params = [last_lane, last_lane, last_project, last_lane, last_project, last_id]
    return condition, params


def _claim_query(
    predicate_state: WheelState,
    stage: Stage,
    cursor: tuple[int, str, int] | None,
    in_flight: set[int],
    now_iso: str,
    limit: int,
) -> tuple[str, list[Any]]:
    conditions = ["w.state = ?"]
    params: list[Any] = [int(predicate_state)]

    keyset_sql, keyset_params = _lane_project_id_keyset(cursor, alias="w")
    if keyset_sql is not None:
        conditions.append(keyset_sql)
        params.extend(keyset_params)

    conditions.append(
        "NOT EXISTS ("
        "SELECT 1 FROM work wk WHERE wk.wheel_id = w.id AND wk.stage = ? "
        "AND (wk.quarantined_at IS NOT NULL OR wk.next_attempt_at > ?))"
    )
    params.extend([stage.value, now_iso])

    if in_flight:
        placeholders = ", ".join("?" for _ in in_flight)
        conditions.append(f"w.id NOT IN ({placeholders})")
        params.extend(sorted(in_flight))

    where_sql = " AND ".join(conditions)
    sql = (
        f"SELECT w.id, w.lane, w.project FROM wheels w WHERE {where_sql} "
        "ORDER BY w.lane, w.project, w.id LIMIT ?"
    )
    params.append(limit)
    return sql, params


def _project_chunk_query(project: str, last_id: int, limit: int) -> tuple[str, list[Any]]:
    """A plain ``id`` keyset page over ``ix_wheels_project``, excluding ``DELETED``."""
    sql = "SELECT id FROM wheels WHERE project = ? AND state != ? AND id > ? ORDER BY id LIMIT ?"
    return sql, [project, int(WheelState.DELETED), last_id, limit]


def _state_chunk_query(
    state: WheelState, cursor: tuple[int, str, int] | None, limit: int
) -> tuple[str, list[Any]]:
    """A ``(lane, project, id)`` keyset page over ``ix_wheels_queue``.

    The redundant ``state != DELETED`` condition makes ``StateSelector(state=DELETED)``
    match nothing rather than needing a special case.
    """
    conditions = ["state = ?", "state != ?"]
    params: list[Any] = [int(state), int(WheelState.DELETED)]
    keyset_sql, keyset_params = _lane_project_id_keyset(cursor)
    if keyset_sql is not None:
        conditions.append(keyset_sql)
        params.extend(keyset_params)
    where_sql = " AND ".join(conditions)
    sql = (
        f"SELECT id, lane, project FROM wheels WHERE {where_sql} ORDER BY lane, project, id LIMIT ?"
    )
    params.append(limit)
    return sql, params


def _reroll_version_wheel_repodata_chunk_query(version: str, limit: int) -> tuple[str, list[Any]]:
    """A ``reroll_version`` seek over ``ix_wheel_repodata_version``, excluding ``DELETED``.

    No keyset cursor: each matched row's ``wheel_repodata`` entry is deleted
    by :func:`_reprocess_chunk_op` before the next page is fetched, so
    re-running this same bounded query always returns the next batch.
    """
    sql = (
        "SELECT wr.wheel_id FROM wheel_repodata wr JOIN wheels w ON w.id = wr.wheel_id "
        "WHERE wr.reroll_version < ? AND w.state != ? ORDER BY wr.reroll_version LIMIT ?"
    )
    return sql, [version, int(WheelState.DELETED), limit]


def _reroll_version_skips_chunk_query(version: str, limit: int) -> tuple[str, list[Any]]:
    """A ``reroll_version`` seek over ``ix_skips_retryable``, excluding ``DELETED``.

    No keyset cursor, for the same reason as
    :func:`_reroll_version_wheel_repodata_chunk_query`: a matched, non-permanent
    ``skips`` row below ``version`` is deleted by :func:`_reprocess_chunk_op`.
    """
    sql = (
        "SELECT sk.wheel_id FROM skips sk JOIN wheels w ON w.id = sk.wheel_id "
        "WHERE sk.permanent = 0 AND sk.reroll_version < ? AND w.state != ? "
        "ORDER BY sk.reroll_version LIMIT ?"
    )
    return sql, [version, int(WheelState.DELETED), limit]


def _project_count_query(project: str) -> tuple[str, list[Any]]:
    """The unpaginated count behind :func:`_project_chunks`'s ``ix_wheels_project`` seek."""
    sql = "SELECT COUNT(*) FROM wheels WHERE project = ? AND state != ?"
    return sql, [project, int(WheelState.DELETED)]


def _state_count_query(state: WheelState) -> tuple[str, list[Any]]:
    """The unpaginated count behind :func:`_state_chunks`'s ``ix_wheels_queue`` seek."""
    sql = "SELECT COUNT(*) FROM wheels WHERE state = ? AND state != ?"
    return sql, [int(state), int(WheelState.DELETED)]


def _reroll_version_wheel_count_query(version: str) -> tuple[str, list[Any]]:
    """Distinct-wheel count of everything :func:`_reroll_version_below_chunks` touches.

    The union of its ``wheel_repodata``-side and ``skips``-side matches,
    deduplicated by ``wheel_id``: :func:`_reprocess_chunk_op` clears both a
    matching ``wheel_repodata`` row and a matching stale ``skips`` row for
    the same wheel in one ``_apply`` call, so a wheel matching both sides
    is touched by the real campaign exactly once, not twice.
    """
    sql = (
        "SELECT COUNT(*) FROM ("
        "SELECT wr.wheel_id AS wheel_id FROM wheel_repodata wr "
        "JOIN wheels w ON w.id = wr.wheel_id WHERE wr.reroll_version < ? AND w.state != ? "
        "UNION "
        "SELECT sk.wheel_id AS wheel_id FROM skips sk JOIN wheels w ON w.id = sk.wheel_id "
        "WHERE sk.permanent = 0 AND sk.reroll_version < ? AND w.state != ?"
        ")"
    )
    return sql, [version, int(WheelState.DELETED), version, int(WheelState.DELETED)]


def _reroll_version_skips_row_count_query(version: str) -> tuple[str, list[Any]]:
    """Row count of exactly the ``skips`` rows :func:`_reprocess_chunk_op` deletes
    for a :class:`RerollVersionBelow` campaign."""
    sql = (
        "SELECT COUNT(*) FROM skips sk JOIN wheels w ON w.id = sk.wheel_id "
        "WHERE sk.permanent = 0 AND sk.reroll_version < ? AND w.state != ?"
    )
    return sql, [version, int(WheelState.DELETED)]


def _preview_reroll_version_below(
    conn: sqlite3.Connection, version: str, *, read_budget: float
) -> SelectorPreview:
    wheel_sql, wheel_params = _reroll_version_wheel_count_query(version)
    skips_row_sql, skips_row_params = _reroll_version_skips_row_count_query(version)
    with read_txn(conn, budget=read_budget, label="dispatcher.preview_selector"):
        (wheel_count,) = conn.execute(wheel_sql, wheel_params).fetchone()
        (skips_row_count,) = conn.execute(skips_row_sql, skips_row_params).fetchone()
    return SelectorPreview(
        wheel_count=wheel_count,
        skips_to_clear_count=skips_row_count,
    )


def _run_count(conn: sqlite3.Connection, sql: str, params: list[Any], *, budget: float) -> int:
    with read_txn(conn, budget=budget, label="dispatcher.preview_selector"):
        (count,) = conn.execute(sql, params).fetchone()
    return count


def _reprocess_chunk_op(
    chunk_ids: list[int],
    selector: Selector,
    next_seq: Callable[[], int],
    iso_now: Callable[[], str],
) -> WriteOp:
    placeholders = ", ".join("?" for _ in chunk_ids)

    def _apply(conn: sqlite3.Connection) -> int:
        conn.execute(f"DELETE FROM wheel_repodata WHERE wheel_id IN ({placeholders})", chunk_ids)
        if isinstance(selector, RerollVersionBelow):
            conn.execute(
                f"DELETE FROM skips WHERE permanent = 0 AND reroll_version < ? "
                f"AND wheel_id IN ({placeholders})",
                (selector.version, *chunk_ids),
            )
        conn.execute(f"DELETE FROM work WHERE wheel_id IN ({placeholders})", chunk_ids)
        now_iso = iso_now()
        affected = 0
        for wheel_id in chunk_ids:
            cursor = conn.execute(
                "UPDATE wheels SET state = ?, lane = 1, change_seq = ?, updated_at = ? "
                "WHERE id = ?",
                (int(WheelState.NEED_CONVERT), next_seq(), now_iso, wheel_id),
            )
            affected += cursor.rowcount
        return affected

    return WriteOp(name="reprocess_chunk", apply=_apply)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


def _parse_iso(text: str) -> float:
    return datetime.fromisoformat(text).timestamp()
