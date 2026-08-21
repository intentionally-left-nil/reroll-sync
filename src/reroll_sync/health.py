"""A single point-in-time health snapshot: :func:`snapshot` and :func:`alarms`.

Replaces ``stats.py``. Every count derived from ``wheels`` goes through an
index (see the private ``_*_query`` functions below, each with a dedicated
``EXPLAIN QUERY PLAN`` test); every multi-row read is wrapped in
:func:`~reroll_sync.writer.read_txn` so a slow health check cannot itself
become the leaked-reader problem it exists to detect.

``index_lag``/``pipeline_backlog`` are deliberately cheap approximations of
"how far behind is this system", not an expensive exact computation across
every project's wheels -- see ``specs/11-health-and-fsck.md``'s "headline
metric" section for why.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .archive.store import ArchiveStore
from .daemon.circuit_breaker import CircuitBreaker
from .daemon.stage_loop import StageLoopStats
from .dispatcher import StageMetrics
from .ratelimit import ChildLimiterSnapshot, HierarchicalLimiter
from .schema import WheelState
from .writer import ReadTxnWatchdog, Writer, read_txn

DEFAULT_READ_BUDGET = 0.25
ERROR_WINDOW_1H_SECONDS = 3600.0
ERROR_WINDOW_24H_SECONDS = 86400.0

TERMINAL_STATES: tuple[WheelState, ...] = (
    WheelState.READY,
    WheelState.SKIPPED,
    WheelState.NO_METADATA,
)
"""A wheel in one of these (or ``DELETED``) needs no further processing."""

PENDING_STATES: tuple[WheelState, ...] = (
    WheelState.NEED_METADATA,
    WheelState.NEED_CONVERT,
    WheelState.QUARANTINED,
)
"""Non-terminal, non-tombstoned states: what ``pipeline_backlog`` counts."""


@dataclass(frozen=True)
class StageQueue:
    """One stage's derived-queue metrics, from :meth:`Dispatcher.metrics`."""

    depth: int
    depth_by_lane: Mapping[int, int]
    in_flight: int
    oldest_pending_age_seconds: float | None
    throughput_ema: float
    ok_count: int
    skip_count: int
    retry_count: int
    rate_limited_count: int


@dataclass(frozen=True)
class StageHealth:
    """One stage loop's pause/run/failure counters, from :meth:`StageLoop.stats`."""

    paused: bool
    last_run_at: float | None
    last_success_at: float | None
    consecutive_failures: int


@dataclass(frozen=True)
class DependencyHealth:
    """One circuit breaker's state, from `daemon.circuit_breaker.CircuitBreaker`."""

    state: str
    consecutive_failures: int
    next_trial_at: float | None


@dataclass(frozen=True)
class StageInput:
    """Per-stage input to :func:`snapshot`.

    ``loop`` is required for every stage. ``queue`` is supplied only for a
    stage with a derived queue (``fetch``/``convert``); every other stage
    passes ``None``. ``remote_last_serial``/``last_change_at`` are only
    ever supplied by ``index_poll`` (the one stage that ever observes a
    live simple-index poll's ``_last-serial``, via
    ``IndexPollStage.snapshot()``); every other stage passes ``None`` for
    both, since neither is persisted anywhere a plain reader connection
    could recover it.
    """

    loop: StageLoopStats
    queue: StageMetrics | None = None
    remote_last_serial: int | None = None
    last_change_at: float | None = None


@dataclass(frozen=True)
class Health:
    """A single point-in-time health snapshot. See :func:`snapshot`."""

    snapshot_at: float

    # Freshness
    index_lag: int
    remote_last_serial: int | None
    local_max_serial: int
    last_index_poll_at: float | None
    last_index_change_at: float | None
    projects_indexed: int
    projects_stale: int
    pipeline_backlog: int
    wheels_synced: int

    # Queues per stage
    queues: Mapping[str, StageQueue]

    # Wheel state census
    state_counts: Mapping[str, int]
    quarantined_count: int
    skipped_count: int
    requires_prerelease_count: int

    # Storage / sqlite
    wal_bytes: int
    seconds_since_truncate_checkpoint: float | None
    consecutive_checkpoint_failures: int
    longest_read_txn_ms: float
    read_txn_budget_violations: int
    db_bytes: int
    freelist_count: int
    writer_queue_depth: int
    writer_failed_ops: int

    # Archive
    segments_sealed: int
    segments_open: int
    open_segment_age_seconds: float | None
    open_segment_bytes: int
    unsealed_records: int
    archive_bytes: int
    disk_free_bytes: int | None

    # Rate limiting
    limiter_global_available: float
    limiter_children: Mapping[str, ChildLimiterSnapshot]

    # Stages and dependencies
    stages: Mapping[str, StageHealth]
    dependencies: Mapping[str, DependencyHealth]

    # Errors
    error_counts_1h: Mapping[str, int]
    error_counts_24h: Mapping[str, int]


def snapshot(
    reader: sqlite3.Connection,
    writer: Writer | None = None,
    limiter: HierarchicalLimiter | None = None,
    breakers: Mapping[str, CircuitBreaker] | None = None,
    stages: Mapping[str, StageInput] | None = None,
    *,
    archive_store: ArchiveStore | None = None,
    watchdog: ReadTxnWatchdog | None = None,
    now: Callable[[], float] = time.time,
    read_budget: float = DEFAULT_READ_BUDGET,
) -> Health:
    """Build a :class:`Health` snapshot.

    ``reader`` must be a read-only connection (e.g. ``db.connect_reader``).
    ``archive_store``/``watchdog`` are optional: without an
    ``archive_store``, ``open_segment_bytes``/``open_segment_age_seconds``/
    ``disk_free_bytes`` report ``0``/``None``/``None``; without a
    ``watchdog``, ``longest_read_txn_ms``/``read_txn_budget_violations``
    report ``0.0``/``0`` rather than reflecting real system-wide
    ``read_txn`` activity outside this function's own calls.

    ``writer``/``limiter``/``breakers``/``stages`` are also optional, for a
    one-shot process (e.g. the CLI's ``status`` command) that has only a
    read-only connection and none of the live daemon's in-process state.
    Without ``writer``, ``wal_bytes``/``freelist_count`` are computed
    directly from ``reader`` instead, and
    ``seconds_since_truncate_checkpoint``/``consecutive_checkpoint_failures``/
    ``writer_queue_depth``/``writer_failed_ops`` (counters that exist only
    in a live writer's memory) report ``None``/``0``/``0``/``0``. Without
    ``limiter``, rate-limiting fields report ``0.0``/no children. Without
    ``breakers``, ``dependencies`` is empty. Without ``stages``, every
    stage/queue/freshness field derived from it (``queues``, ``stages``,
    ``index_lag``, ``remote_last_serial``, ``last_index_poll_at``,
    ``last_index_change_at``) reports empty/zero/``None`` rather than
    crashing -- there is nothing to compare the database against without a
    live index-poll stage.
    """
    now_value = now()
    stages = stages if stages is not None else {}

    freshness = _read_freshness(reader, now_value=now_value, budget=read_budget, watchdog=watchdog)
    census = _read_census(reader, budget=read_budget, watchdog=watchdog)
    queues = _read_queues(
        reader, stages, now_value=now_value, budget=read_budget, watchdog=watchdog
    )
    errors = _read_errors(reader, now_value=now_value, budget=read_budget, watchdog=watchdog)
    archive = _read_archive(reader, archive_store, budget=read_budget, watchdog=watchdog)

    remote_last_serial = _first_not_none(
        stage_input.remote_last_serial for stage_input in stages.values()
    )
    last_index_change_at = _first_not_none(
        stage_input.last_change_at for stage_input in stages.values()
    )
    index_lag = (
        0 if remote_last_serial is None else max(0, remote_last_serial - freshness.local_max_serial)
    )

    checkpoint_at = None if writer is None else writer.last_truncate_checkpoint_at()
    watchdog_snapshot = watchdog.snapshot() if watchdog is not None else None
    limiter_snapshot = limiter.snapshot() if limiter is not None else None

    return Health(
        snapshot_at=now_value,
        index_lag=index_lag,
        remote_last_serial=remote_last_serial,
        local_max_serial=freshness.local_max_serial,
        last_index_poll_at=_first_not_none(
            stage_input.loop.last_run_at
            for name, stage_input in stages.items()
            if name == "index_poll"
        ),
        last_index_change_at=last_index_change_at,
        projects_indexed=freshness.projects_indexed,
        projects_stale=census.projects_stale,
        pipeline_backlog=census.pipeline_backlog,
        wheels_synced=census.wheels_synced,
        queues=queues,
        state_counts=census.state_counts,
        quarantined_count=census.state_counts.get(WheelState.QUARANTINED.name, 0),
        skipped_count=census.state_counts.get(WheelState.SKIPPED.name, 0),
        requires_prerelease_count=census.requires_prerelease_count,
        wal_bytes=_wal_bytes(reader, writer),
        seconds_since_truncate_checkpoint=(
            None if checkpoint_at is None else now_value - checkpoint_at
        ),
        consecutive_checkpoint_failures=(
            0 if writer is None else writer.consecutive_checkpoint_failures()
        ),
        longest_read_txn_ms=0.0 if watchdog_snapshot is None else watchdog_snapshot.longest_ms,
        read_txn_budget_violations=(
            0 if watchdog_snapshot is None else watchdog_snapshot.over_budget_count
        ),
        db_bytes=_db_file_size(reader),
        freelist_count=_freelist_count(reader, writer),
        writer_queue_depth=0 if writer is None else writer.queue_depth(),
        writer_failed_ops=0 if writer is None else writer.failed_ops(),
        segments_sealed=archive.segments_sealed,
        segments_open=archive.segments_open,
        open_segment_age_seconds=archive.open_segment_age_seconds,
        open_segment_bytes=archive.open_segment_bytes,
        unsealed_records=archive.unsealed_records,
        archive_bytes=archive.archive_bytes,
        disk_free_bytes=archive.disk_free_bytes,
        limiter_global_available=(
            0.0 if limiter_snapshot is None else limiter_snapshot.global_available
        ),
        limiter_children={} if limiter_snapshot is None else limiter_snapshot.children,
        stages={name: _stage_health(stage_input) for name, stage_input in stages.items()},
        dependencies={
            name: _dependency_health(breaker) for name, breaker in (breakers or {}).items()
        },
        error_counts_1h=errors.counts_1h,
        error_counts_24h=errors.counts_24h,
    )


WAL_BYTES_CRITICAL_DEFAULT = 2 * 1024**3
CHECKPOINT_FAILURES_CRITICAL_DEFAULT = 5
DISK_FREE_FLOOR_BYTES_DEFAULT = 20 * 1024**3
SEAL_SECONDS_DEFAULT = 21600.0
INDEX_LAG_STALE_SECONDS_DEFAULT = 3600.0


@dataclass(frozen=True)
class Alarm:
    """One finding from :func:`alarms`: a threshold crossed, with severity and a message."""

    severity: str
    """``"critical"`` or ``"warning"``."""
    condition: str
    """A stable, machine-readable name for the threshold that fired."""
    message: str


def alarms(
    health: Health,
    *,
    previous: Health | None = None,
    wal_bytes_critical: int = WAL_BYTES_CRITICAL_DEFAULT,
    checkpoint_failures_critical: int = CHECKPOINT_FAILURES_CRITICAL_DEFAULT,
    disk_free_floor_bytes: int = DISK_FREE_FLOOR_BYTES_DEFAULT,
    seal_seconds: float = SEAL_SECONDS_DEFAULT,
    index_lag_stale_seconds: float = INDEX_LAG_STALE_SECONDS_DEFAULT,
) -> tuple[Alarm, ...]:
    """Evaluate ``health`` against a fixed threshold table, returning every finding.

    Thresholds are parameters (defaulting to the values
    ``specs/11-health-and-fsck.md`` specifies), not hardcoded, so a caller
    with different operational limits -- or a test probing a boundary --
    can override them. ``previous`` is the prior snapshot, if any; without
    it, ``read_txn_budget_violations`` is treated as "increasing" whenever
    it is nonzero (there is nothing to compare against). Critical alarms
    sort before warnings; order is otherwise by ``condition`` name, for a
    stable, diffable report.
    """
    found: list[Alarm] = []

    if health.wal_bytes > wal_bytes_critical:
        found.append(
            Alarm(
                "critical",
                "wal_bytes",
                f"wal_bytes {health.wal_bytes} exceeds {wal_bytes_critical}",
            )
        )
    if health.consecutive_checkpoint_failures >= checkpoint_failures_critical:
        found.append(
            Alarm(
                "critical",
                "consecutive_checkpoint_failures",
                f"{health.consecutive_checkpoint_failures} consecutive TRUNCATE checkpoint "
                "failures; likely a leaked reader",
            )
        )
    if health.disk_free_bytes is not None and health.disk_free_bytes < disk_free_floor_bytes:
        found.append(
            Alarm(
                "critical",
                "disk_free_bytes",
                f"disk_free_bytes {health.disk_free_bytes} is below floor {disk_free_floor_bytes}",
            )
        )

    for name, dependency in health.dependencies.items():
        if dependency.state != "closed":
            found.append(
                Alarm("warning", "breaker_open", f"breaker {name!r} is {dependency.state}")
            )

    previous_violations = 0 if previous is None else previous.read_txn_budget_violations
    if health.read_txn_budget_violations > previous_violations:
        found.append(
            Alarm(
                "warning",
                "read_txn_budget_violations",
                f"read_txn_budget_violations increased to {health.read_txn_budget_violations}",
            )
        )

    if health.quarantined_count > 0:
        found.append(
            Alarm(
                "warning",
                "quarantined_count",
                f"{health.quarantined_count} wheel(s) quarantined",
            )
        )

    if (
        health.last_index_change_at is not None
        and health.projects_stale > 0
        and (health.snapshot_at - health.last_index_change_at) > index_lag_stale_seconds
    ):
        found.append(
            Alarm(
                "warning",
                "index_lag_stale",
                f"index unchanged for {health.snapshot_at - health.last_index_change_at:.0f}s "
                f"with {health.projects_stale} project(s) still stale",
            )
        )

    if (
        health.open_segment_age_seconds is not None
        and health.open_segment_age_seconds > 2 * seal_seconds
    ):
        found.append(
            Alarm(
                "warning",
                "open_segment_age",
                f"open_segment_age_seconds {health.open_segment_age_seconds:.0f} exceeds "
                f"2x seal_seconds ({2 * seal_seconds:.0f})",
            )
        )

    for name, stage in health.stages.items():
        if stage.consecutive_failures > 0:
            found.append(
                Alarm(
                    "warning",
                    "stage_consecutive_failures",
                    f"stage {name!r} has {stage.consecutive_failures} consecutive failure(s)",
                )
            )

    if health.writer_failed_ops > 0:
        found.append(
            Alarm(
                "warning",
                "writer_failed_ops",
                f"{health.writer_failed_ops} write op(s) have failed",
            )
        )

    severity_rank = {"critical": 0, "warning": 1}
    found.sort(key=lambda alarm: (severity_rank[alarm.severity], alarm.condition))
    return tuple(found)


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Freshness:
    local_max_serial: int
    projects_indexed: int


def _local_max_serial_query() -> str:
    return "SELECT COALESCE(MAX(serial), 0) FROM pypi_index"


def _projects_indexed_query() -> str:
    return "SELECT COUNT(*) FROM pypi_index"


def _read_freshness(
    reader: sqlite3.Connection,
    *,
    now_value: float,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> _Freshness:
    del now_value
    with read_txn(reader, budget=budget, label="health.freshness", strict=True, watchdog=watchdog):
        (local_max_serial,) = reader.execute(_local_max_serial_query()).fetchone()
        (projects_indexed,) = reader.execute(_projects_indexed_query()).fetchone()
    return _Freshness(local_max_serial=local_max_serial, projects_indexed=projects_indexed)


# ---------------------------------------------------------------------------
# Wheel state census
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Census:
    state_counts: Mapping[str, int]
    requires_prerelease_count: int
    projects_stale: int
    pipeline_backlog: int
    wheels_synced: int


def _state_census_query() -> str:
    return "SELECT state, COUNT(*) FROM wheels GROUP BY state"


def _requires_prerelease_query() -> str:
    return "SELECT COUNT(*) FROM wheel_repodata WHERE requires_prerelease = 1"


def _pending_states_placeholders() -> str:
    return ", ".join("?" for _ in PENDING_STATES)


def _projects_stale_query() -> tuple[str, list[int]]:
    placeholders = _pending_states_placeholders()
    sql = f"SELECT COUNT(DISTINCT project) FROM wheels WHERE state IN ({placeholders})"
    return sql, [int(state) for state in PENDING_STATES]


def _pipeline_backlog_query() -> tuple[str, list[int]]:
    placeholders = _pending_states_placeholders()
    sql = f"SELECT COUNT(*) FROM wheels WHERE state IN ({placeholders})"
    return sql, [int(state) for state in PENDING_STATES]


def _wheels_synced_query() -> str:
    return "SELECT COUNT(*) FROM wheels"


def _read_census(
    reader: sqlite3.Connection, *, budget: float, watchdog: ReadTxnWatchdog | None
) -> _Census:
    with read_txn(reader, budget=budget, label="health.census", strict=True, watchdog=watchdog):
        state_rows = reader.execute(_state_census_query()).fetchall()
        (requires_prerelease_count,) = reader.execute(_requires_prerelease_query()).fetchone()
        stale_sql, stale_params = _projects_stale_query()
        (projects_stale,) = reader.execute(stale_sql, stale_params).fetchone()
        backlog_sql, backlog_params = _pipeline_backlog_query()
        (pipeline_backlog,) = reader.execute(backlog_sql, backlog_params).fetchone()
        (wheels_synced,) = reader.execute(_wheels_synced_query()).fetchone()

    state_counts = {state.name: 0 for state in WheelState}
    for raw_state, count in state_rows:
        state_counts[WheelState(raw_state).name] = count

    return _Census(
        state_counts=state_counts,
        requires_prerelease_count=requires_prerelease_count,
        projects_stale=projects_stale,
        pipeline_backlog=pipeline_backlog,
        wheels_synced=wheels_synced,
    )


# ---------------------------------------------------------------------------
# Queues per stage
# ---------------------------------------------------------------------------

_QUEUE_STAGE_STATE: Mapping[str, WheelState] = {
    "fetch": WheelState.NEED_METADATA,
    "convert": WheelState.NEED_CONVERT,
}


def _queue_depth_by_lane_query() -> str:
    return "SELECT lane, COUNT(*) FROM wheels WHERE state = ? GROUP BY lane"


def _oldest_pending_query() -> str:
    return "SELECT MIN(updated_at) FROM wheels WHERE state = ?"


def _read_queues(
    reader: sqlite3.Connection,
    stages: Mapping[str, StageInput],
    *,
    now_value: float,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> dict[str, StageQueue]:
    queues: dict[str, StageQueue] = {}
    for name, stage_input in stages.items():
        state = _QUEUE_STAGE_STATE.get(name)
        if state is None or stage_input.queue is None:
            continue
        with read_txn(
            reader, budget=budget, label=f"health.queue.{name}", strict=True, watchdog=watchdog
        ):
            lane_rows = reader.execute(_queue_depth_by_lane_query(), (int(state),)).fetchall()
            (oldest_updated_at,) = reader.execute(_oldest_pending_query(), (int(state),)).fetchone()
        oldest_pending_age = (
            None if oldest_updated_at is None else now_value - _parse_iso(oldest_updated_at)
        )
        metrics = stage_input.queue
        queues[name] = StageQueue(
            depth=metrics.queue_depth,
            depth_by_lane=dict(lane_rows),
            in_flight=metrics.in_flight,
            oldest_pending_age_seconds=oldest_pending_age,
            throughput_ema=metrics.throughput_ema,
            ok_count=metrics.outcome_counts.get("ok", 0),
            skip_count=metrics.outcome_counts.get("skip", 0),
            retry_count=metrics.outcome_counts.get("retry", 0),
            rate_limited_count=metrics.outcome_counts.get("rate_limited", 0),
        )
    return queues


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Errors:
    counts_1h: Mapping[str, int]
    counts_24h: Mapping[str, int]


def _errors_by_category_query() -> str:
    return (
        "SELECT error_category, COUNT(*) FROM errors WHERE created_at >= ? GROUP BY error_category"
    )


def _read_errors(
    reader: sqlite3.Connection,
    *,
    now_value: float,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> _Errors:
    since_1h = _iso(now_value - ERROR_WINDOW_1H_SECONDS)
    since_24h = _iso(now_value - ERROR_WINDOW_24H_SECONDS)
    with read_txn(reader, budget=budget, label="health.errors", strict=True, watchdog=watchdog):
        rows_1h = reader.execute(_errors_by_category_query(), (since_1h,)).fetchall()
        rows_24h = reader.execute(_errors_by_category_query(), (since_24h,)).fetchall()
    return _Errors(counts_1h=dict(rows_1h), counts_24h=dict(rows_24h))


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Archive:
    segments_sealed: int
    segments_open: int
    open_segment_age_seconds: float | None
    open_segment_bytes: int
    unsealed_records: int
    archive_bytes: int
    disk_free_bytes: int | None


def _segments_sealed_query() -> str:
    return "SELECT COUNT(*) FROM segments WHERE sealed_at IS NOT NULL"


def _segments_open_query() -> str:
    return "SELECT COUNT(*) FROM segments WHERE sealed_at IS NULL"


def _unsealed_segment_ids_query() -> str:
    return "SELECT id FROM segments WHERE sealed_at IS NULL"


def _archive_bytes_query() -> str:
    return "SELECT COALESCE(SUM(bytes), 0) FROM segments WHERE sealed_at IS NOT NULL"


def _blobs_for_segment_query() -> str:
    return "SELECT COUNT(*) FROM blobs WHERE segment_id = ?"


def _read_archive(
    reader: sqlite3.Connection,
    archive_store: ArchiveStore | None,
    *,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> _Archive:
    with read_txn(reader, budget=budget, label="health.archive", strict=True, watchdog=watchdog):
        (segments_sealed,) = reader.execute(_segments_sealed_query()).fetchone()
        (segments_open,) = reader.execute(_segments_open_query()).fetchone()
        (archive_bytes,) = reader.execute(_archive_bytes_query()).fetchone()
        unsealed_ids = [row[0] for row in reader.execute(_unsealed_segment_ids_query())]
        unsealed_records = 0
        for segment_id in unsealed_ids:
            (count,) = reader.execute(_blobs_for_segment_query(), (segment_id,)).fetchone()
            unsealed_records += count

    open_segment_age_seconds: float | None = None
    open_segment_bytes = 0
    disk_free_bytes: int | None = None
    if archive_store is not None:
        disk_free_bytes = archive_store.disk_free_bytes()
        open_writer = archive_store.open_writer_if_any()
        if open_writer is not None:
            open_segment_age_seconds = open_writer.age_seconds()
            open_segment_bytes = open_writer.compressed_bytes()

    return _Archive(
        segments_sealed=segments_sealed,
        segments_open=segments_open,
        open_segment_age_seconds=open_segment_age_seconds,
        open_segment_bytes=open_segment_bytes,
        unsealed_records=unsealed_records,
        archive_bytes=archive_bytes,
        disk_free_bytes=disk_free_bytes,
    )


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------


def _stage_health(stage_input: StageInput) -> StageHealth:
    loop = stage_input.loop
    return StageHealth(
        paused=loop.paused,
        last_run_at=loop.last_run_at,
        last_success_at=loop.last_success_at,
        consecutive_failures=loop.consecutive_failures,
    )


def _dependency_health(breaker: CircuitBreaker) -> DependencyHealth:
    return DependencyHealth(
        state=breaker.state().value,
        consecutive_failures=breaker.consecutive_failures(),
        next_trial_at=breaker.next_trial_at(),
    )


def _wal_bytes(reader: sqlite3.Connection, writer: Writer | None) -> int:
    if writer is not None:
        return writer.wal_bytes()
    for _seq, name, filename in reader.execute("PRAGMA database_list").fetchall():
        if name == "main" and filename:
            wal_path = Path(f"{filename}-wal")
            if wal_path.exists():
                return wal_path.stat().st_size
    return 0


def _freelist_count(reader: sqlite3.Connection, writer: Writer | None) -> int:
    if writer is not None:
        return writer.freelist_count()
    (value,) = reader.execute("PRAGMA freelist_count").fetchone()
    return value


def _db_file_size(reader: sqlite3.Connection) -> int:
    rows = reader.execute("PRAGMA database_list").fetchall()
    for _seq, name, filename in rows:
        if name == "main" and filename:
            path = Path(filename)
            if path.exists():
                return path.stat().st_size
    return 0


def _first_not_none(values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


def _parse_iso(text: str) -> float:
    return datetime.fromisoformat(text).timestamp()
