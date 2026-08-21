"""Tests for `health.snapshot()`: freshness, queues, census, storage,
archive, rate limiting, stages/dependencies, and errors.
"""

from __future__ import annotations

import contextlib
import dataclasses
import sqlite3
from typing import cast

import pytest

from reroll_sync.daemon.circuit_breaker import CircuitBreaker
from reroll_sync.daemon.stage_loop import StageLoopStats
from reroll_sync.db import connect_reader, init_db
from reroll_sync.dispatcher import StageMetrics
from reroll_sync.health import (
    Health,
    StageInput,
    _archive_bytes_query,
    _blobs_for_segment_query,
    _errors_by_category_query,
    _oldest_pending_query,
    _pipeline_backlog_query,
    _projects_indexed_query,
    _projects_stale_query,
    _queue_depth_by_lane_query,
    _requires_prerelease_query,
    _segments_open_query,
    _segments_sealed_query,
    _state_census_query,
    _wheels_synced_query,
    snapshot,
)
from reroll_sync.ratelimit import HierarchicalLimiter
from reroll_sync.schema import WheelState
from reroll_sync.writer import ReadTxnBudgetExceeded, ReadTxnWatchdog, Writer

# ---------------------------------------------------------------------------
# Fixtures/helpers
# ---------------------------------------------------------------------------


class FakeClock:
    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.value = start

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "health.db")
    init_db(path)
    return path


@pytest.fixture
def reader(db_path):
    conn = connect_reader(db_path)
    yield conn
    conn.close()


@pytest.fixture
def writer(db_path):
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    w = Writer(conn, batch_size=1, batch_interval=1_000_000.0)
    w.start()
    yield w
    if not w._stopped:
        w.stop(drain=False)


def _limiter() -> HierarchicalLimiter:
    return HierarchicalLimiter(2000.0, {"pypi.org": 200.0, "files.pythonhosted.org": 1800.0})


def _breakers() -> dict[str, CircuitBreaker]:
    return {
        "pypi.org": CircuitBreaker(),
        "files.pythonhosted.org": CircuitBreaker(),
        "local_disk": CircuitBreaker(),
    }


def _loop_stats(**overrides) -> StageLoopStats:
    defaults = {
        "last_run_at": None,
        "last_success_at": None,
        "consecutive_failures": 0,
        "paused": False,
    }
    defaults.update(overrides)
    return StageLoopStats(**defaults)


def _default_stages() -> dict[str, StageInput]:
    return {
        "project_sync": StageInput(loop=_loop_stats()),
        "index_poll": StageInput(loop=_loop_stats()),
        "convert": StageInput(loop=_loop_stats(), queue=_empty_queue_metrics()),
        "fetch": StageInput(loop=_loop_stats(), queue=_empty_queue_metrics()),
        "gc": StageInput(loop=_loop_stats()),
        "disk_guard": StageInput(loop=_loop_stats()),
    }


def _empty_queue_metrics() -> StageMetrics:
    return StageMetrics(
        queue_depth=0,
        in_flight=0,
        oldest_pending_age=None,
        throughput_ema=0.0,
        outcome_counts={},
        retry_count=0,
        quarantine_count=0,
    )


def _insert_wheel(
    conn: sqlite3.Connection,
    *,
    filename: str,
    project: str = "proj",
    state: WheelState = WheelState.NEED_CONVERT,
    lane: int = 0,
    updated_at: str = "2024-01-01T00:00:00+00:00",
) -> int:
    cursor = conn.execute(
        "INSERT INTO wheels "
        "(filename, project, state, lane, url, serial, change_seq, updated_at) "
        "VALUES (?, ?, ?, ?, ?, 1, 1, ?)",
        (filename, project, int(state), lane, f"https://example.test/{filename}", updated_at),
    )
    conn.commit()
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _snapshot(reader_conn, writer_obj, **kwargs) -> Health:
    return snapshot(
        reader_conn,
        writer_obj,
        kwargs.pop("limiter", None) or _limiter(),
        kwargs.pop("breakers", None) or _breakers(),
        kwargs.pop("stages", None) or _default_stages(),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Empty database
# ---------------------------------------------------------------------------


def test_snapshot_on_empty_database_returns_zeros_not_none_or_exceptions(reader, writer):
    health = _snapshot(reader, writer)

    assert health.index_lag == 0
    assert health.local_max_serial == 0
    assert health.projects_indexed == 0
    assert health.projects_stale == 0
    assert health.pipeline_backlog == 0
    assert health.wheels_synced == 0
    assert health.quarantined_count == 0
    assert health.skipped_count == 0
    assert health.requires_prerelease_count == 0
    assert health.segments_sealed == 0
    assert health.segments_open == 0
    assert health.unsealed_records == 0
    assert health.archive_bytes == 0
    assert health.writer_queue_depth == 0
    assert health.writer_failed_ops == 0
    assert health.error_counts_1h == {}
    assert health.error_counts_24h == {}
    assert all(count == 0 for count in health.state_counts.values())


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------


def test_local_max_serial_reflects_pypi_index(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 42, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    health = _snapshot(reader, writer)
    assert health.local_max_serial == 42
    assert health.projects_indexed == 1


def test_index_lag_is_zero_when_caught_up(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 42, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    stages = _default_stages()
    stages["index_poll"] = StageInput(loop=_loop_stats(), remote_last_serial=42)
    health = _snapshot(reader, writer, stages=stages)
    assert health.index_lag == 0


def test_index_lag_is_correct_when_behind(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 42, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    stages = _default_stages()
    stages["index_poll"] = StageInput(loop=_loop_stats(), remote_last_serial=100)
    health = _snapshot(reader, writer, stages=stages)
    assert health.index_lag == 58
    assert health.remote_last_serial == 100


def test_index_lag_is_zero_without_remote_last_serial(reader, writer):
    health = _snapshot(reader, writer)
    assert health.index_lag == 0
    assert health.remote_last_serial is None


def test_last_index_poll_at_comes_from_index_poll_stage_last_run(reader, writer):
    stages = _default_stages()
    stages["index_poll"] = StageInput(loop=_loop_stats(last_run_at=123.0))
    health = _snapshot(reader, writer, stages=stages)
    assert health.last_index_poll_at == 123.0


def test_last_index_change_at_comes_from_index_poll_stage(reader, writer):
    stages = _default_stages()
    stages["index_poll"] = StageInput(loop=_loop_stats(), last_change_at=456.0)
    health = _snapshot(reader, writer, stages=stages)
    assert health.last_index_change_at == 456.0


def test_projects_stale_counts_distinct_projects_in_pending_states(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(
        conn, filename="a-1.0-py3-none-any.whl", project="proj-a", state=WheelState.NEED_METADATA
    )
    _insert_wheel(
        conn, filename="b-1.0-py3-none-any.whl", project="proj-a", state=WheelState.NEED_CONVERT
    )
    _insert_wheel(
        conn, filename="c-1.0-py3-none-any.whl", project="proj-b", state=WheelState.QUARANTINED
    )
    _insert_wheel(conn, filename="d-1.0-py3-none-any.whl", project="proj-c", state=WheelState.READY)
    conn.close()

    health = _snapshot(reader, writer)
    assert health.projects_stale == 2


def test_pipeline_backlog_counts_pending_wheels(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA)
    _insert_wheel(conn, filename="b-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT)
    _insert_wheel(conn, filename="c-1.0-py3-none-any.whl", state=WheelState.QUARANTINED)
    _insert_wheel(conn, filename="d-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.close()

    health = _snapshot(reader, writer)
    assert health.pipeline_backlog == 3


def test_wheels_synced_counts_every_wheel(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.READY)
    _insert_wheel(conn, filename="b-1.0-py3-none-any.whl", state=WheelState.DELETED)
    conn.close()

    health = _snapshot(reader, writer)
    assert health.wheels_synced == 2


# --- Query plans -----------------------------------------------------------


def test_projects_indexed_query_plan_has_no_wheels_scan(db_path, reader, writer):
    plan = reader.execute(f"EXPLAIN QUERY PLAN {_projects_indexed_query()}").fetchall()
    details = [row[-1] for row in plan]
    assert not any("wheels" in d.lower() for d in details)


def test_projects_stale_query_plan_uses_ix_wheels_queue(db_path, reader, writer):
    sql, params = _projects_stale_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


def test_pipeline_backlog_query_plan_uses_ix_wheels_queue(db_path, reader, writer):
    sql, params = _pipeline_backlog_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


def test_wheels_synced_query_plan_uses_a_covering_index_not_the_table(db_path, reader, writer):
    plan = reader.execute(f"EXPLAIN QUERY PLAN {_wheels_synced_query()}").fetchall()
    details = [row[-1] for row in plan]
    assert any("COVERING INDEX" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


# ---------------------------------------------------------------------------
# Wheel state census
# ---------------------------------------------------------------------------


def test_state_counts_reflects_every_state(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.READY)
    _insert_wheel(conn, filename="b-1.0-py3-none-any.whl", state=WheelState.READY)
    _insert_wheel(conn, filename="c-1.0-py3-none-any.whl", state=WheelState.SKIPPED)
    _insert_wheel(conn, filename="d-1.0-py3-none-any.whl", state=WheelState.QUARANTINED)
    conn.close()

    health = _snapshot(reader, writer)
    assert health.state_counts["READY"] == 2
    assert health.state_counts["SKIPPED"] == 1
    assert health.state_counts["QUARANTINED"] == 1
    assert health.state_counts["NEED_METADATA"] == 0
    assert health.quarantined_count == 1
    assert health.skipped_count == 1


def test_requires_prerelease_count_uses_partial_index(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.READY)
    other_id = _insert_wheel(conn, filename="b-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.execute(
        "INSERT INTO wheel_repodata (wheel_id, repodata_zst, requires_prerelease, reroll_version) "
        "VALUES (?, ?, 1, '1.0')",
        (wheel_id, b"x"),
    )
    conn.execute(
        "INSERT INTO wheel_repodata (wheel_id, repodata_zst, requires_prerelease, reroll_version) "
        "VALUES (?, ?, 0, '1.0')",
        (other_id, b"x"),
    )
    conn.commit()
    conn.close()

    health = _snapshot(reader, writer)
    assert health.requires_prerelease_count == 1


def test_state_census_query_plan_uses_covering_index_not_table_scan(db_path, reader, writer):
    plan = reader.execute(f"EXPLAIN QUERY PLAN {_state_census_query()}").fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


def test_requires_prerelease_query_plan_uses_partial_index(db_path, reader, writer):
    plan = reader.execute(f"EXPLAIN QUERY PLAN {_requires_prerelease_query()}").fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheel_repodata_prerelease" in d for d in details)
    assert not any("SCAN TABLE" in d for d in details)


# ---------------------------------------------------------------------------
# Queues per stage
# ---------------------------------------------------------------------------


def test_queue_depth_split_by_lane(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT, lane=0)
    _insert_wheel(conn, filename="b-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT, lane=0)
    _insert_wheel(conn, filename="c-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT, lane=1)
    conn.close()

    stages = _default_stages()
    stages["convert"] = StageInput(
        loop=_loop_stats(),
        queue=StageMetrics(
            queue_depth=3,
            in_flight=1,
            oldest_pending_age=None,
            throughput_ema=0.5,
            outcome_counts={"ok": 2, "skip": 1, "retry": 3, "rate_limited": 4},
            retry_count=3,
            quarantine_count=0,
        ),
    )
    health = _snapshot(reader, writer, stages=stages)
    convert_queue = health.queues["convert"]
    assert convert_queue.depth_by_lane == {0: 2, 1: 1}
    assert convert_queue.depth == 3
    assert convert_queue.in_flight == 1
    assert convert_queue.throughput_ema == 0.5
    assert convert_queue.ok_count == 2
    assert convert_queue.skip_count == 1
    assert convert_queue.retry_count == 3
    assert convert_queue.rate_limited_count == 4


def test_oldest_pending_age_is_none_on_empty_queue(reader, writer):
    health = _snapshot(reader, writer)
    assert health.queues["fetch"].oldest_pending_age_seconds is None
    assert health.queues["convert"].oldest_pending_age_seconds is None


def test_oldest_pending_age_reflects_the_oldest_updated_at(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(
        conn,
        filename="a-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        updated_at="2024-01-01T00:00:00+00:00",
    )
    conn.close()

    stages = _default_stages()

    from datetime import UTC, datetime

    now_value = datetime(2024, 1, 1, 1, 0, 0, tzinfo=UTC).timestamp()
    health = _snapshot(reader, writer, stages=stages, now=lambda: now_value)
    assert health.queues["convert"].oldest_pending_age_seconds == pytest.approx(3600.0)


def test_a_stage_with_no_derived_queue_is_absent_from_queues(reader, writer):
    health = _snapshot(reader, writer)
    assert "project_sync" not in health.queues
    assert "index_poll" not in health.queues
    assert "gc" not in health.queues
    assert "disk_guard" not in health.queues


def test_queue_depth_by_lane_query_plan_uses_ix_wheels_queue(db_path, reader, writer):
    sql = _queue_depth_by_lane_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", (int(WheelState.NEED_CONVERT),)).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


def test_oldest_pending_query_plan_uses_ix_wheels_queue(db_path, reader, writer):
    sql = _oldest_pending_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", (int(WheelState.NEED_CONVERT),)).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN TABLE wheels" in d for d in details)


# ---------------------------------------------------------------------------
# Storage / sqlite
# ---------------------------------------------------------------------------


def test_writer_queue_depth_reflects_unpulled_ops(db_path, reader):
    from reroll_sync.writer import WriteOp

    conn = sqlite3.connect(db_path, check_same_thread=False)
    w = Writer(conn, batch_size=1000, batch_interval=1_000_000.0)
    # Never started: nothing drains the queue.
    w.submit(WriteOp(name="noop", apply=lambda _conn: None))
    try:
        health = _snapshot(reader, w)
        assert health.writer_queue_depth == 1
    finally:
        conn.close()


def test_writer_failed_ops_is_reported(db_path, reader, writer):
    from reroll_sync.writer import WriteOp

    def _boom(_conn):
        raise RuntimeError("nope")

    with contextlib.suppress(RuntimeError):
        writer.submit_and_wait(WriteOp(name="boom", apply=_boom))

    health = _snapshot(reader, writer)
    assert health.writer_failed_ops == 1


def test_wal_bytes_freelist_and_checkpoint_fields_come_from_the_writer(reader, writer):
    health = _snapshot(reader, writer)
    assert health.wal_bytes == writer.wal_bytes()
    assert health.freelist_count == writer.freelist_count()
    assert health.consecutive_checkpoint_failures == writer.consecutive_checkpoint_failures()
    assert health.seconds_since_truncate_checkpoint is None  # no checkpoint has run yet


def test_seconds_since_truncate_checkpoint_reflects_elapsed_time(db_path, reader):
    clock = FakeClock()
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    w = Writer(
        conn, batch_size=1, batch_interval=1_000_000.0, checkpoint_interval=1.0, now=clock.now
    )
    w.start()
    try:
        w._run_checkpoint()
        clock.advance(30.0)
        health = _snapshot(reader, w, now=clock.now)
        assert health.seconds_since_truncate_checkpoint == pytest.approx(30.0)
    finally:
        w.stop(drain=False)


def test_db_bytes_reflects_the_real_file_size(db_path, reader, writer):
    from pathlib import Path

    health = _snapshot(reader, writer)
    assert health.db_bytes == Path(db_path).stat().st_size
    assert health.db_bytes > 0


def test_longest_read_txn_ms_and_violations_default_to_zero_without_a_watchdog(reader, writer):
    health = _snapshot(reader, writer)
    assert health.longest_read_txn_ms == 0.0
    assert health.read_txn_budget_violations == 0


def test_longest_read_txn_ms_and_violations_reflect_an_injected_watchdog(reader, writer):
    watchdog = ReadTxnWatchdog()
    watchdog.record(42.0, over_budget=False)
    watchdog.record(500.0, over_budget=True)

    health = _snapshot(reader, writer, watchdog=watchdog)
    assert health.longest_read_txn_ms == 500.0
    assert health.read_txn_budget_violations == 1


def test_no_read_in_snapshot_exceeds_the_watchdog_budget_in_strict_mode(db_path, reader, writer):
    # snapshot()'s own reads run with strict mode enabled: a comfortable
    # budget must complete without raising at all.
    conn = sqlite3.connect(db_path)
    for i in range(500):
        _insert_wheel(conn, filename=f"pkg-{i}-1.0-py3-none-any.whl")
    conn.close()

    _snapshot(reader, writer, read_budget=5.0)


def test_snapshot_raises_in_strict_mode_when_its_own_budget_is_exceeded(reader, writer):
    with pytest.raises(ReadTxnBudgetExceeded):
        _snapshot(reader, writer, read_budget=-1.0)


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------


def test_archive_fields_are_zero_without_an_archive_store(reader, writer):
    health = _snapshot(reader, writer)
    assert health.open_segment_age_seconds is None
    assert health.open_segment_bytes == 0
    assert health.disk_free_bytes is None


def test_segments_sealed_and_archive_bytes_from_sealed_segments(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO segments (id, sealed_at, bytes, records, footer_sha) "
        "VALUES (1, ?, 1000, 5, 'x')",
        ("2024-01-01T00:00:00+00:00",),
    )
    conn.execute(
        "INSERT INTO segments (id, sealed_at, bytes, records, footer_sha) "
        "VALUES (2, ?, 2000, 7, 'y')",
        ("2024-01-01T00:00:00+00:00",),
    )
    conn.execute("INSERT INTO segments (id) VALUES (3)")  # open, unsealed
    conn.commit()
    conn.close()

    health = _snapshot(reader, writer)
    assert health.segments_sealed == 2
    assert health.segments_open == 1
    assert health.archive_bytes == 3000


def test_unsealed_records_counts_blobs_in_the_open_segment(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    conn.execute("INSERT INTO segments (id) VALUES (1)")  # open, unsealed
    conn.execute(
        "INSERT INTO segments (id, sealed_at, bytes, records, footer_sha) "
        "VALUES (2, ?, 10, 1, 'y')",
        ("2024-01-01T00:00:00+00:00",),
    )
    conn.execute(
        "INSERT INTO blobs (sha256, segment_id, block_no, offset, length) VALUES ('a', 1, 0, 0, 5)"
    )
    conn.execute(
        "INSERT INTO blobs (sha256, segment_id, block_no, offset, length) VALUES ('b', 1, 0, 5, 5)"
    )
    conn.execute(
        "INSERT INTO blobs (sha256, segment_id, block_no, offset, length) VALUES ('c', 2, 0, 0, 5)"
    )
    conn.commit()
    conn.close()

    health = _snapshot(reader, writer)
    assert health.unsealed_records == 2


def test_open_segment_bytes_and_age_come_from_a_live_archive_store(
    tmp_path, db_path, reader, writer
):
    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        clock = {"t": 0.0}
        store = ArchiveStore(tmp_path / "segments", conn, monotonic=lambda: clock["t"])
        store.add(b"0" * 10_000_000)  # first record: buffered, no flush check yet
        store.add(b"1")  # second record: now over block_target_bytes, forces a flush
        clock["t"] = 12.0
        open_writer = store.current_writer()

        health = _snapshot(reader, writer, archive_store=store)
        assert health.open_segment_bytes == open_writer.compressed_bytes()
        assert health.open_segment_bytes > 0
        assert health.open_segment_age_seconds == pytest.approx(12.0)
    finally:
        open_writer._file.close()
        conn.close()


def test_disk_free_bytes_comes_from_a_live_archive_store(tmp_path, db_path, reader, writer):
    import shutil

    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        store = ArchiveStore(tmp_path / "segments", conn)

        health = _snapshot(reader, writer, archive_store=store)
        # Real free space can shift by a few pages between the two disk_usage
        # calls made a moment apart; assert it's a real, current-ish value
        # rather than an exact snapshot equality.
        assert isinstance(health.disk_free_bytes, int)
        assert health.disk_free_bytes == pytest.approx(
            shutil.disk_usage(tmp_path / "segments").free, rel=0.01
        )
    finally:
        conn.close()


def test_disk_free_bytes_is_none_when_the_archive_directory_is_missing(
    tmp_path, db_path, reader, writer
):
    import shutil

    from reroll_sync.archive.store import ArchiveStore

    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        store = ArchiveStore(tmp_path / "segments", conn)
        shutil.rmtree(tmp_path / "segments")

        health = _snapshot(reader, writer, archive_store=store)
        assert health.disk_free_bytes is None
    finally:
        conn.close()


def test_segments_sealed_query_plan_does_not_scan_wheels(db_path, reader, writer):
    plan = reader.execute(f"EXPLAIN QUERY PLAN {_segments_sealed_query()}").fetchall()
    details = [row[-1] for row in plan]
    assert not any("wheels" in d.lower() for d in details)


def test_segments_open_query_plan_does_not_scan_wheels(db_path, reader, writer):
    plan = reader.execute(f"EXPLAIN QUERY PLAN {_segments_open_query()}").fetchall()
    details = [row[-1] for row in plan]
    assert not any("wheels" in d.lower() for d in details)


def test_archive_bytes_query_plan_does_not_scan_wheels(db_path, reader, writer):
    plan = reader.execute(f"EXPLAIN QUERY PLAN {_archive_bytes_query()}").fetchall()
    details = [row[-1] for row in plan]
    assert not any("wheels" in d.lower() for d in details)


def test_blobs_for_segment_query_plan_uses_ix_blobs_segment(db_path, reader, writer):
    sql = _blobs_for_segment_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", (1,)).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_blobs_segment" in d for d in details)
    assert not any("SCAN TABLE blobs" in d for d in details)


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


def test_limiter_snapshot_reflects_children_and_global(reader, writer):
    limiter = _limiter()
    limiter.acquire("pypi.org")
    limiter.penalize("files.pythonhosted.org", 30.0)

    health = _snapshot(reader, writer, limiter=limiter)
    assert health.limiter_global_available > 0
    assert set(health.limiter_children) == {"pypi.org", "files.pythonhosted.org"}
    assert health.limiter_children["pypi.org"].acquired == 1
    assert health.limiter_children["files.pythonhosted.org"].penalty_deadline > 0


# ---------------------------------------------------------------------------
# Stages and dependencies
# ---------------------------------------------------------------------------


def test_stage_health_reflects_loop_stats(reader, writer):
    stages = _default_stages()
    stages["fetch"] = StageInput(
        loop=_loop_stats(paused=True, last_run_at=1.0, last_success_at=2.0, consecutive_failures=3),
        queue=_empty_queue_metrics(),
    )
    health = _snapshot(reader, writer, stages=stages)
    fetch_health = health.stages["fetch"]
    assert fetch_health.paused is True
    assert fetch_health.last_run_at == 1.0
    assert fetch_health.last_success_at == 2.0
    assert fetch_health.consecutive_failures == 3


def test_dependency_health_reflects_breaker_state(reader, writer):
    breakers = _breakers()
    for _ in range(5):
        breakers["pypi.org"].record_failure()

    health = _snapshot(reader, writer, breakers=breakers)
    dep = health.dependencies["pypi.org"]
    assert dep.state == "open"
    assert dep.consecutive_failures == 5
    assert dep.next_trial_at is not None
    assert health.dependencies["files.pythonhosted.org"].state == "closed"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def test_error_counts_split_by_window(db_path, reader, writer):
    from datetime import UTC, datetime

    now_value = datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp()
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="a-1.0-py3-none-any.whl")
    conn.execute(
        "INSERT INTO errors (wheel_id, error_category, reroll_version, created_at) "
        "VALUES (?, 'network', '1.0', ?)",
        (wheel_id, "2024-01-01T23:30:00+00:00"),  # 30 min ago: in both windows
    )
    conn.execute(
        "INSERT INTO errors (wheel_id, error_category, reroll_version, created_at) "
        "VALUES (?, 'network', '1.0', ?)",
        (wheel_id, "2024-01-01T12:00:00+00:00"),  # 12h ago: only in the 24h window
    )
    conn.execute(
        "INSERT INTO errors (wheel_id, error_category, reroll_version, created_at) "
        "VALUES (?, 'parse', '1.0', ?)",
        (wheel_id, "2023-12-30T00:00:00+00:00"),  # 3 days ago: in neither window
    )
    conn.commit()
    conn.close()

    health = _snapshot(reader, writer, now=lambda: now_value)
    assert health.error_counts_1h == {"network": 1}
    assert health.error_counts_24h == {"network": 2}


def test_errors_by_category_query_plan_uses_a_covering_index(db_path, reader, writer):
    sql = _errors_by_category_query()
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", ("2024-01-01T00:00:00+00:00",)).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_errors_cat" in d for d in details)
    assert not any("SCAN TABLE errors" in d for d in details)


def test_db_bytes_is_zero_for_an_in_memory_database():
    from reroll_sync.health import _db_file_size

    conn = sqlite3.connect(":memory:")
    try:
        assert _db_file_size(conn) == 0
    finally:
        conn.close()


def test_db_bytes_is_zero_when_the_main_file_no_longer_exists(tmp_path):
    from reroll_sync.health import _db_file_size

    db_path = tmp_path / "vanished.db"
    init_db(db_path)
    conn = sqlite3.connect(str(db_path))
    db_path.unlink()
    try:
        assert _db_file_size(conn) == 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Alarms
# ---------------------------------------------------------------------------


def _healthy_health(**overrides: object) -> Health:
    from reroll_sync.health import DependencyHealth, StageHealth

    base = Health(
        snapshot_at=1_700_000_000.0,
        index_lag=0,
        remote_last_serial=100,
        local_max_serial=100,
        last_index_poll_at=1_700_000_000.0,
        last_index_change_at=1_700_000_000.0,
        projects_indexed=10,
        projects_stale=0,
        pipeline_backlog=0,
        wheels_synced=10,
        queues={},
        state_counts={state.name: 0 for state in WheelState},
        quarantined_count=0,
        skipped_count=0,
        requires_prerelease_count=0,
        wal_bytes=1000,
        seconds_since_truncate_checkpoint=10.0,
        consecutive_checkpoint_failures=0,
        longest_read_txn_ms=1.0,
        read_txn_budget_violations=0,
        db_bytes=1000,
        freelist_count=0,
        writer_queue_depth=0,
        writer_failed_ops=0,
        segments_sealed=1,
        segments_open=1,
        open_segment_age_seconds=10.0,
        open_segment_bytes=1000,
        unsealed_records=0,
        archive_bytes=1000,
        disk_free_bytes=100 * 1024**3,
        limiter_global_available=100.0,
        limiter_children={},
        stages={
            "fetch": StageHealth(
                paused=False, last_run_at=1.0, last_success_at=1.0, consecutive_failures=0
            ),
        },
        dependencies={
            "pypi.org": DependencyHealth(
                state="closed", consecutive_failures=0, next_trial_at=None
            ),
        },
        error_counts_1h={},
        error_counts_24h={},
    )
    return dataclasses.replace(base, **overrides)


def _severities(found) -> list[str]:
    return [a.severity for a in found]


def test_no_alarms_on_a_healthy_snapshot():
    from reroll_sync.health import alarms

    assert alarms(_healthy_health()) == ()


# --- wal_bytes > 2 GB (critical) --------------------------------------------


def test_wal_bytes_just_below_threshold_is_no_alarm():
    from reroll_sync.health import WAL_BYTES_CRITICAL_DEFAULT, alarms

    health = _healthy_health(wal_bytes=WAL_BYTES_CRITICAL_DEFAULT - 1)
    assert not any(a.condition == "wal_bytes" for a in alarms(health))


def test_wal_bytes_at_threshold_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(wal_bytes=2 * 1024**3 + 0)
    assert not any(a.condition == "wal_bytes" for a in alarms(health))


def test_wal_bytes_just_above_threshold_is_critical():
    from reroll_sync.health import alarms

    health = _healthy_health(wal_bytes=2 * 1024**3 + 1)
    found = [a for a in alarms(health) if a.condition == "wal_bytes"]
    assert len(found) == 1
    assert found[0].severity == "critical"


# --- consecutive_checkpoint_failures >= 5 (critical) ------------------------


def test_checkpoint_failures_just_below_threshold_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(consecutive_checkpoint_failures=4)
    assert not any(a.condition == "consecutive_checkpoint_failures" for a in alarms(health))


def test_checkpoint_failures_at_threshold_is_critical():
    from reroll_sync.health import alarms

    health = _healthy_health(consecutive_checkpoint_failures=5)
    found = [a for a in alarms(health) if a.condition == "consecutive_checkpoint_failures"]
    assert len(found) == 1
    assert found[0].severity == "critical"


def test_checkpoint_failures_just_above_threshold_is_critical():
    from reroll_sync.health import alarms

    health = _healthy_health(consecutive_checkpoint_failures=6)
    found = [a for a in alarms(health) if a.condition == "consecutive_checkpoint_failures"]
    assert len(found) == 1
    assert found[0].severity == "critical"


# --- disk_free_bytes < floor (critical) -------------------------------------


def test_disk_free_bytes_just_above_floor_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(disk_free_bytes=20 * 1024**3 + 1)
    assert not any(a.condition == "disk_free_bytes" for a in alarms(health))


def test_disk_free_bytes_at_floor_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(disk_free_bytes=20 * 1024**3)
    assert not any(a.condition == "disk_free_bytes" for a in alarms(health))


def test_disk_free_bytes_just_below_floor_is_critical():
    from reroll_sync.health import alarms

    health = _healthy_health(disk_free_bytes=20 * 1024**3 - 1)
    found = [a for a in alarms(health) if a.condition == "disk_free_bytes"]
    assert len(found) == 1
    assert found[0].severity == "critical"


def test_disk_free_bytes_none_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(disk_free_bytes=None)
    assert not any(a.condition == "disk_free_bytes" for a in alarms(health))


# --- any breaker open (warning) ---------------------------------------------


def test_all_breakers_closed_is_no_alarm():
    from reroll_sync.health import alarms

    assert not any(a.condition == "breaker_open" for a in alarms(_healthy_health()))


def test_half_open_breaker_is_a_warning():
    from reroll_sync.health import DependencyHealth, alarms

    health = _healthy_health(
        dependencies={
            "pypi.org": DependencyHealth(
                state="half_open", consecutive_failures=5, next_trial_at=None
            )
        }
    )
    found = [a for a in alarms(health) if a.condition == "breaker_open"]
    assert len(found) == 1
    assert found[0].severity == "warning"


def test_open_breaker_is_a_warning():
    from reroll_sync.health import DependencyHealth, alarms

    health = _healthy_health(
        dependencies={
            "pypi.org": DependencyHealth(state="open", consecutive_failures=5, next_trial_at=1.0)
        }
    )
    found = [a for a in alarms(health) if a.condition == "breaker_open"]
    assert len(found) == 1
    assert found[0].severity == "warning"
    assert "pypi.org" in found[0].message


# --- read_txn_budget_violations increasing (warning) ------------------------


def test_zero_read_txn_budget_violations_with_no_previous_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(read_txn_budget_violations=0)
    assert not any(a.condition == "read_txn_budget_violations" for a in alarms(health))


def test_one_read_txn_budget_violation_with_no_previous_is_a_warning():
    from reroll_sync.health import alarms

    health = _healthy_health(read_txn_budget_violations=1)
    found = [a for a in alarms(health) if a.condition == "read_txn_budget_violations"]
    assert len(found) == 1
    assert found[0].severity == "warning"


def test_unchanged_read_txn_budget_violations_against_previous_is_no_alarm():
    from reroll_sync.health import alarms

    previous = _healthy_health(read_txn_budget_violations=3)
    health = _healthy_health(read_txn_budget_violations=3)
    assert not any(
        a.condition == "read_txn_budget_violations" for a in alarms(health, previous=previous)
    )


def test_increasing_read_txn_budget_violations_against_previous_is_a_warning():
    from reroll_sync.health import alarms

    previous = _healthy_health(read_txn_budget_violations=3)
    health = _healthy_health(read_txn_budget_violations=4)
    found = [
        a for a in alarms(health, previous=previous) if a.condition == "read_txn_budget_violations"
    ]
    assert len(found) == 1
    assert found[0].severity == "warning"


# --- quarantined_count > 0 (warning) ----------------------------------------


def test_zero_quarantined_is_no_alarm():
    from reroll_sync.health import alarms

    assert not any(a.condition == "quarantined_count" for a in alarms(_healthy_health()))


def test_one_quarantined_is_a_warning():
    from reroll_sync.health import alarms

    health = _healthy_health(quarantined_count=1)
    found = [a for a in alarms(health) if a.condition == "quarantined_count"]
    assert len(found) == 1
    assert found[0].severity == "warning"


# --- index_lag unchanged > 1h while stale projects remain (warning) --------


def test_index_lag_unchanged_just_under_an_hour_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(
        projects_stale=1,
        last_index_change_at=1_700_000_000.0,
        snapshot_at=1_700_000_000.0 + 3599.0,
    )
    assert not any(a.condition == "index_lag_stale" for a in alarms(health))


def test_index_lag_unchanged_at_exactly_an_hour_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(
        projects_stale=1,
        last_index_change_at=1_700_000_000.0,
        snapshot_at=1_700_000_000.0 + 3600.0,
    )
    assert not any(a.condition == "index_lag_stale" for a in alarms(health))


def test_index_lag_unchanged_just_over_an_hour_is_a_warning():
    from reroll_sync.health import alarms

    health = _healthy_health(
        projects_stale=1,
        last_index_change_at=1_700_000_000.0,
        snapshot_at=1_700_000_000.0 + 3601.0,
    )
    found = [a for a in alarms(health) if a.condition == "index_lag_stale"]
    assert len(found) == 1
    assert found[0].severity == "warning"


def test_index_lag_unchanged_over_an_hour_with_no_stale_projects_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(
        projects_stale=0,
        last_index_change_at=1_700_000_000.0,
        snapshot_at=1_700_000_000.0 + 7200.0,
    )
    assert not any(a.condition == "index_lag_stale" for a in alarms(health))


def test_index_lag_stale_with_no_last_change_at_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(projects_stale=1, last_index_change_at=None)
    assert not any(a.condition == "index_lag_stale" for a in alarms(health))


# --- open_segment_age_seconds > 2x seal_seconds (warning) -------------------


def test_open_segment_age_just_below_double_seal_seconds_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(open_segment_age_seconds=2 * 21600.0 - 1.0)
    assert not any(a.condition == "open_segment_age" for a in alarms(health))


def test_open_segment_age_at_double_seal_seconds_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(open_segment_age_seconds=2 * 21600.0)
    assert not any(a.condition == "open_segment_age" for a in alarms(health))


def test_open_segment_age_just_above_double_seal_seconds_is_a_warning():
    from reroll_sync.health import alarms

    health = _healthy_health(open_segment_age_seconds=2 * 21600.0 + 1.0)
    found = [a for a in alarms(health) if a.condition == "open_segment_age"]
    assert len(found) == 1
    assert found[0].severity == "warning"


def test_open_segment_age_none_is_no_alarm():
    from reroll_sync.health import alarms

    health = _healthy_health(open_segment_age_seconds=None)
    assert not any(a.condition == "open_segment_age" for a in alarms(health))


# --- a stage with consecutive_failures > 0 (warning) ------------------------


def test_zero_stage_consecutive_failures_is_no_alarm():
    from reroll_sync.health import alarms

    assert not any(a.condition == "stage_consecutive_failures" for a in alarms(_healthy_health()))


def test_one_stage_consecutive_failure_is_a_warning():
    from reroll_sync.health import StageHealth, alarms

    health = _healthy_health(
        stages={
            "fetch": StageHealth(
                paused=False, last_run_at=1.0, last_success_at=1.0, consecutive_failures=1
            )
        }
    )
    found = [a for a in alarms(health) if a.condition == "stage_consecutive_failures"]
    assert len(found) == 1
    assert found[0].severity == "warning"
    assert "fetch" in found[0].message


# --- writer_failed_ops > 0 (warning) ----------------------------------------


def test_zero_writer_failed_ops_is_no_alarm():
    from reroll_sync.health import alarms

    assert not any(a.condition == "writer_failed_ops" for a in alarms(_healthy_health()))


def test_one_writer_failed_op_is_a_warning():
    from reroll_sync.health import alarms

    health = _healthy_health(writer_failed_ops=1)
    found = [a for a in alarms(health) if a.condition == "writer_failed_ops"]
    assert len(found) == 1
    assert found[0].severity == "warning"


# --- Ordering and combinations -----------------------------------------------


def test_multiple_simultaneous_alarms_are_all_reported():
    from reroll_sync.health import alarms

    health = _healthy_health(quarantined_count=1, writer_failed_ops=1)
    found = alarms(health)
    conditions = {a.condition for a in found}
    assert conditions == {"quarantined_count", "writer_failed_ops"}


def test_a_critical_alarm_sorts_before_a_warning():
    from reroll_sync.health import alarms

    health = _healthy_health(quarantined_count=1, wal_bytes=2 * 1024**3 + 1)
    found = alarms(health)
    assert _severities(found) == ["critical", "warning"]


def test_snapshot_performs_no_writes(db_path, reader, writer):
    class _NoWriteConn:
        """Wraps a real connection, raising on any mutating `execute`."""

        # PRAGMA is deliberately not banned here: `_db_file_size` reads
        # `PRAGMA database_list`, which is a read-only reflection query,
        # not a mutation -- unlike, say, `PRAGMA wal_checkpoint`.
        _MUTATING_PREFIXES = ("INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT", "ROLLBACK")

        def __init__(self, real: sqlite3.Connection) -> None:
            self._real = real

        def execute(self, sql: str, parameters=()):
            if sql.strip().upper().startswith(_NoWriteConn._MUTATING_PREFIXES):
                raise AssertionError(f"snapshot() attempted a mutating statement: {sql!r}")
            return self._real.execute(sql, parameters)

        def __getattr__(self, name):
            return getattr(self._real, name)

    guarded = cast(sqlite3.Connection, _NoWriteConn(reader))
    health = _snapshot(guarded, writer)
    assert health.wheels_synced == 0
