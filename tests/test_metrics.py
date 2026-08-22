"""Tests for the Prometheus text-format exporter in `metrics.py`."""

from __future__ import annotations

import dataclasses
import re

from reroll_sync.health import (
    DependencyHealth,
    Health,
    StageHealth,
    StageQueue,
    snapshot,
)
from reroll_sync.metrics import render_metrics
from reroll_sync.ratelimit import ChildLimiterSnapshot
from reroll_sync.schema import WheelState

_SAMPLE_LINE_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[^}]*\})?\s+-?[0-9.eE+-]+$")
_TYPE_LINE_RE = re.compile(r"^# TYPE ([a-zA-Z_:][a-zA-Z0-9_:]*) (counter|gauge)$")


def _full_health() -> Health:
    """A `Health` with every mapping field populated, for the drift detector."""
    return Health(
        snapshot_at=1_700_000_000.0,
        index_lag=5,
        remote_last_serial=105,
        local_max_serial=100,
        last_index_poll_at=1_700_000_000.0,
        last_index_change_at=1_700_000_000.0,
        projects_indexed=10,
        projects_stale=1,
        pipeline_backlog=2,
        wheels_synced=20,
        queues={
            "fetch": StageQueue(
                depth=3,
                depth_by_lane={0: 2, 1: 1},
                in_flight=1,
                oldest_pending_age_seconds=12.0,
                throughput_ema=0.5,
                ok_count=1,
                skip_count=1,
                retry_count=1,
                rate_limited_count=1,
            ),
        },
        state_counts={state.name: 1 for state in WheelState},
        quarantined_count=1,
        skipped_count=1,
        requires_prerelease_count=1,
        wal_bytes=1000,
        seconds_since_truncate_checkpoint=10.0,
        consecutive_checkpoint_failures=0,
        longest_read_txn_ms=5.0,
        read_txn_budget_violations=0,
        db_bytes=1000,
        freelist_count=0,
        writer_queue_depth=0,
        writer_failed_ops=0,
        segments_sealed=1,
        segments_open=1,
        open_segment_age_seconds=10.0,
        open_segment_bytes=1000,
        unsealed_records=1,
        archive_bytes=1000,
        disk_free_bytes=100 * 1024**3,
        limiter_global_available=100.0,
        limiter_children={
            "pypi.org": ChildLimiterSnapshot(
                available=10.0, acquired=5, denied=1, penalty_deadline=0.0
            ),
        },
        stages={
            "fetch": StageHealth(
                paused=False, last_run_at=1.0, last_success_at=2.0, consecutive_failures=0
            ),
        },
        dependencies={
            "pypi.org": DependencyHealth(state="open", consecutive_failures=5, next_trial_at=123.0),
        },
        error_counts_1h={"network": 1},
        error_counts_24h={"network": 2},
    )


def _lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line]


# ---------------------------------------------------------------------------
# Valid Prometheus text
# ---------------------------------------------------------------------------


def test_output_ends_with_a_trailing_newline():
    text = render_metrics(_full_health())
    assert text.endswith("\n")


def test_every_line_is_a_type_declaration_or_a_valid_sample():
    text = render_metrics(_full_health())
    for line in _lines(text):
        if line.startswith("# TYPE"):
            assert _TYPE_LINE_RE.match(line), line
        else:
            assert _SAMPLE_LINE_RE.match(line), line


def test_every_metric_name_is_declared_before_its_first_sample():
    text = render_metrics(_full_health())
    declared: set[str] = set()
    for line in _lines(text):
        type_match = _TYPE_LINE_RE.match(line)
        if type_match:
            declared.add(type_match.group(1))
            continue
        name = line.split("{")[0].split(" ")[0]
        assert name in declared, f"{name} sampled before its # TYPE line"


def test_every_metric_name_is_prefixed():
    text = render_metrics(_full_health())
    for line in _lines(text):
        name = line.split()[2] if line.startswith("# TYPE") else line.split("{")[0].split(" ")[0]
        assert name.startswith("reroll_sync_")


# ---------------------------------------------------------------------------
# Counter vs gauge typing
# ---------------------------------------------------------------------------


def test_writer_failed_ops_is_a_counter():
    text = render_metrics(_full_health())
    assert "# TYPE reroll_sync_writer_failed_ops counter" in text


def test_read_txn_budget_violations_is_a_counter():
    text = render_metrics(_full_health())
    assert "# TYPE reroll_sync_read_txn_budget_violations counter" in text


def test_queue_outcome_counters_are_counters():
    text = render_metrics(_full_health())
    for name in ("ok_count", "skip_count", "retry_count", "rate_limited_count"):
        assert f"# TYPE reroll_sync_queues_{name} counter" in text


def test_segments_sealed_is_a_counter():
    text = render_metrics(_full_health())
    assert "# TYPE reroll_sync_segments_sealed counter" in text


def test_archive_bytes_is_a_counter():
    # segments_sealed and archive_bytes both derive from the same
    # never-shrinking sealed-segment set, so both get the same
    # classification (see metrics.py's `_COUNTER_FAMILIES` docstring).
    text = render_metrics(_full_health())
    assert "# TYPE reroll_sync_archive_bytes counter" in text


def test_wal_bytes_is_a_gauge():
    text = render_metrics(_full_health())
    assert "# TYPE reroll_sync_wal_bytes gauge" in text


def test_queue_depth_is_a_gauge():
    text = render_metrics(_full_health())
    assert "# TYPE reroll_sync_queues_depth gauge" in text


def test_state_counts_is_a_gauge():
    text = render_metrics(_full_health())
    assert "# TYPE reroll_sync_state_counts gauge" in text


# ---------------------------------------------------------------------------
# Values and labels
# ---------------------------------------------------------------------------


def test_scalar_field_value_matches_the_snapshot():
    text = render_metrics(_full_health())
    assert "reroll_sync_wal_bytes 1000" in _lines(text)


def test_state_counts_are_labeled_by_state():
    text = render_metrics(_full_health())
    assert 'reroll_sync_state_counts{state="READY"} 1' in _lines(text)


def test_error_counts_are_labeled_by_category():
    text = render_metrics(_full_health())
    assert 'reroll_sync_error_counts_1h{category="network"} 1' in _lines(text)


def test_queue_depth_by_lane_has_two_labels():
    text = render_metrics(_full_health())
    assert 'reroll_sync_queues_depth_by_lane{stage="fetch",lane="0"} 2' in _lines(text)
    assert 'reroll_sync_queues_depth_by_lane{stage="fetch",lane="1"} 1' in _lines(text)


def test_dependency_state_is_numerically_encoded():
    text = render_metrics(_full_health())
    assert 'reroll_sync_dependencies_state{dependency="pypi.org"} 2' in _lines(text)


def _decode_label_value(text: str) -> str:
    """Reverse of `metrics._escape_label_value`, for round-trip assertions."""
    out: list[str] = []
    i = 0
    while i < len(text):
        char = text[i]
        if char == "\\" and i + 1 < len(text):
            nxt = text[i + 1]
            out.append({"n": "\n", '"': '"', "\\": "\\"}.get(nxt, nxt))
            i += 2
        else:
            out.append(char)
            i += 1
    return "".join(out)


def test_label_value_with_quote_and_backslash_is_escaped_and_round_trippable():
    raw = 'has "quotes" and \\backslashes\\'
    health = dataclasses.replace(_full_health(), error_counts_1h={raw: 1})
    text = render_metrics(health)

    escaped = raw.replace("\\", "\\\\").replace('"', '\\"')
    assert f'category="{escaped}"' in text

    line = next(line for line in _lines(text) if line.startswith("reroll_sync_error_counts_1h{"))
    assert _SAMPLE_LINE_RE.match(line)
    label_value = line[len('reroll_sync_error_counts_1h{category="') : line.index('"} ')]
    assert _decode_label_value(label_value) == raw


def test_none_valued_fields_produce_no_sample():
    health = dataclasses.replace(_full_health(), remote_last_serial=None)
    text = render_metrics(health)
    assert "reroll_sync_remote_last_serial" not in text


def test_empty_mapping_produces_no_samples_for_that_family():
    health = dataclasses.replace(_full_health(), error_counts_1h={})
    text = render_metrics(health)
    assert "reroll_sync_error_counts_1h" not in text


# ---------------------------------------------------------------------------
# Drift detector: every Health field must appear in the output
# ---------------------------------------------------------------------------


def test_every_health_field_appears_in_the_metrics_output():
    health = _full_health()
    text = render_metrics(health)
    missing = [
        field.name
        for field in dataclasses.fields(Health)
        if f"reroll_sync_{field.name}" not in text
    ]
    assert missing == []


# ---------------------------------------------------------------------------
# Same source as snapshot()
# ---------------------------------------------------------------------------


def test_metrics_and_snapshot_share_one_source_of_numbers(tmp_path):
    import sqlite3

    from reroll_sync.daemon.circuit_breaker import CircuitBreaker
    from reroll_sync.daemon.stage_loop import StageLoopStats
    from reroll_sync.db import connect_reader, init_db
    from reroll_sync.health import StageInput
    from reroll_sync.ratelimit import HierarchicalLimiter
    from reroll_sync.writer import Writer

    path = str(tmp_path / "metrics.db")
    init_db(path)
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO wheels (filename, project, state, lane, url, serial, change_seq, updated_at) "
        "VALUES ('a-1.0-py3-none-any.whl', 'proj', 5, 0, 'https://x', 1, 1, '2024-01-01T00:00:00Z')"
    )
    conn.commit()
    conn.close()

    reader = connect_reader(path)
    writer_conn = sqlite3.connect(path, check_same_thread=False)
    writer = Writer(writer_conn, batch_size=1, batch_interval=1_000_000.0)
    writer.start()
    try:
        health = snapshot(
            reader,
            writer,
            HierarchicalLimiter(2000.0, {"pypi.org": 200.0}),
            {"pypi.org": CircuitBreaker()},
            {"fetch": StageInput(loop=StageLoopStats(None, None, 0, False))},
        )
        text = render_metrics(health)
        assert 'reroll_sync_state_counts{state="QUARANTINED"} 1' in _lines(text)
        assert health.quarantined_count == 1
    finally:
        writer.stop(drain=False)
        reader.close()


def test_to_numeric_returns_none_for_an_unsupported_type():
    from reroll_sync.metrics import _to_numeric

    assert _to_numeric([1, 2, 3]) is None
