"""Tests for the dispatcher: queue selection, outcome application, backoff,
serialization, reprocess campaigns, and metrics.
"""

from __future__ import annotations

import random
import sqlite3
import threading

import pytest

from reroll_sync.convert import ConvertOk, ConvertRetry, ConvertSkip
from reroll_sync.db import init_db
from reroll_sync.dispatcher import (
    BASE_BACKOFF_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    MAX_BACKOFF_SECONDS,
    Dispatcher,
    IllegalTransitionError,
    Ok,
    ProjectSelector,
    QueueItem,
    RateLimited,
    RerollVersionBelow,
    SkippedOnly,
    Stage,
    StateSelector,
    adapt_convert_outcome,
    compress_json,
    compute_backoff,
    decompress_json,
)
from reroll_sync.schema import WheelState
from reroll_sync.writer import Writer

# ---------------------------------------------------------------------------
# Shared fixtures/helpers
# ---------------------------------------------------------------------------


class FakeClock:
    """A manually-advanced clock returning a float "epoch-like" value."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.value = start

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def _writer_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 50")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "dispatcher.db")
    init_db(path)
    return path


@pytest.fixture
def writers():
    created: list[Writer] = []

    def _make(conn, **kwargs) -> Writer:
        writer = Writer(conn, **kwargs)
        created.append(writer)
        return writer

    yield _make
    for writer in created:
        if writer._started and not writer._stopped:
            writer.stop(drain=False)


@pytest.fixture
def writer(db_path, writers):
    conn = _writer_conn(db_path)
    w = writers(conn, batch_size=1, batch_interval=1_000_000.0)
    w.start()
    return w


@pytest.fixture
def reader(db_path):
    conn = sqlite3.connect(str(db_path))
    yield conn
    conn.close()


def _insert_wheel(
    conn: sqlite3.Connection,
    *,
    filename: str,
    project: str = "proj",
    state: WheelState = WheelState.NEED_CONVERT,
    lane: int = 0,
    conda_name: str | None = None,
) -> int:
    cursor = conn.execute(
        "INSERT INTO wheels "
        "(filename, project, conda_name, state, lane, url, serial, change_seq, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 1, 1, '2024-01-01T00:00:00+00:00')",
        (filename, project, conda_name, int(state), lane, f"https://example.test/{filename}"),
    )
    conn.commit()
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _insert_work(
    conn: sqlite3.Connection,
    *,
    wheel_id: int,
    stage: str = "convert",
    attempts: int = 1,
    next_attempt_at: str = "2024-01-01T00:00:00+00:00",
    quarantined_at: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO work (wheel_id, stage, attempts, next_attempt_at, quarantined_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (wheel_id, stage, attempts, next_attempt_at, quarantined_at),
    )
    conn.commit()


def _dispatcher(reader, writer, *, clock: FakeClock | None = None, **kwargs) -> Dispatcher:
    clock = clock if clock is not None else FakeClock()
    return Dispatcher(reader, writer, reroll_version="1.2.3", now=clock.now, **kwargs)


def _record(name="example", resolutions=()):
    import reroll

    return reroll.WheelRecord(
        name=name,
        version="1.0",
        build="py_0",
        build_number=0,
        subdir="noarch",
        fn=f"{name}-1.0-py3-none-any.whl",
        noarch="python",
        depends=(),
        extra_depends={},
        name_resolutions=resolutions,
    )


def _name_resolution(pypi_name="example"):
    import reroll
    from reroll.name_mapping import CandidateSource

    winner = reroll.Winner(
        conda_name=pypi_name, probability=1.0, source=CandidateSource.PASSTHROUGH, mapper="m"
    )
    return reroll.NameResolution(pypi_name=pypi_name, winner=winner)


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


def test_claim_returns_at_most_limit_items(reader, writer):
    conn = sqlite3.connect(reader.execute("PRAGMA database_list").fetchone()[2])
    for i in range(5):
        _insert_wheel(conn, filename=f"pkg-{i}-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    items = dispatcher.claim(Stage.CONVERT, limit=3)
    assert len(items) == 3


class _CountingCursor:
    """Wraps a real cursor, recording how many rows ``fetchall`` ever returns."""

    def __init__(self, cursor, counts: list[int]) -> None:
        self._cursor = cursor
        self._counts = counts

    def fetchall(self):
        rows = self._cursor.fetchall()
        self._counts.append(len(rows))
        return rows

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _CountingConn:
    """Wraps a real connection, routing ``execute`` through :class:`_CountingCursor`."""

    def __init__(self, conn: sqlite3.Connection, counts: list[int]) -> None:
        self._conn = conn
        self._counts = counts

    def execute(self, sql, params=()):
        return _CountingCursor(self._conn.execute(sql, params), self._counts)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_claim_never_materializes_more_than_limit_rows(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(50):
        _insert_wheel(conn, filename=f"many-{i}-1.0-py3-none-any.whl")
    conn.close()

    fetched_row_counts: list[int] = []
    counting_reader = _CountingConn(reader, fetched_row_counts)
    dispatcher = _dispatcher(counting_reader, writer)
    items = dispatcher.claim(Stage.CONVERT, limit=5)
    assert len(items) == 5
    assert all(count <= 5 for count in fetched_row_counts)


def test_items_are_ordered_by_lane_project_id(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="b-1.0-py3-none-any.whl", project="b")
    _insert_wheel(conn, filename="a-2.0-py3-none-any.whl", project="a")
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", project="a")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    items = dispatcher.claim(Stage.CONVERT, limit=10)
    assert [(i.project, i.id) for i in items] == [("a", 2), ("a", 3), ("b", 1)]


def test_lane_0_items_returned_before_lane_1_even_with_lower_ids(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="backfill-1.0-py3-none-any.whl", lane=1)
    _insert_wheel(conn, filename="incremental-1.0-py3-none-any.whl", lane=0)
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    items = dispatcher.claim(Stage.CONVERT, limit=10)
    assert [i.lane for i in items] == [0, 1]
    assert items[0].id == 2


def test_item_with_future_next_attempt_at_is_excluded(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="future-1.0-py3-none-any.whl")
    _insert_work(conn, wheel_id=wheel_id, next_attempt_at="2999-01-01T00:00:00+00:00")
    conn.close()
    clock = FakeClock(start=1_700_000_000.0)
    dispatcher = _dispatcher(reader, writer, clock=clock)
    items = dispatcher.claim(Stage.CONVERT, limit=10)
    assert items == []


def test_item_becomes_eligible_once_clock_passes_next_attempt_at(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="soon-1.0-py3-none-any.whl")
    _insert_work(conn, wheel_id=wheel_id, next_attempt_at="1970-01-01T00:00:31+00:00")
    conn.close()
    clock = FakeClock(start=30.0)
    dispatcher = _dispatcher(reader, writer, clock=clock)
    assert dispatcher.claim(Stage.CONVERT, limit=10) == []
    clock.advance(2.0)
    items = dispatcher.claim(Stage.CONVERT, limit=10)
    assert [i.id for i in items] == [wheel_id]


def test_quarantined_item_is_never_returned(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="quarantined-1.0-py3-none-any.whl")
    _insert_work(
        conn,
        wheel_id=wheel_id,
        next_attempt_at="1970-01-01T00:00:00+00:00",
        quarantined_at="2024-01-01T00:00:00+00:00",
    )
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    assert dispatcher.claim(Stage.CONVERT, limit=10) == []


def test_in_flight_item_is_not_returned_by_a_second_claim(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="one-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    first = dispatcher.claim(Stage.CONVERT, limit=10)
    assert len(first) == 1
    second = dispatcher.claim(Stage.CONVERT, limit=10)
    assert second == []


def test_keyset_cursor_advances_and_wraps(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", project="p")
    _insert_wheel(conn, filename="b-1.0-py3-none-any.whl", project="p")
    _insert_wheel(conn, filename="c-1.0-py3-none-any.whl", project="p")
    conn.close()
    dispatcher = _dispatcher(reader, writer)

    first = dispatcher.claim(Stage.CONVERT, limit=2)
    assert [i.id for i in first] == [1, 2]
    for item in first:
        dispatcher.release(Stage.CONVERT, item.id)

    second = dispatcher.claim(Stage.CONVERT, limit=2)
    assert [i.id for i in second] == [3]
    for item in second:
        dispatcher.release(Stage.CONVERT, item.id)

    third = dispatcher.claim(Stage.CONVERT, limit=2)
    assert [i.id for i in third] == [1, 2]


def test_claim_on_empty_queue_returns_empty_list(reader, writer):
    dispatcher = _dispatcher(reader, writer)
    assert dispatcher.claim(Stage.CONVERT, limit=10) == []


def test_claim_query_plan_has_no_scan(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(20):
        _insert_wheel(conn, filename=f"plan-{i}-1.0-py3-none-any.whl")
    conn.close()
    from reroll_sync.dispatcher import _claim_query
    from reroll_sync.schema import WheelState as WS

    sql, params = _claim_query(
        WS.NEED_CONVERT, Stage.CONVERT, None, set(), "1970-01-01T00:00:00+00:00", 10
    )
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN" in d for d in details)


def test_claim_query_plan_has_no_scan_with_cursor_and_in_flight(db_path, reader, writer):
    # The combination the spec worries about: a keyset cursor *and* a
    # non-empty in-flight exclusion set at once, which adds both an OR-chain
    # lower bound and a `NOT IN (...)` filter to the same query -- either
    # could tempt the planner into abandoning the index for a full scan.
    conn = sqlite3.connect(db_path)
    for i in range(20):
        _insert_wheel(conn, filename=f"plan-cursor-{i}-1.0-py3-none-any.whl")
    conn.close()
    from reroll_sync.dispatcher import _claim_query
    from reroll_sync.schema import WheelState as WS

    sql, params = _claim_query(
        WS.NEED_CONVERT,
        Stage.CONVERT,
        (0, "proj", 5),
        {7, 8, 9},
        "1970-01-01T00:00:00+00:00",
        10,
    )
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN" in d for d in details)


def test_claim_query_plan_has_no_scan_for_fetch_stage(db_path, reader, writer):
    # The spec requires this for *each* stage, not just CONVERT.
    conn = sqlite3.connect(db_path)
    for i in range(20):
        _insert_wheel(
            conn, filename=f"plan-fetch-{i}-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA
        )
    conn.close()
    from reroll_sync.dispatcher import _claim_query
    from reroll_sync.schema import WheelState as WS

    sql, params = _claim_query(
        WS.NEED_METADATA, Stage.FETCH, None, set(), "1970-01-01T00:00:00+00:00", 10
    )
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN" in d for d in details)


def test_fetch_stage_queue_uses_need_metadata_state(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    _insert_wheel(conn, filename="fetchme-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA)
    _insert_wheel(conn, filename="convertme-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT)
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    fetch_items = dispatcher.claim(Stage.FETCH, limit=10)
    assert [i.id for i in fetch_items] == [1]


# ---------------------------------------------------------------------------
# Outcome application
# ---------------------------------------------------------------------------


def _item(wheel_id: int, *, state=WheelState.NEED_CONVERT, project="proj", lane=0) -> QueueItem:
    return QueueItem(id=wheel_id, project=project, lane=lane, state=state)


def test_ok_advances_state_writes_payload_bumps_change_seq(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="ok-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    seq_before = writer.current_seq()

    outcome = adapt_convert_outcome(
        ConvertOk(
            records=(_record(),),
            resolutions=(_name_resolution(),),
            conda_name="example",
            requires_prerelease=False,
        ),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)

    row = reader.execute(
        "SELECT state, conda_name, change_seq FROM wheels WHERE id = ?", (wheel_id,)
    ).fetchone()
    assert row[0] == int(WheelState.READY)
    assert row[1] == "example"
    assert row[2] > seq_before
    repodata_row = reader.execute(
        "SELECT repodata_zst, reroll_version FROM wheel_repodata WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert repodata_row is not None
    assert repodata_row[1] == "1.2.3"


def test_ok_deletes_preexisting_work_row(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="hadwork-1.0-py3-none-any.whl")
    _insert_work(conn, wheel_id=wheel_id, attempts=3)
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    outcome = adapt_convert_outcome(
        ConvertOk(
            records=(_record(),), resolutions=(), conda_name="example", requires_prerelease=False
        ),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)
    row = reader.execute(
        "SELECT COUNT(*) FROM work WHERE wheel_id = ? AND stage = 'convert'", (wheel_id,)
    ).fetchone()
    assert row[0] == 0


def test_skip_sets_skipped_writes_skips_and_errors_deletes_work(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="skip-1.0-py3-none-any.whl")
    _insert_work(conn, wheel_id=wheel_id, attempts=2)
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    outcome = adapt_convert_outcome(
        ConvertSkip(
            reason="bad_metadata",
            subcategory="ParseError",
            details="oops",
            permanent=False,
            reroll_version="1.2.3",
        ),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)

    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.SKIPPED)

    skip_row = reader.execute(
        "SELECT reason, permanent, reroll_version FROM skips "
        "WHERE wheel_id = ? AND stage = 'convert'",
        (wheel_id,),
    ).fetchone()
    assert skip_row == ("bad_metadata", 0, "1.2.3")

    error_row = reader.execute(
        "SELECT error_category, error_subcat, details FROM errors WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert error_row == ("bad_metadata", "ParseError", "oops")

    work_row = reader.execute(
        "SELECT COUNT(*) FROM work WHERE wheel_id = ? AND stage = 'convert'", (wheel_id,)
    ).fetchone()
    assert work_row[0] == 0


def test_skip_with_permanent_true_writes_null_reroll_version(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="perm-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    outcome = adapt_convert_outcome(
        ConvertSkip(
            reason="bad_encoding",
            subcategory="UnicodeDecodeError",
            details="nope",
            permanent=True,
            reroll_version=None,
        ),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)
    skip_row = reader.execute(
        "SELECT permanent, reroll_version FROM skips WHERE wheel_id = ? AND stage = 'convert'",
        (wheel_id,),
    ).fetchone()
    assert skip_row == (1, None)


def test_retry_creates_work_row_with_attempts_one_state_unchanged(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="retry-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    outcome = adapt_convert_outcome(
        ConvertRetry(reason="transient", details="503"), reroll_version="1.2.3"
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)

    work_row = reader.execute(
        "SELECT attempts FROM work WHERE wheel_id = ? AND stage = 'convert'", (wheel_id,)
    ).fetchone()
    assert work_row[0] == 1
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.NEED_CONVERT)


def test_successive_retries_increment_attempts_and_push_next_attempt_at_out(
    db_path, reader, writer
):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="successive-1.0-py3-none-any.whl")
    conn.close()
    clock = FakeClock()
    dispatcher = _dispatcher(reader, writer, clock=clock, rng=random.Random(1))
    outcome = adapt_convert_outcome(
        ConvertRetry(reason="transient", details="x"), reroll_version="1.2.3"
    )

    attempts_seen = []
    next_attempts_seen = []
    for _ in range(3):
        dispatcher.claim(Stage.CONVERT, limit=10)
        dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)
        row = reader.execute(
            "SELECT attempts, next_attempt_at FROM work WHERE wheel_id = ?", (wheel_id,)
        ).fetchone()
        attempts_seen.append(row[0])
        next_attempts_seen.append(row[1])
        clock.advance(1.0)

    assert attempts_seen == [1, 2, 3]
    assert next_attempts_seen == sorted(next_attempts_seen)
    assert len(set(next_attempts_seen)) == 3


def test_retry_at_max_attempts_sets_quarantined_and_errors_row(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="quarantine-1.0-py3-none-any.whl")
    _insert_work(conn, wheel_id=wheel_id, attempts=DEFAULT_MAX_ATTEMPTS)
    conn.close()
    dispatcher = _dispatcher(reader, writer, rng=random.Random(1))
    outcome = adapt_convert_outcome(
        ConvertRetry(reason="transient", details="still failing"), reroll_version="1.2.3"
    )

    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)

    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.QUARANTINED)
    work_row = reader.execute(
        "SELECT attempts, quarantined_at FROM work WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert work_row[0] == DEFAULT_MAX_ATTEMPTS + 1
    assert work_row[1] is not None
    error_row = reader.execute(
        "SELECT error_category, details FROM errors WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert error_row == ("transient", "still failing")


def test_eighth_attempt_still_retries_not_quarantines(db_path, reader, writer):
    # max_attempts=8 default: the 8th failure still schedules a retry
    # (the ~1h4m delay); only the 9th failure quarantines. This is the
    # arithmetic spec 07 describes ("roughly 30s, ..., 1h4m before
    # quarantine, ~2 hours total").
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="eighth-1.0-py3-none-any.whl")
    _insert_work(conn, wheel_id=wheel_id, attempts=DEFAULT_MAX_ATTEMPTS - 1)
    conn.close()
    dispatcher = _dispatcher(reader, writer, rng=random.Random(1))
    outcome = adapt_convert_outcome(
        ConvertRetry(reason="transient", details="x"), reroll_version="1.2.3"
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)

    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.NEED_CONVERT)
    work_row = reader.execute(
        "SELECT attempts FROM work WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert work_row[0] == DEFAULT_MAX_ATTEMPTS


def test_illegal_transition_raises(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="illegal-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    outcome = Ok(next_state=WheelState.NEED_METADATA, write=lambda conn, wheel_id: None)
    with pytest.raises(IllegalTransitionError):
        dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id, state=WheelState.READY), outcome)


def test_illegal_transition_still_releases_a_genuinely_claimed_item(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="illegal-claimed-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    [item] = dispatcher.claim(Stage.CONVERT, limit=10)
    assert item.id == wheel_id
    assert wheel_id in dispatcher._in_flight[Stage.CONVERT]

    # NEED_CONVERT -> NEED_METADATA is not in ALLOWED_TRANSITIONS.
    outcome = Ok(next_state=WheelState.NEED_METADATA, write=lambda conn, wheel_id: None)
    with pytest.raises(IllegalTransitionError):
        dispatcher.apply_outcome(Stage.CONVERT, item, outcome)

    assert wheel_id not in dispatcher._in_flight[Stage.CONVERT]


def test_rate_limited_does_not_increment_attempts_calls_penalize_leaves_eligible(
    db_path, reader, writer
):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="throttled-1.0-py3-none-any.whl")
    conn.close()

    penalized = []

    class _FakeLimiter:
        def penalize(self, child_name, seconds):
            penalized.append((child_name, seconds))

    dispatcher = _dispatcher(reader, writer, limiter=_FakeLimiter())
    dispatcher.claim(Stage.CONVERT, limit=10)
    dispatcher.apply_outcome(
        Stage.CONVERT, _item(wheel_id), RateLimited(child="pypi.org", seconds=5.0)
    )

    assert penalized == [("pypi.org", 5.0)]
    work_row = reader.execute(
        "SELECT COUNT(*) FROM work WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert work_row[0] == 0
    items = dispatcher.claim(Stage.CONVERT, limit=10)
    assert [i.id for i in items] == [wheel_id]


def test_rate_limited_without_a_limiter_configured_does_not_raise(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="nolimiter-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    result = dispatcher.apply_outcome(
        Stage.CONVERT, _item(wheel_id), RateLimited(child="x", seconds=1.0)
    )
    assert result is None


def test_apply_outcome_releases_item_from_in_flight_even_on_ok(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="release-1.0-py3-none-any.whl")
    _insert_wheel(conn, filename="release2-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    [item] = [i for i in dispatcher.claim(Stage.CONVERT, limit=10) if i.id == wheel_id]
    outcome = adapt_convert_outcome(
        ConvertOk(
            records=(_record(),), resolutions=(), conda_name="example", requires_prerelease=False
        ),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, item, outcome)
    assert wheel_id not in dispatcher._in_flight[Stage.CONVERT]


def test_adapt_convert_outcome_raises_on_unsupported_type():
    from typing import cast

    from reroll_sync.convert import ConvertOutcome
    from reroll_sync.dispatcher import adapt_convert_outcome as adapt

    with pytest.raises(TypeError):
        adapt(cast(ConvertOutcome, object()), reroll_version="1.0")


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_ok_outcomes_commit_change_seq_in_submission_order(db_path, reader, writer):
    """``change_seq`` must be allocated in the writer's own commit order.

    Forces thread A's write to be submitted to the writer strictly before
    thread B's, via ``threading.Event``s controlling both ``next_seq`` and
    ``submit`` calls -- regardless of which thread (or the writer's own
    background thread) actually invokes ``next_seq``. If ``change_seq`` were
    allocated on the calling thread before submission (the bug), thread B
    could win the ``next_seq`` race yet still be submitted second, leaving
    the wheel committed second with a *smaller* ``change_seq`` than the one
    committed first.
    """
    conn = sqlite3.connect(db_path)
    wheel_a = _insert_wheel(conn, filename="race-a-1.0-py3-none-any.whl")
    wheel_b = _insert_wheel(conn, filename="race-b-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)

    thread_a_started = threading.Event()
    thread_b_started = threading.Event()
    b_next_seq_done = threading.Event()
    a_submit_done = threading.Event()

    real_next_seq = writer.next_seq
    real_submit = writer.submit
    thread_a: threading.Thread | None = None
    thread_b: threading.Thread | None = None

    def patched_next_seq() -> int:
        current = threading.current_thread()
        if current is thread_b:
            value = real_next_seq()
            b_next_seq_done.set()
            return value
        if current is thread_a:
            b_next_seq_done.wait(timeout=5.0)
            return real_next_seq()
        return real_next_seq()

    def patched_submit(op) -> None:
        current = threading.current_thread()
        if current is thread_a:
            real_submit(op)
            a_submit_done.set()
            return
        if current is thread_b:
            a_submit_done.wait(timeout=5.0)
            real_submit(op)
            return
        real_submit(op)

    writer.next_seq = patched_next_seq
    writer.submit = patched_submit

    def _run_a() -> None:
        thread_a_started.set()
        outcome = Ok(next_state=WheelState.READY, write=lambda conn, wheel_id: None)
        dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_a), outcome)

    def _run_b() -> None:
        thread_b_started.set()
        outcome = Ok(next_state=WheelState.READY, write=lambda conn, wheel_id: None)
        dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_b), outcome)

    thread_a = threading.Thread(target=_run_a)
    thread_b = threading.Thread(target=_run_b)
    thread_b.start()
    thread_b_started.wait(timeout=5.0)
    thread_a.start()
    thread_a_started.wait(timeout=5.0)
    thread_a.join(timeout=5.0)
    thread_b.join(timeout=5.0)
    assert not thread_a.is_alive()
    assert not thread_b.is_alive()

    seq_a, seq_b = (
        reader.execute("SELECT change_seq FROM wheels WHERE id = ?", (wheel_a,)).fetchone()[0],
        reader.execute("SELECT change_seq FROM wheels WHERE id = ?", (wheel_b,)).fetchone()[0],
    )
    # Thread A was forced to submit (enter the writer's queue) strictly
    # before thread B, and the writer's batch_size=1 commits one op at a
    # time in FIFO order -- so A committed first and must carry the
    # smaller change_seq no matter which thread won the next_seq race.
    assert seq_a != seq_b
    assert seq_a < seq_b


# ---------------------------------------------------------------------------
# Backoff
# ---------------------------------------------------------------------------


def test_backoff_delays_follow_formula_within_jitter_band():
    rng = random.Random(0)
    for n in range(1, 9):
        expected_base = BASE_BACKOFF_SECONDS * 2 ** (n - 1)
        delay = compute_backoff(n, rng=rng)
        assert expected_base * 0.5 <= delay < expected_base * 1.5


def test_backoff_is_capped_at_six_hours():
    rng = random.Random(0)
    delay = compute_backoff(20, rng=rng)
    assert delay <= MAX_BACKOFF_SECONDS * 1.5
    assert delay >= MAX_BACKOFF_SECONDS * 0.5


def test_jitter_produces_different_delays_for_equal_attempts_with_seeded_rng():
    rng = random.Random(42)
    delay_a = compute_backoff(3, rng=rng)
    delay_b = compute_backoff(3, rng=rng)
    assert delay_a != delay_b


def test_jitter_never_produces_a_delay_less_than_or_equal_to_zero():
    rng = random.Random(7)
    for n in range(1, 9):
        assert compute_backoff(n, rng=rng) > 0


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def test_compress_json_round_trips_a_record_list():
    obj = [{"name": "numpy", "version": "1.0"}, {"name": "attrs", "version": "2.0"}]
    assert decompress_json(compress_json(obj)) == obj


def test_compress_json_round_trips_empty_list():
    assert decompress_json(compress_json([])) == []


def test_compress_json_round_trips_empty_dict():
    assert decompress_json(compress_json({})) == {}


def test_compress_json_round_trips_non_ascii_strings():
    obj = {"name": "pačkage", "note": "日本語"}
    assert decompress_json(compress_json(obj)) == obj


def test_stored_repodata_zst_decompresses_to_matching_json(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="repod-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    record = _record(name="matchcheck")
    outcome = adapt_convert_outcome(
        ConvertOk(
            records=(record,), resolutions=(), conda_name="matchcheck", requires_prerelease=False
        ),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)
    (blob,) = reader.execute(
        "SELECT repodata_zst FROM wheel_repodata WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    decoded = decompress_json(blob)
    assert decoded == [record.model_dump(mode="json")]


def test_compression_reduces_size_for_a_realistic_record_list():
    obj = [
        {"name": "numpy", "version": "1.26.4", "depends": ["python >=3.9"] * 20} for _ in range(50)
    ]
    raw = __import__("json").dumps(obj).encode("utf-8")
    compressed = compress_json(obj)
    assert len(compressed) < len(raw)


# ---------------------------------------------------------------------------
# Reprocess
# ---------------------------------------------------------------------------


def test_reroll_version_below_resets_matching_wheel_repodata_rows(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="old-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.execute(
        "INSERT INTO wheel_repodata (wheel_id, repodata_zst, reroll_version) VALUES (?, ?, ?)",
        (wheel_id, b"blob", "1.0.0"),
    )
    conn.commit()
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(RerollVersionBelow(version="2.0.0"))
    assert affected == 1
    row = reader.execute("SELECT state, lane FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row == (int(WheelState.NEED_CONVERT), 1)
    repodata_row = reader.execute(
        "SELECT COUNT(*) FROM wheel_repodata WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert repodata_row[0] == 0


def test_reroll_version_below_deletes_non_permanent_skips_below_version(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="skipped-1.0-py3-none-any.whl", state=WheelState.SKIPPED
    )
    conn.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, 'convert', 'r', 0, '1.0.0', '2024-01-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.commit()
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(RerollVersionBelow(version="2.0.0"))
    assert affected == 1
    skips_row = reader.execute(
        "SELECT COUNT(*) FROM skips WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert skips_row[0] == 0
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.NEED_CONVERT)


def test_reroll_version_below_preserves_permanent_skips(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="permskip-1.0-py3-none-any.whl", state=WheelState.SKIPPED
    )
    conn.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, 'convert', 'r', 1, NULL, '2024-01-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.commit()
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(RerollVersionBelow(version="2.0.0"))
    assert affected == 0
    skips_row = reader.execute(
        "SELECT COUNT(*) FROM skips WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert skips_row[0] == 1
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.SKIPPED)


def test_reroll_version_below_preserves_skips_with_newer_reroll_version(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="newerskip-1.0-py3-none-any.whl", state=WheelState.SKIPPED
    )
    conn.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, 'convert', 'r', 0, '5.0.0', '2024-01-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.commit()
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(RerollVersionBelow(version="2.0.0"))
    assert affected == 0
    skips_row = reader.execute(
        "SELECT COUNT(*) FROM skips WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert skips_row[0] == 1


def test_reroll_version_below_dedupes_a_wheel_skipped_at_multiple_stages(db_path, reader, writer):
    # A single wheel can have a non-permanent skips row per stage. Both
    # rows below the version threshold must only reset the wheel once.
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="multistage-1.0-py3-none-any.whl", state=WheelState.SKIPPED
    )
    conn.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, 'fetch', 'r', 0, '1.0.0', '2024-01-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, 'convert', 'r', 0, '1.0.0', '2024-01-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.commit()
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(RerollVersionBelow(version="2.0.0"))
    assert affected == 1
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.NEED_CONVERT)


def test_reroll_version_below_processes_multiple_distinct_versions(db_path, reader, writer):
    # wheel_repodata rows below the threshold with different reroll_version
    # values must all be found, across however many chunk pages that takes.
    conn = sqlite3.connect(db_path)
    wheel_ids = []
    for i, version in enumerate(("1.0.0", "1.1.0", "1.2.0")):
        wheel_id = _insert_wheel(
            conn, filename=f"multiversion-{i}-1.0-py3-none-any.whl", state=WheelState.READY
        )
        conn.execute(
            "INSERT INTO wheel_repodata (wheel_id, repodata_zst, reroll_version) VALUES (?, ?, ?)",
            (wheel_id, b"blob", version),
        )
        wheel_ids.append(wheel_id)
    conn.commit()
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(RerollVersionBelow(version="2.0.0"), chunk_size=1)
    assert affected == 3
    for wheel_id in wheel_ids:
        row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
        assert row[0] == int(WheelState.NEED_CONVERT)


def test_reprocess_clears_work_rows_including_quarantined(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="quar-1.0-py3-none-any.whl", state=WheelState.QUARANTINED
    )
    _insert_work(conn, wheel_id=wheel_id, attempts=9, quarantined_at="2024-01-01T00:00:00+00:00")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(StateSelector(state=WheelState.QUARANTINED))
    assert affected == 1
    work_row = reader.execute(
        "SELECT COUNT(*) FROM work WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert work_row[0] == 0
    wheel_row = reader.execute(
        "SELECT state, lane FROM wheels WHERE id = ?", (wheel_id,)
    ).fetchone()
    assert wheel_row == (int(WheelState.NEED_CONVERT), 1)


def test_reprocess_is_chunked_ceil_n_over_c_write_ops(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(7):
        _insert_wheel(conn, filename=f"chunk-{i}-1.0-py3-none-any.whl", project="chunky")
    conn.close()

    calls = []
    dispatcher = _dispatcher(reader, writer)
    real_submit_and_wait = writer.submit_and_wait

    def _counting_submit_and_wait(op):
        calls.append(op)
        return real_submit_and_wait(op)

    writer.submit_and_wait = _counting_submit_and_wait
    affected = dispatcher.reprocess(ProjectSelector(project="chunky"), chunk_size=3)
    assert affected == 7
    assert len(calls) == 3  # ceil(7 / 3)


def test_reprocess_returned_count_matches_rows_affected(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(4):
        _insert_wheel(conn, filename=f"count-{i}-1.0-py3-none-any.whl", project="counted")
    _insert_wheel(conn, filename="other-1.0-py3-none-any.whl", project="different")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(ProjectSelector(project="counted"))
    assert affected == 4


def test_reprocess_selector_matching_nothing_returns_zero_and_writes_nothing(
    db_path, reader, writer
):
    dispatcher = _dispatcher(reader, writer)
    calls = []
    real_submit_and_wait = writer.submit_and_wait

    def _counting_submit_and_wait(op):
        calls.append(op)
        return real_submit_and_wait(op)

    writer.submit_and_wait = _counting_submit_and_wait
    affected = dispatcher.reprocess(ProjectSelector(project="does-not-exist"))
    assert affected == 0
    assert calls == []


def test_skipped_only_selector_resets_skipped_wheels(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="skippedonly-1.0-py3-none-any.whl", state=WheelState.SKIPPED
    )
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(SkippedOnly())
    assert affected == 1
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.NEED_CONVERT)


def test_state_selector_paginates_across_multiple_full_chunks(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_ids = [
        _insert_wheel(
            conn, filename=f"statepage-{i}-1.0-py3-none-any.whl", state=WheelState.QUARANTINED
        )
        for i in range(5)
    ]
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(StateSelector(state=WheelState.QUARANTINED), chunk_size=2)
    assert affected == 5
    for wheel_id in wheel_ids:
        row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
        assert row[0] == int(WheelState.NEED_CONVERT)


def test_reroll_version_below_skips_side_paginates_across_multiple_full_chunks(
    db_path, reader, writer
):
    conn = sqlite3.connect(db_path)
    wheel_ids = []
    for i in range(3):
        wheel_id = _insert_wheel(
            conn, filename=f"skipspage-{i}-1.0-py3-none-any.whl", state=WheelState.SKIPPED
        )
        conn.execute(
            "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
            "VALUES (?, 'convert', 'r', 0, '1.0.0', '2024-01-01T00:00:00+00:00')",
            (wheel_id,),
        )
        wheel_ids.append(wheel_id)
    conn.commit()
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(RerollVersionBelow(version="2.0.0"), chunk_size=1)
    assert affected == 3
    for wheel_id in wheel_ids:
        row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
        assert row[0] == int(WheelState.NEED_CONVERT)


def test_project_selector_never_resurrects_a_deleted_wheel(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="tombstone-1.0-py3-none-any.whl", project="tomb", state=WheelState.DELETED
    )
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(ProjectSelector(project="tomb"))
    assert affected == 0
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.DELETED)


def test_state_selector_targeting_deleted_is_a_noop(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="tombstone2-1.0-py3-none-any.whl", state=WheelState.DELETED
    )
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(StateSelector(state=WheelState.DELETED))
    assert affected == 0
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.DELETED)


def test_reroll_version_below_never_resurrects_a_deleted_wheel(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="tombstone3-1.0-py3-none-any.whl", state=WheelState.DELETED
    )
    conn.execute(
        "INSERT INTO wheel_repodata (wheel_id, repodata_zst, reroll_version) VALUES (?, ?, ?)",
        (wheel_id, b"blob", "1.0.0"),
    )
    conn.commit()
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(RerollVersionBelow(version="2.0.0"))
    assert affected == 0
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.DELETED)
    repodata_row = reader.execute(
        "SELECT COUNT(*) FROM wheel_repodata WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert repodata_row[0] == 1


def test_skipped_only_cannot_match_a_deleted_wheel(db_path, reader, writer):
    # A DELETED wheel can never also be in state SKIPPED, so this is a
    # sanity check that the invariant holds even without touching this
    # selector's query -- documented here rather than left implicit.
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(
        conn, filename="tombstone4-1.0-py3-none-any.whl", state=WheelState.DELETED
    )
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    affected = dispatcher.reprocess(SkippedOnly())
    assert affected == 0
    wheel_row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert wheel_row[0] == int(WheelState.DELETED)


def test_reprocess_raises_on_unsupported_selector(db_path, reader, writer):
    from typing import cast

    from reroll_sync.dispatcher import Selector

    dispatcher = _dispatcher(reader, writer)
    with pytest.raises(TypeError):
        dispatcher.reprocess(cast(Selector, object()))


def test_project_selector_chunk_query_plan_has_no_scan(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(20):
        _insert_wheel(conn, filename=f"projplan-{i}-1.0-py3-none-any.whl", project="planproj")
    conn.close()
    from reroll_sync.dispatcher import _project_chunk_query

    sql, params = _project_chunk_query("planproj", 0, 10)
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_project" in d for d in details)
    assert not any("SCAN" in d for d in details)
    assert not any("TEMP B-TREE" in d for d in details)


def test_state_selector_chunk_query_plan_has_no_scan(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(20):
        _insert_wheel(
            conn, filename=f"stateplan-{i}-1.0-py3-none-any.whl", state=WheelState.QUARANTINED
        )
    conn.close()
    from reroll_sync.dispatcher import _state_chunk_query
    from reroll_sync.schema import WheelState as WS

    sql, params = _state_chunk_query(WS.QUARANTINED, None, 10)
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN" in d for d in details)
    assert not any("TEMP B-TREE" in d for d in details)

    sql, params = _state_chunk_query(WS.QUARANTINED, (0, "proj", 5), 10)
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN" in d for d in details)
    assert not any("TEMP B-TREE" in d for d in details)


def test_skipped_only_reuses_the_state_chunk_query(db_path, reader, writer):
    # SkippedOnly has no fields of its own to plug into _state_chunk_query
    # directly; it's exercised through reprocess() (see
    # test_skipped_only_selector_resets_skipped_wheels), which routes it
    # through the very query plan-tested above via WheelState.SKIPPED.
    from reroll_sync.dispatcher import _state_chunk_query
    from reroll_sync.schema import WheelState as WS

    sql, params = _state_chunk_query(WS.SKIPPED, None, 10)
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in d for d in details)
    assert not any("SCAN" in d for d in details)


def test_reroll_version_below_chunk_query_plans_have_no_scan(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(20):
        wheel_id = _insert_wheel(
            conn, filename=f"rvplan-{i}-1.0-py3-none-any.whl", state=WheelState.READY
        )
        conn.execute(
            "INSERT INTO wheel_repodata (wheel_id, repodata_zst, reroll_version) VALUES (?, ?, ?)",
            (wheel_id, b"blob", "1.0.0"),
        )
        conn.execute(
            "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
            "VALUES (?, 'fetch', 'r', 0, '1.0.0', '2024-01-01T00:00:00+00:00')",
            (wheel_id,),
        )
    conn.commit()
    conn.close()
    from reroll_sync.dispatcher import (
        _reroll_version_skips_chunk_query,
        _reroll_version_wheel_repodata_chunk_query,
    )

    sql, params = _reroll_version_wheel_repodata_chunk_query("2.0.0", 10)
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_wheel_repodata_version" in d for d in details)
    assert not any("SCAN" in d for d in details)
    assert not any("TEMP B-TREE" in d for d in details)
    assert not any("MULTI-INDEX OR" in d for d in details)

    sql, params = _reroll_version_skips_chunk_query("2.0.0", 10)
    plan = reader.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_skips_retryable" in d for d in details)
    assert not any("SCAN" in d for d in details)
    assert not any("TEMP B-TREE" in d for d in details)
    assert not any("MULTI-INDEX OR" in d for d in details)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def test_metrics_queries_plan_has_no_scan(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(20):
        _insert_wheel(conn, filename=f"metricsplan-{i}-1.0-py3-none-any.whl")
    conn.close()
    for sql in (
        "SELECT COUNT(*) FROM wheels WHERE state = ?",
        "SELECT MIN(updated_at) FROM wheels WHERE state = ?",
    ):
        plan = reader.execute(
            f"EXPLAIN QUERY PLAN {sql}", (int(WheelState.NEED_CONVERT),)
        ).fetchall()
        details = [row[-1] for row in plan]
        assert any("ix_wheels_queue" in d for d in details)
        assert not any("SCAN" in d for d in details)


def test_queue_depth_matches_hand_counted_expectation(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(4):
        _insert_wheel(conn, filename=f"depth-{i}-1.0-py3-none-any.whl")
    _insert_wheel(conn, filename="other-state-1.0-py3-none-any.whl", state=WheelState.READY)
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    metrics = dispatcher.metrics(Stage.CONVERT)
    assert metrics.queue_depth == 4


def test_oldest_pending_age_uses_updated_at_and_none_on_empty_queue(db_path, reader, writer):
    dispatcher = _dispatcher(reader, writer)
    assert dispatcher.metrics(Stage.CONVERT).oldest_pending_age is None

    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO wheels "
        "(filename, project, state, lane, url, serial, change_seq, updated_at) "
        "VALUES ('aged-1.0-py3-none-any.whl', 'proj', ?, 0, 'https://example.test/x', 1, 1, ?)",
        (int(WheelState.NEED_CONVERT), "1970-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()
    clock = FakeClock(start=100.0)
    dispatcher2 = _dispatcher(reader, writer, clock=clock)
    age = dispatcher2.metrics(Stage.CONVERT).oldest_pending_age
    assert age == pytest.approx(100.0)


def test_outcome_counters_increment_per_kind(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    ok_id = _insert_wheel(conn, filename="metric-ok-1.0-py3-none-any.whl")
    skip_id = _insert_wheel(conn, filename="metric-skip-1.0-py3-none-any.whl")
    retry_id = _insert_wheel(conn, filename="metric-retry-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)

    ok_outcome = adapt_convert_outcome(
        ConvertOk(
            records=(_record(),), resolutions=(), conda_name="example", requires_prerelease=False
        ),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(ok_id), ok_outcome)

    skip_outcome = adapt_convert_outcome(
        ConvertSkip(reason="r", subcategory="s", details="d", permanent=True, reroll_version=None),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(skip_id), skip_outcome)

    retry_outcome = adapt_convert_outcome(
        ConvertRetry(reason="r", details="d"), reroll_version="1.2.3"
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(retry_id), retry_outcome)

    metrics = dispatcher.metrics(Stage.CONVERT)
    assert metrics.outcome_counts["ok"] == 1
    assert metrics.outcome_counts["skip"] == 1
    assert metrics.outcome_counts["retry"] == 1
    assert metrics.retry_count == 1
    assert metrics.quarantine_count == 0


def test_quarantine_counter_increments_separately_from_retry_counter(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="quarmetric-1.0-py3-none-any.whl")
    _insert_work(conn, wheel_id=wheel_id, attempts=DEFAULT_MAX_ATTEMPTS)
    conn.close()
    dispatcher = _dispatcher(reader, writer, rng=random.Random(1))
    outcome = adapt_convert_outcome(ConvertRetry(reason="r", details="d"), reroll_version="1.2.3")
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), outcome)
    metrics = dispatcher.metrics(Stage.CONVERT)
    assert metrics.quarantine_count == 1
    assert metrics.retry_count == 0


def test_rate_limited_outcome_is_counted_separately(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    wheel_id = _insert_wheel(conn, filename="ratemetric-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    dispatcher.apply_outcome(Stage.CONVERT, _item(wheel_id), RateLimited(child="x", seconds=1.0))
    metrics = dispatcher.metrics(Stage.CONVERT)
    assert metrics.outcome_counts["rate_limited"] == 1


def test_throughput_ema_becomes_positive_after_two_completions_with_elapsed_time(
    db_path, reader, writer
):
    conn = sqlite3.connect(db_path)
    a = _insert_wheel(conn, filename="throughput-a-1.0-py3-none-any.whl")
    b = _insert_wheel(conn, filename="throughput-b-1.0-py3-none-any.whl")
    conn.close()
    clock = FakeClock()
    dispatcher = _dispatcher(reader, writer, clock=clock)
    assert dispatcher.metrics(Stage.CONVERT).throughput_ema == 0.0
    outcome = adapt_convert_outcome(
        ConvertOk(
            records=(_record(),), resolutions=(), conda_name="example", requires_prerelease=False
        ),
        reroll_version="1.2.3",
    )
    dispatcher.apply_outcome(Stage.CONVERT, _item(a), outcome)
    clock.advance(1.0)
    dispatcher.apply_outcome(Stage.CONVERT, _item(b), outcome)
    assert dispatcher.metrics(Stage.CONVERT).throughput_ema > 0.0


def test_in_flight_count_reflects_claimed_but_unresolved_items(db_path, reader, writer):
    conn = sqlite3.connect(db_path)
    for i in range(3):
        _insert_wheel(conn, filename=f"inflightmetric-{i}-1.0-py3-none-any.whl")
    conn.close()
    dispatcher = _dispatcher(reader, writer)
    dispatcher.claim(Stage.CONVERT, limit=2)
    metrics = dispatcher.metrics(Stage.CONVERT)
    assert metrics.in_flight == 2


# ---------------------------------------------------------------------------
# Acceptance: no silent-swallow patterns, no direct writer thread violations
# ---------------------------------------------------------------------------


def test_dispatcher_module_has_no_silent_swallow_pattern():
    from pathlib import Path

    from reroll_sync import dispatcher as dispatcher_module

    assert dispatcher_module.__file__ is not None
    text = Path(dispatcher_module.__file__).read_text()
    assert "except OSError: continue" not in text
    assert "except Exception: continue" not in text


def test_dispatcher_module_never_calls_commit_rollback_or_begin():
    import re
    from pathlib import Path

    from reroll_sync import dispatcher as dispatcher_module

    assert dispatcher_module.__file__ is not None
    text = Path(dispatcher_module.__file__).read_text()
    pattern = re.compile(
        r"\.commit\s*\(|\.rollback\s*\(|execute\s*\(\s*[\"']\s*BEGIN", re.IGNORECASE
    )
    assert not pattern.search(text)
