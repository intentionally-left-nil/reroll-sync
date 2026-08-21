"""Tests for the convert stage: fed by both the fetch/archive handoff and
bulk reads of already-archived-but-unconverted wheels, over a worker pool.

The archive-handoff path is the one with a real integration hazard: the
`QueueItem` `ArchiveHandoff.on_archived` hands back still carries the
*fetch* claim's stale `state=NEED_METADATA`, even though the wheel's actual
state was just written as `NEED_CONVERT`. Applying a convert outcome with
that stale state would raise `IllegalTransitionError` (`ALLOWED_TRANSITIONS`
has no `NEED_METADATA -> READY`/`SKIPPED` edge), so `ConvertStage.on_archived`
rebuilds a fresh `QueueItem` with the correct current state instead of
passing the stale one straight through.
"""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from reroll.name_mapping import passthrough_mapper

from reroll_sync.archive.store import ArchiveStore
from reroll_sync.convert import worker_init
from reroll_sync.daemon.stages.convert import ConvertStage
from reroll_sync.db import init_db
from reroll_sync.dispatcher import Dispatcher, QueueItem, Stage
from reroll_sync.fetch import ArchiveHandoff, BulkConvertSource, ByteBudgetedQueue, HandoffItem
from reroll_sync.schema import WheelState
from reroll_sync.version import REROLL_VERSION
from reroll_sync.writer import Writer

_E2E_MAPPERS = (passthrough_mapper,)


def _writer_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "convert_stage.db")
    init_db(path)
    return path


@pytest.fixture
def writer(db_path):
    conn = _writer_conn(db_path)
    w = Writer(conn, batch_size=1, batch_interval=1_000_000.0)
    w.start()
    yield w
    if not w._stopped:
        w.stop(drain=False)


@pytest.fixture
def reader(db_path):
    conn = sqlite3.connect(str(db_path))
    yield conn
    conn.close()


@pytest.fixture
def dispatcher(reader, writer):
    return Dispatcher(reader, writer, reroll_version="1.0", now=lambda: 1_700_000_000.0)


@pytest.fixture
def store(tmp_path, db_path):
    conn = _writer_conn(db_path)
    s = ArchiveStore(tmp_path / "segments", conn)
    yield s
    if s._current_writer is not None and not s._current_writer._file.closed:
        s._current_writer._file.close()
    conn.close()


@pytest.fixture(autouse=True)
def _worker_init(monkeypatch):
    # `worker_init` normally calls `reroll.default_mappers()`, which
    # reloads network-backed lookup tables; stub it with the same
    # passthrough mapper `test_fetch.py`'s own end-to-end tests use, so
    # this test module never touches the network.
    monkeypatch.setattr("reroll_sync.convert.reroll.default_mappers", lambda: _E2E_MAPPERS)
    worker_init(REROLL_VERSION)


@pytest.fixture
def pool():
    executor = ThreadPoolExecutor(max_workers=2)
    yield executor
    executor.shutdown(wait=True)


def _insert_wheel(
    db_path: str,
    *,
    filename: str,
    project: str = "widget",
    state: WheelState = WheelState.NEED_CONVERT,
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, serial, change_seq, updated_at) "
            "VALUES (?, ?, ?, 0, 'https://example.test/x', 1, 1, '2024-01-01T00:00:00+00:00')",
            (filename, project, int(state)),
        )
        conn.commit()
        assert cursor.lastrowid is not None
        return cursor.lastrowid
    finally:
        conn.close()


def _metadata_text(name: str = "example", version: str = "1.0") -> str:
    return f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"


def _stage(dispatcher, reader, store, pool, *, limit=10, on_applied=None) -> ConvertStage:
    bulk_source = BulkConvertSource(store, dispatcher, reader)
    return ConvertStage(
        dispatcher,
        bulk_source,
        pool,
        reroll_version=REROLL_VERSION,
        limit=limit,
        on_applied=on_applied,
    )


def test_empty_queues_report_no_work(dispatcher, reader, store, pool):
    stage = _stage(dispatcher, reader, store, pool)
    assert stage.iterate() is False


def test_bulk_claim_converts_and_transitions_to_ready(db_path, reader, dispatcher, store, pool):
    wheel_id = _insert_wheel(db_path, filename="example-1.0-py3-none-any.whl")
    location = store.add(_metadata_text().encode())
    store.seal_writer(store.current_writer())  # BulkConvertSource only reads sealed segments
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (location.sha256, wheel_id))
    conn.commit()
    conn.close()

    stage = _stage(dispatcher, reader, store, pool)
    did_work = stage.iterate()

    assert did_work is True
    row = reader.execute(
        "SELECT state, conda_name FROM wheels WHERE id = ?", (wheel_id,)
    ).fetchone()
    assert row[0] == int(WheelState.READY)
    assert row[1] == "example"


def test_fed_queue_is_drained_before_bulk_claim(db_path, reader, dispatcher, store, pool):
    """A wheel handed off directly from `on_archived` must convert even
    though it was never claimed via `Dispatcher.claim(Stage.CONVERT, ...)`
    at all -- the fed-queue path bypasses bulk claiming entirely.
    """
    wheel_id = _insert_wheel(
        db_path, filename="example-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA
    )
    # Mirrors what ArchiveHandoff hands `on_archived`: a QueueItem carrying
    # the *fetch* claim's now-stale state, since ArchiveHandoff itself only
    # touches Stage.FETCH's dispatcher bookkeeping.
    stale_queue_item = QueueItem(
        id=wheel_id, project="widget", lane=0, state=WheelState.NEED_METADATA
    )
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE wheels SET state = ? WHERE id = ?", (int(WheelState.NEED_CONVERT), wheel_id)
    )
    conn.commit()
    conn.close()

    stage = _stage(dispatcher, reader, store, pool)
    stage.on_archived(stale_queue_item, "example-1.0-py3-none-any.whl", _metadata_text().encode())

    did_work = stage.iterate()

    assert did_work is True
    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.READY)


def test_end_to_end_through_a_real_archive_handoff(
    db_path, reader, writer, dispatcher, store, pool
):
    """The real integration path: fetch claims, archives, hands off to
    convert via `on_archived` -- proving the stale-state hazard is actually
    avoided when wired through `ArchiveHandoff`, not just in an isolated unit
    test with a hand-built stale `QueueItem`.
    """
    wheel_id = _insert_wheel(
        db_path, filename="example-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA
    )
    fetch_item = dispatcher.claim(Stage.FETCH, 10)[0]
    assert fetch_item.id == wheel_id

    handoff_queue = ByteBudgetedQueue(budget_bytes=10_000)
    stage = _stage(dispatcher, reader, store, pool)
    handoff = ArchiveHandoff(handoff_queue, store, dispatcher, writer, stage.on_archived)

    handoff_queue.put(
        HandoffItem(
            queue_item=fetch_item,
            filename="example-1.0-py3-none-any.whl",
            data=_metadata_text().encode(),
            sha256="unused-in-this-path",
        ),
        size=len(_metadata_text().encode()),
    )
    handoff.process_one()

    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.NEED_CONVERT)

    did_work = stage.iterate()

    assert did_work is True
    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.READY)


def test_iterate_processes_multiple_fed_items(db_path, reader, dispatcher, store, pool):
    ids = []
    for i in range(3):
        wheel_id = _insert_wheel(
            db_path, filename=f"example{i}-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT
        )
        ids.append(wheel_id)

    stage = _stage(dispatcher, reader, store, pool)
    for i, wheel_id in enumerate(ids):
        item = QueueItem(id=wheel_id, project="widget", lane=0, state=WheelState.NEED_CONVERT)
        stage.on_archived(
            item, f"example{i}-1.0-py3-none-any.whl", _metadata_text(name=f"example{i}").encode()
        )

    did_work = stage.iterate()

    assert did_work is True
    for wheel_id in ids:
        row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
        assert row[0] == int(WheelState.READY)


def test_fed_queue_limit_caps_batch_size(db_path, reader, dispatcher, store, pool):
    ids = []
    for i in range(3):
        wheel_id = _insert_wheel(
            db_path, filename=f"example{i}-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT
        )
        ids.append(wheel_id)

    stage = _stage(dispatcher, reader, store, pool, limit=2)
    for i, wheel_id in enumerate(ids):
        item = QueueItem(id=wheel_id, project="widget", lane=0, state=WheelState.NEED_CONVERT)
        stage.on_archived(
            item, f"example{i}-1.0-py3-none-any.whl", _metadata_text(name=f"example{i}").encode()
        )

    stage.iterate()

    ready_count = reader.execute(
        "SELECT COUNT(*) FROM wheels WHERE state = ?", (int(WheelState.READY),)
    ).fetchone()[0]
    assert ready_count == 2


def test_on_applied_hook_is_called_once_per_applied_outcome(
    db_path, reader, dispatcher, store, pool
):
    """The `on_applied` hook exists to let a test synchronize on "this
    outcome is now committed" deterministically instead of polling for a
    DB state change -- confirms it actually fires, with the right
    arguments, once per item.
    """
    wheel_id = _insert_wheel(db_path, filename="example-1.0-py3-none-any.whl")
    location = store.add(_metadata_text().encode())
    store.seal_writer(store.current_writer())
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (location.sha256, wheel_id))
    conn.commit()
    conn.close()

    applied_calls = []
    stage = _stage(
        dispatcher,
        reader,
        store,
        pool,
        on_applied=lambda item, outcome: applied_calls.append((item, outcome)),
    )

    did_work = stage.iterate()

    assert did_work is True
    assert len(applied_calls) == 1
    applied_item, _applied_outcome = applied_calls[0]
    assert applied_item.id == wheel_id


def test_on_applied_hook_defaults_to_none_and_is_not_required(
    db_path, reader, dispatcher, store, pool
):
    wheel_id = _insert_wheel(db_path, filename="example-1.0-py3-none-any.whl")
    location = store.add(_metadata_text().encode())
    store.seal_writer(store.current_writer())
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (location.sha256, wheel_id))
    conn.commit()
    conn.close()

    stage = _stage(dispatcher, reader, store, pool)  # on_applied left at its default of None

    did_work = stage.iterate()

    assert did_work is True
