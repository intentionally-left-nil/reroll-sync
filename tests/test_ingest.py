"""Tests for index polling, project reconciliation, and their write-side effects."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Sequence
from typing import Any
from unittest import mock

import httpx
import pytest

from reroll_sync.db import init_db
from reroll_sync.dispatcher import DEFAULT_MAX_ATTEMPTS
from reroll_sync.ingest import (
    NO_SIDECAR_SKIP_REASON,
    IngestSummary,
    ProjectBackoff,
    SyncGone,
    SyncOk,
    SyncRateLimited,
    SyncRetry,
    _diff_common,
    _LocalWheelRow,
    _plan_new_wheel,
    _read_local_serials,
    _stale_projects,
    apply_project_outcome,
    ingest_stale_projects,
    poll_index,
    sync_project,
)
from reroll_sync.pypi_client import ACCEPT_HEADER, IndexProject, ProjectFile, PyPIClient
from reroll_sync.schema import WheelState
from reroll_sync.writer import ReadTxnWatchdog, Writer, read_txn

_USER_AGENT = "reroll-sync-test (contact@example.invalid)"

# ---------------------------------------------------------------------------
# Fixtures and small helpers
# ---------------------------------------------------------------------------


class _FakeLimiter:
    """A minimal stand-in for HierarchicalLimiter: acquire always grants, penalize records."""

    def __init__(self, hosts: frozenset[str] = frozenset({"pypi.org"})) -> None:
        self._hosts = hosts
        self.penalized: list[tuple[str, float]] = []

    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        if child_name not in self._hosts:
            raise KeyError(child_name)
        return True

    def penalize(self, child_name: str, seconds: float) -> None:
        self.penalized.append((child_name, seconds))


def _make_client(handler, *, limiter: _FakeLimiter | None = None, **kwargs) -> PyPIClient:
    transport = httpx.MockTransport(handler)
    limiter = limiter if limiter is not None else _FakeLimiter()
    return PyPIClient(limiter, user_agent=_USER_AGENT, transport=transport, **kwargs)


def _index_payload(*, last_serial: int = 1, projects: Sequence[dict] = ()) -> dict:
    return {"meta": {"_last-serial": last_serial, "api-version": "1.4"}, "projects": list(projects)}


def _project_payload(*, last_serial: int = 1, files: Sequence[dict] = ()) -> dict:
    return {"meta": {"_last-serial": last_serial, "api-version": "1.4"}, "files": list(files)}


def _file(
    filename: str,
    *,
    url: str | None = None,
    sha256: str | None = None,
    core_metadata: bool | dict = False,
    size: int | None = None,
    upload_time: str | None = None,
    requires_python: str | None = None,
    yanked: bool | str = False,
) -> dict:
    entry: dict = {"filename": filename, "url": url or f"https://files.pythonhosted.org/{filename}"}
    if sha256 is not None:
        entry["hashes"] = {"sha256": sha256}
    if core_metadata is not False:
        entry["core-metadata"] = core_metadata
    if size is not None:
        entry["size"] = size
    if upload_time is not None:
        entry["upload-time"] = upload_time
    if requires_python is not None:
        entry["requires-python"] = requires_python
    if yanked is not False:
        entry["yanked"] = yanked
    return entry


def _json_handler(payload: dict, *, status: int = 200, headers: dict | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            headers={"content-type": ACCEPT_HEADER, **(headers or {})},
            json=payload,
            request=request,
        )

    return handler


def _status_handler(status: int, *, headers: dict | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, headers=headers or {}, content=b"", request=request)

    return handler


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "ingest.db")
    init_db(path)
    return path


def _writer_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 50")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


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
def writer(db_path, writers) -> Writer:
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
    state: WheelState = WheelState.NEED_METADATA,
    url: str | None = None,
    wheel_sha256: str | None = None,
    metadata_sha256: str | None = None,
    size: int | None = None,
    upload_time: str | None = None,
    requires_python: str | None = None,
    yanked: bool = False,
    yanked_reason: str | None = None,
    blob_sha256: str | None = None,
    serial: int = 1,
    change_seq: int = 1,
    deleted_at: str | None = None,
    updated_at: str = "2024-01-01T00:00:00+00:00",
) -> int:
    cursor = conn.execute(
        "INSERT INTO wheels (filename, project, state, lane, url, wheel_sha256, "
        "metadata_sha256, size, upload_time, requires_python, yanked, yanked_reason, "
        "blob_sha256, serial, change_seq, deleted_at, updated_at) "
        "VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            filename,
            project,
            int(state),
            url or f"https://files.pythonhosted.org/{filename}",
            wheel_sha256,
            metadata_sha256,
            size,
            upload_time,
            requires_python,
            int(yanked),
            yanked_reason,
            blob_sha256,
            serial,
            change_seq,
            deleted_at,
            updated_at,
        ),
    )
    conn.commit()
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _wheel_row(conn: sqlite3.Connection, filename: str) -> tuple[Any, ...]:
    row = conn.execute(
        "SELECT id, state, url, wheel_sha256, metadata_sha256, size, upload_time, "
        "requires_python, yanked, yanked_reason, blob_sha256, serial, change_seq, "
        "deleted_at, updated_at, lane FROM wheels WHERE filename = ?",
        (filename,),
    ).fetchone()
    assert row is not None, f"no wheels row for {filename!r}"
    return row


def _pypi_index_row(conn: sqlite3.Connection, name: str) -> tuple[Any, ...] | None:
    return conn.execute(
        "SELECT serial, updated_at FROM pypi_index WHERE name = ?", (name,)
    ).fetchone()


def _skips_rows(conn: sqlite3.Connection, wheel_id: int) -> list[tuple]:
    return conn.execute(
        "SELECT stage, reason, permanent FROM skips WHERE wheel_id = ?", (wheel_id,)
    ).fetchall()


def _errors_rows(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT wheel_id, error_category, error_subcat, details FROM errors"
    ).fetchall()


def _now() -> float:
    return 1_700_000_000.0


# ---------------------------------------------------------------------------
# Poll
# ---------------------------------------------------------------------------


def test_poll_not_modified_returns_zero_write_ops(reader):
    client = _make_client(_status_handler(304, headers={"etag": '"abc"'}))

    result = poll_index(client, reader, etag='"old"')

    assert result.not_modified is True
    assert result.etag == '"abc"'
    assert result.remote_global_serial is None
    assert result.stale_projects == ()


def test_poll_stale_when_absent_locally(reader):
    index = _index_payload(last_serial=10, projects=[{"name": "numpy", "_last-serial": 5}])
    client = _make_client(_json_handler(index))

    result = poll_index(client, reader, etag=None)

    assert result.not_modified is False
    assert result.stale_projects == ("numpy",)
    assert result.remote_global_serial == 10


def test_poll_stale_when_remote_serial_higher(reader):
    reader.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 5, "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    index = _index_payload(last_serial=99, projects=[{"name": "numpy", "_last-serial": 10}])
    client = _make_client(_json_handler(index))

    result = poll_index(client, reader, etag=None)

    assert result.stale_projects == ("numpy",)


def test_poll_not_stale_when_remote_serial_equal(reader):
    reader.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 10, "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    index = _index_payload(last_serial=99, projects=[{"name": "numpy", "_last-serial": 10}])
    client = _make_client(_json_handler(index))

    result = poll_index(client, reader, etag=None)

    assert result.stale_projects == ()


def test_poll_not_stale_when_remote_serial_lower_and_logs_warning(reader, caplog):
    reader.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 50, "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    index = _index_payload(last_serial=99, projects=[{"name": "numpy", "_last-serial": 10}])
    client = _make_client(_json_handler(index))

    with caplog.at_level("WARNING", logger="reroll_sync.ingest"):
        result = poll_index(client, reader, etag=None)

    assert result.stale_projects == ()
    assert "lower than stored serial" in caplog.text


def test_poll_etag_persisted_across_calls(reader):
    seen_headers: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers.get("if-none-match"))
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER, "etag": '"v2"'},
            json=_index_payload(last_serial=1),
            request=request,
        )

    client = _make_client(handler)
    first = poll_index(client, reader, etag=None)
    poll_index(client, reader, etag=first.etag)

    assert first.etag == '"v2"'
    assert seen_headers == [None, '"v2"']


def test_poll_local_serial_map_read_in_exactly_three_chunks(reader):
    for i in range(250):
        reader.execute(
            "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
            (f"pkg-{i:04d}", 1, "2024-01-01T00:00:00+00:00"),
        )
    reader.commit()
    client = _make_client(_json_handler(_index_payload(last_serial=1)))

    with mock.patch("reroll_sync.ingest.read_txn", wraps=read_txn) as spy:
        poll_index(client, reader, etag=None, chunk_size=100)

    serial_map_calls = [
        call
        for call in spy.call_args_list
        if call.kwargs.get("label") == "ingest.poll_index.serial_map"
    ]
    assert len(serial_map_calls) == 3


def test_poll_no_read_transaction_exceeds_watchdog_budget(reader):
    for i in range(10):
        reader.execute(
            "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
            (f"pkg-{i}", 1, "2024-01-01T00:00:00+00:00"),
        )
    reader.commit()
    client = _make_client(_json_handler(_index_payload(last_serial=1)))
    watchdog = ReadTxnWatchdog()

    poll_index(client, reader, etag=None, watchdog=watchdog)

    assert watchdog.snapshot().over_budget_count == 0


def test_poll_fast_path_missed_logs_when_serial_unchanged(reader, caplog):
    reader.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 99, "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    index = _index_payload(last_serial=99, projects=[{"name": "numpy", "_last-serial": 99}])
    client = _make_client(_json_handler(index))

    with caplog.at_level("INFO", logger="reroll_sync.ingest"):
        poll_index(client, reader, etag=None)

    assert "fast path missed" in caplog.text


# ---------------------------------------------------------------------------
# _stale_projects / _read_local_serials (direct unit tests for branch coverage)
# ---------------------------------------------------------------------------


def test_stale_projects_absent_locally_is_stale():
    result = _stale_projects((IndexProject(name="a", serial=1),), {})
    assert result == ("a",)


def test_stale_projects_equal_serial_not_stale():
    result = _stale_projects((IndexProject(name="a", serial=5),), {"a": 5})
    assert result == ()


def test_read_local_serials_empty_table_returns_empty_dict(reader):
    assert _read_local_serials(reader, chunk_size=100, budget=0.25, watchdog=None) == {}


# ---------------------------------------------------------------------------
# New files
# ---------------------------------------------------------------------------


def test_new_whl_with_metadata_true_inserted_as_need_metadata(reader, writer):
    page = _project_payload(
        last_serial=1, files=[_file("pkg-1.0-py3-none-any.whl", core_metadata=True)]
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "pkg", now=_now)
    apply_project_outcome(writer, None, "pkg", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row is not None
    assert row[1] == int(WheelState.NEED_METADATA)


def test_new_whl_with_metadata_false_inserted_as_no_metadata(reader, writer):
    page = _project_payload(last_serial=1, files=[_file("pkg-1.0-py3-none-any.whl")])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "pkg", now=_now)
    apply_project_outcome(writer, None, "pkg", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NO_METADATA)


def test_non_whl_files_are_not_inserted(reader, writer):
    page = _project_payload(
        last_serial=1,
        files=[
            _file("pkg-1.0.tar.gz"),
            _file("pkg-1.0-py3-none-any.whl"),
            _file("pkg-1.0.zip"),
            _file("pkg-1.0.egg"),
        ],
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "pkg", now=_now)
    apply_project_outcome(writer, None, "pkg", outcome, now=_now)

    rows = reader.execute("SELECT filename FROM wheels WHERE project = ?", ("pkg",)).fetchall()
    assert [r[0] for r in rows] == ["pkg-1.0-py3-none-any.whl"]


def test_new_wheel_every_normalized_column_is_populated(reader, writer):
    page = _project_payload(
        last_serial=7,
        files=[
            _file(
                "pkg-1.0-py3-none-any.whl",
                sha256="wheelhash",
                core_metadata={"sha256": "metahash"},
                size=1234,
                upload_time="2024-05-01T00:00:00Z",
                requires_python=">=3.9",
                yanked="bad release",
            )
        ],
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "pkg", now=_now)
    apply_project_outcome(writer, None, "pkg", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    (
        _id,
        state,
        url,
        wheel_sha256,
        metadata_sha256,
        size,
        upload_time,
        requires_python,
        yanked,
        yanked_reason,
        blob_sha256,
        serial,
        _change_seq,
        deleted_at,
        _updated_at,
        lane,
    ) = row
    assert state == int(WheelState.NEED_METADATA)
    assert url == "https://files.pythonhosted.org/pkg-1.0-py3-none-any.whl"
    assert wheel_sha256 == "wheelhash"
    assert metadata_sha256 == "metahash"
    assert size == 1234
    assert upload_time == "2024-05-01T00:00:00Z"
    assert requires_python == ">=3.9"
    assert yanked == 1
    assert yanked_reason == "bad release"
    assert blob_sha256 is None
    assert serial == 7
    assert deleted_at is None
    assert lane == 0


def test_duplicate_filename_across_projects_does_not_crash_and_is_recorded(reader, writer):
    other_id = _insert_wheel(reader, filename="dup-1.0-py3-none-any.whl", project="other")
    page = _project_payload(
        last_serial=1,
        files=[_file("dup-1.0-py3-none-any.whl"), _file("ok-1.0-py3-none-any.whl")],
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert result is not None
    assert result.inserted == 1
    assert (
        reader.execute(
            "SELECT COUNT(*) FROM wheels WHERE filename = ?", ("dup-1.0-py3-none-any.whl",)
        ).fetchone()[0]
        == 1
    )
    assert _wheel_row(reader, "ok-1.0-py3-none-any.whl") is not None
    errors = _errors_rows(reader)
    assert len(errors) == 1
    assert errors[0][0] == other_id
    assert errors[0][1] == "duplicate_filename"
    assert errors[0][2] == "proj"
    assert _pypi_index_row(reader, "proj") is not None


def test_new_wheel_in_unlinked_blobs_is_need_convert_and_row_deleted(reader, writer):
    reader.execute(
        "INSERT INTO unlinked_blobs (filename, sha256, noted_at) VALUES (?, ?, ?)",
        ("pkg-1.0-py3-none-any.whl", "archivedhash", "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    page = _project_payload(last_serial=1, files=[_file("pkg-1.0-py3-none-any.whl")])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "pkg", now=_now)
    apply_project_outcome(writer, None, "pkg", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_CONVERT)
    assert row[10] == "archivedhash"
    assert (
        reader.execute(
            "SELECT COUNT(*) FROM unlinked_blobs WHERE filename = ?", ("pkg-1.0-py3-none-any.whl",)
        ).fetchone()[0]
        == 0
    )


def test_new_wheel_not_in_unlinked_blobs_is_unaffected(reader, writer):
    page = _project_payload(last_serial=1, files=[_file("pkg-1.0-py3-none-any.whl")])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "pkg", now=_now)
    apply_project_outcome(writer, None, "pkg", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NO_METADATA)
    assert row[10] is None


def test_unlinked_blobs_lookup_is_a_primary_key_hit(reader):
    plan = reader.execute(
        "EXPLAIN QUERY PLAN SELECT filename, sha256 FROM unlinked_blobs WHERE filename IN (?, ?)",
        ("a", "b"),
    ).fetchall()
    plan_text = " ".join(str(row[-1]) for row in plan)
    assert "SCAN" not in plan_text
    assert "unlinked_blobs" in plan_text


# ---------------------------------------------------------------------------
# Changed files
# ---------------------------------------------------------------------------


def test_yank_flip_updates_yanked_columns_bumps_seq_leaves_state_unchanged(reader, writer):
    wheel_id = _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.READY,
        yanked=False,
        change_seq=1,
    )
    page = _project_payload(
        last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl", yanked="security issue")]
    )
    client = _make_client(_json_handler(page))
    seq_before = writer.current_seq()

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[0] == wheel_id
    assert row[1] == int(WheelState.READY), "state must be unchanged by a yank flip"
    assert row[8] == 1
    assert row[9] == "security issue"
    assert row[12] > seq_before, "change_seq must bump"


def test_unyank_flips_it_back(reader, writer):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.READY,
        yanked=True,
        yanked_reason="old reason",
    )
    page = _project_payload(last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl", yanked=False)])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.READY)
    assert row[8] == 0
    assert row[9] is None


@pytest.mark.parametrize(
    ("yanked_value", "expected_reason"),
    [("a real reason", "a real reason"), (True, None), ("", None)],
)
def test_yanked_field_variants_store_expected_reason(reader, writer, yanked_value, expected_reason):
    _insert_wheel(reader, filename="pkg-1.0-py3-none-any.whl", state=WheelState.READY, yanked=False)
    page = _project_payload(
        last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl", yanked=yanked_value)]
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[8] == 1
    assert row[9] == expected_reason


def test_has_metadata_false_to_true_moves_no_metadata_to_need_metadata_and_deletes_skip(
    reader, writer
):
    wheel_id = _insert_wheel(
        reader, filename="pkg-1.0-py3-none-any.whl", state=WheelState.NO_METADATA
    )
    reader.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, 'fetch', ?, 1, NULL, ?)",
        (wheel_id, NO_SIDECAR_SKIP_REASON, "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    page = _project_payload(
        last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl", core_metadata={"sha256": "h"})]
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_METADATA)
    assert row[4] == "h"
    assert _skips_rows(reader, wheel_id) == []


def test_has_metadata_false_to_true_does_not_delete_a_non_permanent_no_sidecar_skip(reader, writer):
    wheel_id = _insert_wheel(
        reader, filename="pkg-1.0-py3-none-any.whl", state=WheelState.NO_METADATA
    )
    reader.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, 'fetch', ?, 0, ?, ?)",
        (wheel_id, NO_SIDECAR_SKIP_REASON, "1.0", "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    page = _project_payload(
        last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl", core_metadata={"sha256": "h"})]
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_METADATA)
    assert _skips_rows(reader, wheel_id) == [("fetch", NO_SIDECAR_SKIP_REASON, 0)]


def test_has_metadata_false_to_true_past_no_metadata_changes_only_hash(reader, writer):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        metadata_sha256=None,
        blob_sha256=None,
    )
    page = _project_payload(
        last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl", core_metadata={"sha256": "h"})]
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_CONVERT), "state must be unaffected"
    assert row[4] == "h"


def test_metadata_sha256_changed_with_archived_blob_clears_blob_and_warns(reader, writer, caplog):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        metadata_sha256="old-hash",
        blob_sha256="archived-blob",
    )
    page = _project_payload(
        last_serial=2,
        files=[_file("pkg-1.0-py3-none-any.whl", core_metadata={"sha256": "new-hash"})],
    )
    client = _make_client(_json_handler(page))

    with caplog.at_level("WARNING", logger="reroll_sync.ingest"):
        outcome = sync_project(client, reader, "proj", now=_now)
        apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_METADATA)
    assert row[4] == "new-hash"
    assert row[10] is None
    assert "does not match archived blob_sha256" in caplog.text


def test_metadata_sha256_mismatch_on_self_healed_row_warns_and_resets(reader, writer, caplog):
    """A spec-13 self-healed row has ``blob_sha256`` set but ``metadata_sha256`` unknown.

    If PyPI later reports a ``metadata_sha256`` that does not match the
    already-archived ``blob_sha256``, that must be treated exactly like the
    already-known-hash-changed case: warn, reset to ``NEED_METADATA``, and
    clear ``blob_sha256``.
    """
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        metadata_sha256=None,
        blob_sha256="archived-blob",
    )
    page = _project_payload(
        last_serial=2,
        files=[_file("pkg-1.0-py3-none-any.whl", core_metadata={"sha256": "mismatched-hash"})],
    )
    client = _make_client(_json_handler(page))

    with caplog.at_level("WARNING", logger="reroll_sync.ingest"):
        outcome = sync_project(client, reader, "proj", now=_now)
        apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_METADATA)
    assert row[4] == "mismatched-hash"
    assert row[10] is None
    assert "does not match archived blob_sha256" in caplog.text


def test_metadata_sha256_matching_archived_blob_on_self_healed_row_does_not_warn(
    reader, writer, caplog
):
    """The newly-reported hash matching ``blob_sha256`` is just confirmation, not a change."""
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        metadata_sha256=None,
        blob_sha256="archived-blob",
    )
    page = _project_payload(
        last_serial=2,
        files=[_file("pkg-1.0-py3-none-any.whl", core_metadata={"sha256": "archived-blob"})],
    )
    client = _make_client(_json_handler(page))

    with caplog.at_level("WARNING", logger="reroll_sync.ingest"):
        outcome = sync_project(client, reader, "proj", now=_now)
        apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_CONVERT), "must be unaffected: the hash is confirmed"
    assert row[4] == "archived-blob", "the now-known hash may be recorded"
    assert row[10] == "archived-blob", "blob_sha256 must not be cleared"
    assert caplog.text == ""


def test_url_changed_alone_does_not_change_state_or_blob(reader, writer):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        url="https://files.pythonhosted.org/old-url.whl",
        blob_sha256="archived-blob",
    )
    page = _project_payload(
        last_serial=2,
        files=[_file("pkg-1.0-py3-none-any.whl", url="https://files.pythonhosted.org/new-url.whl")],
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_CONVERT)
    assert row[2] == "https://files.pythonhosted.org/new-url.whl"
    assert row[10] == "archived-blob"


@pytest.mark.parametrize(
    ("field", "old", "new"),
    [
        ("size", 100, 200),
        ("upload_time", "2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z"),
        ("requires_python", ">=3.8", ">=3.9"),
    ],
)
def test_size_upload_time_requires_python_changes_do_not_change_state(
    reader, writer, field, old, new
):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        size=old if field == "size" else None,
        upload_time=old if field == "upload_time" else None,
        requires_python=old if field == "requires_python" else None,
    )
    page = _project_payload(
        last_serial=2,
        files=[
            _file(
                "pkg-1.0-py3-none-any.whl",
                size=new if field == "size" else None,
                upload_time=new if field == "upload_time" else None,
                requires_python=new if field == "requires_python" else None,
            )
        ],
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_CONVERT)
    column_index = {"size": 5, "upload_time": 6, "requires_python": 7}[field]
    assert row[column_index] == new


def test_wheel_sha256_changed_alone_updates_hash_and_bumps_seq(reader, writer):
    wheel_id = _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.READY,
        wheel_sha256="old-hash",
        change_seq=5,
    )
    page = _project_payload(
        last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl", sha256="new-hash")]
    )
    client = _make_client(_json_handler(page))
    seq_before = writer.current_seq()

    outcome = sync_project(client, reader, "proj", now=_now)
    assert isinstance(outcome, SyncOk)
    assert len(outcome.plan.changed_wheels) == 1
    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert result is not None
    assert result.updated == 1
    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[0] == wheel_id
    assert row[1] == int(WheelState.READY), "state must be unaffected by a hash-only change"
    assert row[3] == "new-hash"
    assert row[12] > seq_before, "change_seq must bump"


def test_identical_entry_produces_zero_write_ops(reader, writer):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.READY,
        url="https://files.pythonhosted.org/pkg-1.0-py3-none-any.whl",
        wheel_sha256=None,
        metadata_sha256=None,
        size=None,
        upload_time=None,
        requires_python=None,
        yanked=False,
        yanked_reason=None,
        change_seq=5,
    )
    page = _project_payload(last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl")])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    assert isinstance(outcome, SyncOk)
    assert outcome.plan.changed_wheels == ()

    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)
    assert result is not None
    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[12] == 5, "change_seq must not bump for an identical entry"
    assert result.updated == 0


# ---------------------------------------------------------------------------
# Vanished files
# ---------------------------------------------------------------------------


def test_vanished_row_is_tombstoned(reader, writer):
    _insert_wheel(reader, filename="pkg-1.0-py3-none-any.whl", state=WheelState.READY)
    page = _project_payload(last_serial=2, files=[])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.DELETED)
    assert row[13] is not None


def test_already_tombstoned_row_is_not_rewritten(reader, writer):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.DELETED,
        deleted_at="2024-01-01T00:00:00+00:00",
        change_seq=3,
    )
    page = _project_payload(last_serial=2, files=[])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    assert isinstance(outcome, SyncOk)
    assert outcome.plan.vanished_wheel_ids == ()
    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)
    assert result is not None

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[13] == "2024-01-01T00:00:00+00:00"
    assert row[12] == 3
    assert result.tombstoned == 0


def test_tombstoned_row_reappearing_is_restored(reader, writer):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.DELETED,
        deleted_at="2024-01-01T00:00:00+00:00",
        blob_sha256="stale-blob",
    )
    page = _project_payload(
        last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl", core_metadata=True)]
    )
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NEED_METADATA)
    assert row[13] is None
    assert row[10] is None


def test_tombstoned_row_reappearing_without_metadata_is_restored_to_no_metadata(reader, writer):
    _insert_wheel(
        reader,
        filename="pkg-1.0-py3-none-any.whl",
        state=WheelState.DELETED,
        deleted_at="2024-01-01T00:00:00+00:00",
        blob_sha256="stale-blob",
    )
    page = _project_payload(last_serial=2, files=[_file("pkg-1.0-py3-none-any.whl")])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    row = _wheel_row(reader, "pkg-1.0-py3-none-any.whl")
    assert row[1] == int(WheelState.NO_METADATA)
    assert row[13] is None
    assert row[10] is None


def test_tombstoning_never_issues_a_delete(reader, writer):
    _insert_wheel(reader, filename="pkg-1.0-py3-none-any.whl", state=WheelState.READY)
    page = _project_payload(last_serial=2, files=[])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert reader.execute("SELECT COUNT(*) FROM wheels").fetchone()[0] == 1


def test_project_page_with_zero_files_tombstones_everything(reader, writer):
    _insert_wheel(reader, filename="a-1.0-py3-none-any.whl", state=WheelState.READY)
    _insert_wheel(reader, filename="b-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA)
    page = _project_payload(last_serial=2, files=[])
    client = _make_client(_json_handler(page))

    outcome = sync_project(client, reader, "proj", now=_now)
    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert result is not None
    assert result.tombstoned == 2
    for filename in ("a-1.0-py3-none-any.whl", "b-1.0-py3-none-any.whl"):
        assert _wheel_row(reader, filename)[1] == int(WheelState.DELETED)


# ---------------------------------------------------------------------------
# Transactionality
# ---------------------------------------------------------------------------


def test_pypi_index_not_updated_when_project_fetch_raises(reader, writer):
    client = _make_client(_status_handler(500))

    outcome = sync_project(client, reader, "proj", now=_now)
    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncRetry)
    assert result is None
    assert _pypi_index_row(reader, "proj") is None


def test_pypi_index_not_updated_when_wheel_write_fails_mid_project(db_path, writer, reader):
    conn = sqlite3.connect(str(db_path))
    page = _project_payload(
        last_serial=5,
        files=[_file("a-1.0-py3-none-any.whl"), _file("b-1.0-py3-none-any.whl")],
    )
    client = _make_client(_json_handler(page))
    outcome = sync_project(client, conn, "proj", now=_now)
    assert isinstance(outcome, SyncOk)
    assert len(outcome.plan.new_wheels) == 2

    original_next_seq = writer.next_seq
    call_count = {"n": 0}

    def flaky_next_seq() -> int:
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError("boom")
        return original_next_seq()

    writer.next_seq = flaky_next_seq

    with pytest.raises(RuntimeError, match="boom"):
        apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert (
        reader.execute("SELECT COUNT(*) FROM wheels WHERE project = ?", ("proj",)).fetchone()[0]
        == 0
    )
    assert _pypi_index_row(reader, "proj") is None
    conn.close()


def test_failed_project_is_retried_on_the_next_poll(db_path, writer):
    """A failing ``sync_project`` must leave ``pypi_index`` untouched, so the project
    reappears in the very next poll's stale set (nothing else makes it retryable).
    """
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return httpx.Response(503, request=request)
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER},
            json=_project_payload(last_serial=1, files=[]),
            request=request,
        )

    client = _make_client(handler)
    reader_conn = sqlite3.connect(str(db_path))

    outcome = sync_project(client, reader_conn, "proj", now=_now)
    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncRetry)
    assert result is None
    assert _pypi_index_row(reader_conn, "proj") is None

    index = _index_payload(last_serial=5, projects=[{"name": "proj", "_last-serial": 5}])
    index_client = _make_client(_json_handler(index))
    poll_result = poll_index(index_client, reader_conn, etag=None)

    assert poll_result.stale_projects == ("proj",)
    reader_conn.close()


def test_all_project_writes_land_in_a_single_writer_submission(reader, writer):
    _insert_wheel(reader, filename="stale-1.0-py3-none-any.whl", state=WheelState.READY)
    page = _project_payload(
        last_serial=9,
        files=[_file("new-1.0-py3-none-any.whl", core_metadata=True)],
    )
    client = _make_client(_json_handler(page))
    outcome = sync_project(client, reader, "proj", now=_now)

    calls: list = []
    original_submit_and_wait = writer.submit_and_wait

    def counting_submit_and_wait(op):
        calls.append(op)
        return original_submit_and_wait(op)

    writer.submit_and_wait = counting_submit_and_wait
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert len(calls) == 1
    assert _wheel_row(reader, "new-1.0-py3-none-any.whl") is not None
    assert _wheel_row(reader, "stale-1.0-py3-none-any.whl")[1] == int(WheelState.DELETED)
    assert _pypi_index_row(reader, "proj") == (9, "2023-11-14T22:13:20+00:00")


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_not_found_tombstones_all_wheels_and_removes_pypi_index_row(reader, writer):
    _insert_wheel(reader, filename="a-1.0-py3-none-any.whl", state=WheelState.READY)
    reader.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("proj", 5, "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    client = _make_client(_status_handler(404))

    outcome = sync_project(client, reader, "proj", now=_now)
    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncGone)
    assert result is not None
    assert result.project_gone is True
    assert result.tombstoned == 1
    assert _wheel_row(reader, "a-1.0-py3-none-any.whl")[1] == int(WheelState.DELETED)
    assert _pypi_index_row(reader, "proj") is None


def test_transient_error_returns_retry_serial_unchanged(reader, writer):
    reader.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("proj", 5, "2024-01-01T00:00:00+00:00"),
    )
    reader.commit()
    client = _make_client(_status_handler(503))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncRetry)
    assert _pypi_index_row(reader, "proj") == (5, "2024-01-01T00:00:00+00:00")


def test_transient_fetch_error_does_not_tombstone_existing_wheels(reader, writer):
    _insert_wheel(reader, filename="a-1.0-py3-none-any.whl", project="proj", state=WheelState.READY)
    _insert_wheel(
        reader, filename="b-1.0-py3-none-any.whl", project="proj", state=WheelState.NEED_METADATA
    )
    client = _make_client(_status_handler(503))

    outcome = sync_project(client, reader, "proj", now=_now)
    result = apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncRetry)
    assert result is None
    assert _wheel_row(reader, "a-1.0-py3-none-any.whl")[1] == int(WheelState.READY)
    assert _wheel_row(reader, "a-1.0-py3-none-any.whl")[13] is None
    assert _wheel_row(reader, "b-1.0-py3-none-any.whl")[1] == int(WheelState.NEED_METADATA)
    assert _wheel_row(reader, "b-1.0-py3-none-any.whl")[13] is None


def test_rate_limited_penalizes_and_does_not_count_as_an_attempt(reader, writer):
    client = _make_client(_status_handler(429, headers={"retry-after": "12"}))
    limiter = _FakeLimiter()

    outcome = sync_project(client, reader, "proj", now=_now)
    result = apply_project_outcome(writer, limiter, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncRateLimited)
    assert outcome.seconds == 12.0
    assert result is None
    assert limiter.penalized == [("pypi.org", 12.0)]


def test_rate_limited_without_retry_after_penalizes_zero_seconds(reader, writer):
    client = _make_client(_status_handler(429))
    limiter = _FakeLimiter()

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, limiter, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncRateLimited)
    assert outcome.seconds == 0.0
    assert limiter.penalized == [("pypi.org", 0.0)]


def test_rate_limited_penalizes_the_clients_actual_configured_host_not_a_hardcoded_one(
    reader, writer
):
    custom_host = "files.example-mirror.invalid"
    client = _make_client(
        _status_handler(429, headers={"retry-after": "9"}),
        limiter=_FakeLimiter(hosts=frozenset({custom_host})),
        index_url=f"https://{custom_host}/simple/",
    )
    limiter = _FakeLimiter(hosts=frozenset({custom_host}))

    outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, limiter, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncRateLimited)
    assert outcome.child == custom_host
    assert limiter.penalized == [(custom_host, 9.0)]


def test_protocol_error_returns_retry_and_logs(reader, writer, caplog):
    client = _make_client(_status_handler(200, headers={"content-type": "text/html"}))

    with caplog.at_level("ERROR", logger="reroll_sync.ingest"):
        outcome = sync_project(client, reader, "proj", now=_now)
    apply_project_outcome(writer, None, "proj", outcome, now=_now)

    assert isinstance(outcome, SyncRetry)
    assert outcome.reason == "protocol_error"
    assert "protocol error" in caplog.text
    assert _pypi_index_row(reader, "proj") is None


# ---------------------------------------------------------------------------
# apply_project_outcome / ingest_stale_projects / ProjectBackoff
# ---------------------------------------------------------------------------


def test_apply_project_outcome_returns_none_for_retry_without_a_limiter(reader, writer):
    outcome = SyncRetry(reason="transient", details="boom")
    assert apply_project_outcome(writer, None, "proj", outcome, now=_now) is None


def test_apply_project_outcome_rate_limited_without_limiter_does_not_raise(reader, writer):
    outcome = SyncRateLimited(child="pypi.org", seconds=5.0)
    assert apply_project_outcome(writer, None, "proj", outcome, now=_now) is None


def test_read_project_wheels_reads_in_multiple_chunks(reader):
    from reroll_sync.ingest import _read_project_wheels

    for i in range(5):
        _insert_wheel(reader, filename=f"pkg-{i}-1.0-py3-none-any.whl", project="proj")

    rows = _read_project_wheels(reader, "proj", chunk_size=2, budget=0.25, watchdog=None)

    assert len(rows) == 5


def test_ingest_stale_projects_aggregates_across_projects(db_path, writer):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if "found" in path:
            return httpx.Response(404, request=request)
        if "limited" in path:
            return httpx.Response(429, headers={"retry-after": "3"}, request=request)
        if "broken" in path:
            return httpx.Response(500, request=request)
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER},
            json=_project_payload(
                last_serial=1,
                files=[_file(f"{path.strip('/').split('/')[-1]}-1.0-py3-none-any.whl")],
            ),
            request=request,
        )

    client = _make_client(handler)
    limiter = _FakeLimiter()
    summary = ingest_stale_projects(
        client,
        lambda: sqlite3.connect(str(db_path)),
        writer,
        limiter,
        ["ok-a", "ok-b", "not-found", "rate-limited", "broken"],
        now=_now,
        max_workers=3,
    )

    assert isinstance(summary, IngestSummary)
    assert summary.inserted == 2
    assert summary.projects_gone == 1
    assert summary.retried == ("broken",)
    assert summary.rate_limited == ("rate-limited",)
    assert limiter.penalized == [("pypi.org", 3.0)]


def test_ingest_stale_projects_records_backoff_failure_and_success(db_path, writer):
    def handler(request: httpx.Request) -> httpx.Response:
        if "broken" in request.url.path:
            return httpx.Response(500, request=request)
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER},
            json=_project_payload(last_serial=1, files=[]),
            request=request,
        )

    client = _make_client(handler)
    backoff = ProjectBackoff(now=_now)

    ingest_stale_projects(
        client,
        lambda: sqlite3.connect(str(db_path)),
        writer,
        None,
        ["ok", "broken"],
        now=_now,
        max_workers=2,
        backoff=backoff,
    )

    assert backoff.is_eligible("ok") is True
    assert backoff.is_eligible("broken") is False


def test_ingest_stale_projects_skips_projects_still_in_backoff_window(db_path, writer):
    """Fix 2 regression: ``ingest_stale_projects`` must consult ``backoff.is_eligible``.

    A project that fails once must not be refetched on the very next call
    while it is still inside its backoff window -- it is only refetched
    once the injected clock reaches its next-eligible time.
    """
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if "broken" in request.url.path:
            return httpx.Response(500, request=request)
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER},
            json=_project_payload(last_serial=1, files=[]),
            request=request,
        )

    client = _make_client(handler)
    clock = {"t": 1000.0}
    backoff = ProjectBackoff(now=lambda: clock["t"])

    ingest_stale_projects(
        client,
        lambda: sqlite3.connect(str(db_path)),
        writer,
        None,
        ["broken"],
        now=lambda: clock["t"],
        backoff=backoff,
    )
    assert any("broken" in path for path in calls)
    assert backoff.attempts("broken") == 1
    assert backoff.is_eligible("broken") is False
    calls.clear()

    # Still inside the backoff window: 'broken' must be skipped entirely.
    ingest_stale_projects(
        client,
        lambda: sqlite3.connect(str(db_path)),
        writer,
        None,
        ["broken", "ok"],
        now=lambda: clock["t"],
        backoff=backoff,
    )

    assert not any("broken" in path for path in calls), "must not be re-fetched while backing off"
    assert any("ok" in path for path in calls)
    assert backoff.attempts("broken") == 1, "must not count as a re-attempt"


def test_ingest_stale_projects_rate_limited_does_not_increment_backoff_attempts(db_path, writer):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "4"}, request=request)

    client = _make_client(handler)
    limiter = _FakeLimiter()
    backoff = ProjectBackoff(now=_now)

    summary = ingest_stale_projects(
        client,
        lambda: sqlite3.connect(str(db_path)),
        writer,
        limiter,
        ["proj"],
        now=_now,
        backoff=backoff,
    )

    assert summary.rate_limited == ("proj",)
    assert backoff.attempts("proj") == 0
    assert backoff.is_eligible("proj") is True
    assert limiter.penalized == [("pypi.org", 4.0)]


def test_ingest_stale_projects_records_quarantine_error_row_and_summary_count(
    db_path, writer, reader, caplog
):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, request=request)

    client = _make_client(handler)
    backoff = ProjectBackoff(now=_now, max_attempts=0)

    with caplog.at_level("ERROR", logger="reroll_sync.ingest"):
        summary = ingest_stale_projects(
            client,
            lambda: sqlite3.connect(str(db_path)),
            writer,
            None,
            ["broken"],
            now=_now,
            backoff=backoff,
        )

    assert summary.quarantined_projects == 1
    assert "quarantined" in caplog.text
    assert "broken" in caplog.text
    errors = _errors_rows(reader)
    assert len(errors) == 1
    wheel_id, category, subcat, details = errors[0]
    assert wheel_id is None
    assert category == "project_ingest_quarantined"
    assert subcat == "broken"
    assert "broken" in details
    assert "1" in details


def test_project_backoff_record_failure_computes_backoff_and_quarantines(caplog):
    clock = {"t": 1000.0}
    backoff = ProjectBackoff(now=lambda: clock["t"])

    for _ in range(DEFAULT_MAX_ATTEMPTS):
        backoff.record_failure("proj")
        assert not backoff.is_eligible("proj")
        clock["t"] += 10_000.0

    assert "proj" not in backoff.quarantined()
    with caplog.at_level("ERROR", logger="reroll_sync.ingest"):
        backoff.record_failure("proj")
    assert backoff.quarantined() == frozenset({"proj"})
    assert backoff.is_eligible("proj") is False
    assert backoff.attempts("proj") == DEFAULT_MAX_ATTEMPTS + 1
    assert "proj" in caplog.text
    assert "quarantined" in caplog.text


def test_project_backoff_record_failure_only_logs_once_per_quarantine_transition(caplog):
    """A subsequent failure past quarantine must not log again -- only the transition does."""
    backoff = ProjectBackoff(now=lambda: 1000.0, max_attempts=0)

    with caplog.at_level("ERROR", logger="reroll_sync.ingest"):
        backoff.record_failure("proj")
        caplog.clear()
        backoff.record_failure("proj")

    assert "quarantined" not in caplog.text


def test_project_backoff_record_success_clears_state():
    backoff = ProjectBackoff(now=lambda: 1000.0)
    backoff.record_failure("proj")
    assert not backoff.is_eligible("proj")

    backoff.record_success("proj")

    assert backoff.is_eligible("proj") is True
    assert backoff.quarantined() == frozenset()


def test_project_backoff_never_failed_is_eligible():
    backoff = ProjectBackoff(now=lambda: 1000.0)
    assert backoff.is_eligible("never-touched") is True


def test_project_backoff_attempts_reflects_recorded_failures():
    backoff = ProjectBackoff(now=lambda: 1000.0)
    assert backoff.attempts("proj") == 0

    backoff.record_failure("proj")
    backoff.record_failure("proj")

    assert backoff.attempts("proj") == 2
    backoff.record_success("proj")
    assert backoff.attempts("proj") == 0


def test_project_backoff_is_thread_safe_under_concurrent_record_failure():
    """No lost updates and no corruption under concurrent ``record_failure`` calls.

    Each of ``num_projects`` threads hammers ``record_failure`` for its own,
    distinct project while a separate reader thread concurrently calls
    ``quarantined()`` (which snapshots the internal set) and ``is_eligible()``
    in a tight loop. Without the lock, mutating ``_quarantined`` (a plain
    ``set``) from one thread while ``frozenset(self._quarantined)`` iterates
    it on another can raise ``RuntimeError: Set changed size during
    iteration``; the lock is what rules that out, not just the GIL.
    """
    num_projects = 20
    calls_per_project = 5
    max_attempts = 3
    backoff = ProjectBackoff(now=lambda: 1000.0, max_attempts=max_attempts)
    start = threading.Event()
    stop_reading = threading.Event()
    reader_errors: list[BaseException] = []

    def _record_worker(project: str) -> None:
        start.wait(timeout=5.0)
        for _ in range(calls_per_project):
            backoff.record_failure(project)

    def _reader_worker() -> None:
        start.wait(timeout=5.0)
        while not stop_reading.is_set():
            try:
                backoff.quarantined()
                backoff.is_eligible("proj-0")
            except Exception as exc:
                reader_errors.append(exc)
                return

    record_threads = [
        threading.Thread(target=_record_worker, args=(f"proj-{i}",)) for i in range(num_projects)
    ]
    reader_thread = threading.Thread(target=_reader_worker)
    reader_thread.start()
    for t in record_threads:
        t.start()
    start.set()
    for t in record_threads:
        t.join(timeout=5.0)
    stop_reading.set()
    reader_thread.join(timeout=5.0)

    assert reader_errors == []
    for i in range(num_projects):
        project = f"proj-{i}"
        assert backoff.attempts(project) == calls_per_project, "no lost updates"
        assert project in backoff.quarantined()


# ---------------------------------------------------------------------------
# Direct unit tests for _diff_common / _plan_new_wheel (exhaustive small branches)
# ---------------------------------------------------------------------------


def _local(**overrides) -> _LocalWheelRow:
    defaults = {
        "id": 1,
        "state": WheelState.READY,
        "url": "https://files.pythonhosted.org/pkg.whl",
        "wheel_sha256": None,
        "metadata_sha256": None,
        "size": None,
        "upload_time": None,
        "requires_python": None,
        "yanked": False,
        "yanked_reason": None,
        "blob_sha256": None,
        "deleted_at": None,
    }
    defaults.update(overrides)
    return _LocalWheelRow(**defaults)


def _remote_file(**overrides) -> ProjectFile:
    defaults = {
        "filename": "pkg.whl",
        "url": "https://files.pythonhosted.org/pkg.whl",
        "wheel_sha256": None,
        "metadata_sha256": None,
        "has_metadata": False,
        "size": None,
        "upload_time": None,
        "requires_python": None,
        "yanked": False,
        "yanked_reason": None,
    }
    defaults.update(overrides)
    return ProjectFile(**defaults)


def test_diff_common_returns_none_when_nothing_changed():
    local = _local()
    remote = _remote_file()
    assert _diff_common(local, remote) is None


def test_diff_common_metadata_gain_without_blob_does_not_warn():
    local = _local(state=WheelState.NEED_CONVERT, metadata_sha256=None, blob_sha256=None)
    remote = _remote_file(metadata_sha256="h", has_metadata=True)
    result = _diff_common(local, remote)
    assert result is not None
    assert result.changes.get("state") is None
    assert result.changes["metadata_sha256"] == "h"


def test_plan_new_wheel_uses_unlinked_sha256_when_present():
    file = _remote_file(filename="pkg.whl", has_metadata=False)
    result = _plan_new_wheel(file, "archived-sha")
    assert result.state == WheelState.NEED_CONVERT
    assert result.blob_sha256 == "archived-sha"
    assert result.unlinked_blob_filename == "pkg.whl"


def test_plan_new_wheel_without_unlinked_and_has_metadata_false():
    file = _remote_file(filename="pkg.whl", has_metadata=False)
    result = _plan_new_wheel(file, None)
    assert result.state == WheelState.NO_METADATA
    assert result.blob_sha256 is None
    assert result.unlinked_blob_filename is None
