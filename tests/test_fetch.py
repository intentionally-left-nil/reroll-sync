"""Tests for the metadata fetch stage: fetch_one, the byte-budgeted handoff
queue, the archive/convert handoff, crash recovery, and bulk convert.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
import pathlib
import sqlite3
import threading
import zlib
from typing import cast

import httpx
import pytest
import reroll
from reroll.errors import UnsupportedPrereleaseError
from reroll.name_mapping import passthrough_mapper

from reroll_sync.archive.format import decompress_block
from reroll_sync.archive.reader import SegmentReader
from reroll_sync.archive.store import ArchiveStore
from reroll_sync.convert import ConvertOk, convert
from reroll_sync.db import init_db
from reroll_sync.dispatcher import (
    Dispatcher,
    QueueItem,
    RateLimited,
    Retry,
    Stage,
    adapt_convert_outcome,
)
from reroll_sync.fetch import (
    ArchiveHandoff,
    BulkConvertSource,
    ByteBudgetedQueue,
    FetchItem,
    FetchOk,
    FetchRateLimited,
    FetchRetry,
    FetchSkip,
    HandoffItem,
    QueueClosed,
    adapt_fetch_outcome,
    dispatch_fetch_item,
    fetch_one,
    recover_unsealed_segment,
)
from reroll_sync.pypi_client import PyPIClient
from reroll_sync.schema import WheelState
from reroll_sync.shutdown import ShutdownError
from reroll_sync.version import REROLL_VERSION
from reroll_sync.writer import Writer

_E2E_MAPPERS = (passthrough_mapper,)

_USER_AGENT = "reroll-sync-test (contact@example.invalid)"
_HOSTS = frozenset({"files.pythonhosted.org"})


class _FakeLimiter:
    def __init__(self, *, acquire_result: bool = True) -> None:
        self.calls: list[str] = []
        self._acquire_result = acquire_result

    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        if child_name not in _HOSTS:
            raise KeyError(child_name)
        self.calls.append(child_name)
        return self._acquire_result


def _client(handler, *, limiter: _FakeLimiter | None = None) -> PyPIClient:
    transport = httpx.MockTransport(handler)
    return PyPIClient(
        limiter if limiter is not None else _FakeLimiter(),
        user_agent=_USER_AGENT,
        transport=transport,
    )


def _item(
    *,
    id: int = 1,
    filename: str = "widget-1.0-py3-none-any.whl",
    url: str = "https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl",
    metadata_sha256: str | None = None,
    has_metadata: bool = True,
    project: str = "widget",
    lane: int = 0,
    state: WheelState = WheelState.NEED_METADATA,
) -> FetchItem:
    return FetchItem(
        id=id,
        project=project,
        lane=lane,
        state=state,
        filename=filename,
        url=url,
        metadata_sha256=metadata_sha256,
        has_metadata=has_metadata,
    )


def _clock():
    return lambda: 1_700_000_000.0


# ---------------------------------------------------------------------------
# fetch_one
# ---------------------------------------------------------------------------


def test_success_returns_ok_with_exact_bytes_and_correct_sha256():
    data = b"Metadata-Version: 2.1\nName: widget\n"
    expected_sha256 = hashlib.sha256(data).hexdigest()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data, request=request)

    client = _client(handler)
    outcome = fetch_one(client, _item(metadata_sha256=expected_sha256), now=_clock())

    assert isinstance(outcome, FetchOk)
    assert outcome.data == data
    assert outcome.sha256 == expected_sha256


def test_requested_url_is_wheel_url_plus_metadata_suffix():
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return httpx.Response(200, content=b"data", request=request)

    client = _client(handler)
    fetch_one(
        client,
        _item(url="https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl"),
        now=_clock(),
    )

    assert requested == ["https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl.metadata"]


def test_expected_sha256_is_passed_through_from_metadata_sha256():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"wrong bytes", request=request)

    client = _client(handler)
    outcome = fetch_one(client, _item(metadata_sha256="deadbeef"), now=_clock())

    assert isinstance(outcome, FetchSkip)
    assert "deadbeef" in outcome.details


def test_metadata_sha256_none_but_has_metadata_true_fetches_unverified():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"unverified data", request=request)

    client = _client(handler)
    outcome = fetch_one(client, _item(metadata_sha256=None, has_metadata=True), now=_clock())

    assert isinstance(outcome, FetchOk)
    assert outcome.data == b"unverified data"


def test_pypi_not_found_is_permanent_skip():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    client = _client(handler)
    outcome = fetch_one(client, _item(), now=_clock())

    assert isinstance(outcome, FetchSkip)
    assert outcome.reason == "metadata_missing"
    assert outcome.permanent is True
    assert outcome.reroll_version is None


def test_metadata_hash_mismatch_is_non_permanent_skip_with_both_digests():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"wrong bytes", request=request)

    client = _client(handler)
    expected = "e" * 64
    outcome = fetch_one(client, _item(metadata_sha256=expected), now=_clock())

    assert isinstance(outcome, FetchSkip)
    assert outcome.reason == "metadata_hash_mismatch"
    assert outcome.permanent is False
    actual = hashlib.sha256(b"wrong bytes").hexdigest()
    assert expected in outcome.details
    assert actual in outcome.details
    assert outcome.reroll_version == "reroll-sync"


def test_pypi_transient_error_is_retry():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    client = _client(handler)
    outcome = fetch_one(client, _item(), now=_clock())

    assert isinstance(outcome, FetchRetry)


def test_pypi_protocol_error_is_retry():
    oversized = b"x" * (32 * 1024 * 1024 + 1)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=oversized, request=request)

    client = _client(handler)
    outcome = fetch_one(client, _item(), now=_clock())

    assert isinstance(outcome, FetchRetry)


def test_pypi_rate_limited_carries_retry_after():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "17"}, request=request)

    client = _client(handler)
    outcome = fetch_one(client, _item(), now=_clock())

    assert isinstance(outcome, FetchRateLimited)
    assert outcome.retry_after == 17.0
    assert outcome.child == "files.pythonhosted.org"


def test_row_that_should_have_been_no_metadata_returns_retry_and_logs(caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("should not fetch a wheel with no metadata hash and flag")

    client = _client(handler)
    with caplog.at_level(logging.ERROR, logger="reroll_sync.fetch"):
        outcome = fetch_one(client, _item(metadata_sha256=None, has_metadata=False), now=_clock())

    assert isinstance(outcome, FetchRetry)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1


def test_fetch_one_performs_no_database_write():
    # fetch_one takes no connection at all -- pins that its signature has
    # no way to reach a database even by accident.
    assert "conn" not in inspect.signature(fetch_one).parameters

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"data", request=request)

    client = _client(handler)
    outcome = fetch_one(client, _item(), now=_clock())
    assert isinstance(outcome, FetchOk)


# ---------------------------------------------------------------------------
# import-bridge lookup
# ---------------------------------------------------------------------------


def _bridge_db(tmp_path, rows: list[tuple[str, bytes]]):
    """Build a minimal ``metadata_blob`` table at ``tmp_path/bridge.db``
    containing one ``(sha256, z_body)`` row per entry in ``rows``.
    """
    path = tmp_path / "bridge.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE metadata_blob (sha256 TEXT PRIMARY KEY, z_body BLOB NOT NULL)")
        conn.executemany("INSERT INTO metadata_blob (sha256, z_body) VALUES (?, ?)", rows)
        conn.commit()
    finally:
        conn.close()
    return path


def test_bridge_db_path_returns_none_when_env_var_unset(monkeypatch):
    from reroll_sync.fetch import _bridge_db_path

    monkeypatch.delenv("REROLL_DATA_BRIDGE_DB_PATH", raising=False)

    assert _bridge_db_path() is None


def test_bridge_db_path_expands_user(monkeypatch):
    from reroll_sync.fetch import _bridge_db_path

    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", "~/bridge.db")

    assert _bridge_db_path() == pathlib.Path("~/bridge.db").expanduser()


def test_bridge_lookup_returns_none_when_metadata_sha256_is_none(tmp_path, monkeypatch):
    from reroll_sync.fetch import _bridge_lookup

    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", str(_bridge_db(tmp_path, [])))

    assert _bridge_lookup(None) is None


def test_bridge_lookup_returns_none_when_env_var_unset(monkeypatch):
    from reroll_sync.fetch import _bridge_lookup

    monkeypatch.delenv("REROLL_DATA_BRIDGE_DB_PATH", raising=False)

    assert _bridge_lookup("e" * 64) is None


def test_bridge_lookup_returns_none_when_db_path_does_not_exist(tmp_path, monkeypatch):
    from reroll_sync.fetch import _bridge_lookup

    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", str(tmp_path / "missing.db"))

    assert _bridge_lookup("e" * 64) is None


def test_bridge_lookup_returns_none_when_db_cannot_be_opened(tmp_path, monkeypatch):
    from reroll_sync.fetch import _bridge_lookup

    # A directory, not a file: exists() is True but sqlite3 can't open it as
    # a database, so connect() itself raises OperationalError.
    not_a_db = tmp_path / "bridge.db"
    not_a_db.mkdir()
    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", str(not_a_db))

    assert _bridge_lookup("e" * 64) is None


def test_bridge_lookup_returns_none_on_a_miss(tmp_path, monkeypatch):
    from reroll_sync.fetch import _bridge_lookup

    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", str(_bridge_db(tmp_path, [])))

    assert _bridge_lookup("e" * 64) is None


def test_bridge_lookup_returns_decompressed_bytes_on_a_hit(tmp_path, monkeypatch):
    from reroll_sync.fetch import _bridge_lookup

    data = b"Metadata-Version: 2.1\nName: widget\n"
    sha256 = hashlib.sha256(data).hexdigest()
    db_path = _bridge_db(tmp_path, [(sha256, zlib.compress(data))])
    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", str(db_path))

    assert _bridge_lookup(sha256) == data


def test_bridge_lookup_returns_none_and_logs_on_a_sha256_mismatch(tmp_path, monkeypatch, caplog):
    from reroll_sync.fetch import _bridge_lookup

    data = b"Metadata-Version: 2.1\nName: widget\n"
    claimed_sha256 = "e" * 64
    db_path = _bridge_db(tmp_path, [(claimed_sha256, zlib.compress(data))])
    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", str(db_path))

    with caplog.at_level(logging.WARNING, logger="reroll_sync.fetch"):
        outcome = _bridge_lookup(claimed_sha256)

    assert outcome is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert claimed_sha256 in warnings[0].message


def test_fetch_one_serves_a_bridge_hit_without_touching_the_network(tmp_path, monkeypatch):
    data = b"Metadata-Version: 2.1\nName: widget\n"
    sha256 = hashlib.sha256(data).hexdigest()
    db_path = _bridge_db(tmp_path, [(sha256, zlib.compress(data))])
    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", str(db_path))

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("a bridge hit must not fetch from PyPI")

    client = _client(handler)
    outcome = fetch_one(client, _item(metadata_sha256=sha256), now=_clock())

    assert isinstance(outcome, FetchOk)
    assert outcome.data == data
    assert outcome.sha256 == sha256


def test_fetch_one_falls_through_to_the_network_on_a_bridge_miss(tmp_path, monkeypatch):
    monkeypatch.setenv("REROLL_DATA_BRIDGE_DB_PATH", str(_bridge_db(tmp_path, [])))
    data = b"fetched from pypi"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data, request=request)

    client = _client(handler)
    outcome = fetch_one(
        client, _item(metadata_sha256=hashlib.sha256(data).hexdigest()), now=_clock()
    )

    assert isinstance(outcome, FetchOk)
    assert outcome.data == data


# ---------------------------------------------------------------------------
# adapt_fetch_outcome / dispatch_fetch_item
# ---------------------------------------------------------------------------


def _writer_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 50")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "fetch.db")
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
    db_path: str,
    *,
    filename: str,
    project: str = "widget",
    state: WheelState = WheelState.NEED_METADATA,
    url: str = "https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl",
    metadata_sha256: str | None = None,
    blob_sha256: str | None = None,
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO wheels "
            "(filename, project, state, lane, url, metadata_sha256, blob_sha256, "
            "serial, change_seq, updated_at) "
            "VALUES (?, ?, ?, 0, ?, ?, ?, 1, 1, '2024-01-01T00:00:00+00:00')",
            (filename, project, int(state), url, metadata_sha256, blob_sha256),
        )
        conn.commit()
        assert cursor.lastrowid is not None
        return cursor.lastrowid
    finally:
        conn.close()


def test_adapt_fetch_skip_maps_to_retry_not_ok_no_metadata():
    """PyPINotFound/MetadataHashMismatch failures adapt onto Retry, not the
    old Ok(NO_METADATA, ...) workaround -- see fetch.py's module docstring.
    """
    outcome = FetchSkip(
        reason="metadata_missing",
        subcategory="PyPINotFound",
        details="404",
        permanent=True,
        reroll_version=None,
    )
    adapted = adapt_fetch_outcome(outcome)
    assert isinstance(adapted, Retry)
    assert adapted.reason == "metadata_missing"
    assert adapted.details == "404"


def test_adapt_fetch_skip_from_hash_mismatch_carries_both_digests_into_retry_details():
    outcome = FetchSkip(
        reason="metadata_hash_mismatch",
        subcategory="MetadataHashMismatch",
        details="expected sha256=" + "e" * 64 + ", actual sha256=" + "a" * 64,
        permanent=False,
        reroll_version="reroll-sync",
    )
    adapted = adapt_fetch_outcome(outcome)
    assert isinstance(adapted, Retry)
    assert "e" * 64 in adapted.details
    assert "a" * 64 in adapted.details


def test_adapt_fetch_retry_maps_fields_through():
    outcome = FetchRetry(reason="pypi_transient_error", details="503")
    adapted = adapt_fetch_outcome(outcome)
    assert isinstance(adapted, Retry)
    assert adapted.reason == "pypi_transient_error"
    assert adapted.details == "503"


def test_adapt_fetch_rate_limited_maps_fields_through():
    outcome = FetchRateLimited(child="files.pythonhosted.org", retry_after=5.0)
    adapted = adapt_fetch_outcome(outcome)
    assert isinstance(adapted, RateLimited)
    assert adapted.child == "files.pythonhosted.org"
    assert adapted.seconds == 5.0


def test_adapt_fetch_rate_limited_with_no_retry_after_defaults_to_zero():
    outcome = FetchRateLimited(child="files.pythonhosted.org", retry_after=None)
    adapted = adapt_fetch_outcome(outcome)
    assert isinstance(adapted, RateLimited)
    assert adapted.seconds == 0.0


def test_adapt_fetch_outcome_raises_on_unsupported_type():
    bogus = cast("FetchSkip | FetchRetry | FetchRateLimited", object())
    with pytest.raises(TypeError):
        adapt_fetch_outcome(bogus)


def test_pypi_not_found_retries_leaving_state_need_metadata_and_no_skip_row(
    db_path, reader, writer
):
    """PyPINotFound adapts onto Retry: the wheel stays NEED_METADATA (never
    NO_METADATA), work.attempts is incremented, and neither skips nor
    errors gets a row on this, non-quarantining, first failure.
    """
    wheel_id = _insert_wheel(db_path, filename="notfound-1.0-py3-none-any.whl")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    client = _client(handler)
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    item = _item(id=wheel_id, filename="notfound-1.0-py3-none-any.whl")

    outcome = dispatch_fetch_item(
        client,
        item,
        dispatcher=dispatcher,
        enqueue=lambda item, data, sha256: pytest.fail("must not enqueue on skip"),
        now=_clock(),
    )

    assert isinstance(outcome, FetchSkip)
    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.NEED_METADATA)  # Retry never touches wheels.state

    work_row = reader.execute(
        "SELECT attempts, quarantined_at FROM work WHERE wheel_id = ? AND stage = 'fetch'",
        (wheel_id,),
    ).fetchone()
    assert work_row == (1, None)

    skip_count = reader.execute(
        "SELECT COUNT(*) FROM skips WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert skip_count[0] == 0  # Retry never writes a skips row

    error_count = reader.execute(
        "SELECT COUNT(*) FROM errors WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert error_count[0] == 0  # no errors row until quarantine


def test_pypi_not_found_quarantines_after_max_attempts_with_errors_row(db_path, reader, writer):
    """After max_attempts failures, the existing retry/quarantine machinery
    (unchanged in dispatcher.py) quarantines the wheel and records an
    errors row -- reusing the NEED_METADATA -> QUARANTINED edge that
    schema.ALLOWED_TRANSITIONS already has.
    """
    wheel_id = _insert_wheel(db_path, filename="notfound-q-1.0-py3-none-any.whl")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    client = _client(handler)
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", max_attempts=1, now=_clock())
    item = _item(id=wheel_id, filename="notfound-q-1.0-py3-none-any.whl")

    for _ in range(2):  # 1st failure: attempts=1, retried; 2nd: attempts=2 > 1, quarantined
        dispatch_fetch_item(
            client,
            item,
            dispatcher=dispatcher,
            enqueue=lambda item, data, sha256: pytest.fail("must not enqueue on skip"),
            now=_clock(),
        )

    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.QUARANTINED)

    work_row = reader.execute(
        "SELECT attempts, quarantined_at FROM work WHERE wheel_id = ? AND stage = 'fetch'",
        (wheel_id,),
    ).fetchone()
    assert work_row[0] == 2
    assert work_row[1] is not None

    error_row = reader.execute(
        "SELECT error_category FROM errors WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert error_row == ("metadata_missing",)


def test_metadata_hash_mismatch_retries_leaving_state_need_metadata_with_digests_in_work(
    db_path, reader, writer
):
    """MetadataHashMismatch adapts onto Retry too: the wheel stays
    NEED_METADATA, and Retry.details -- carrying both digests -- lands
    durably in work.last_error, satisfying spec 09's "record both digests"
    requirement without a separate errors-row write.
    """
    expected = "e" * 64
    wheel_id = _insert_wheel(
        db_path, filename="mismatch-1.0-py3-none-any.whl", metadata_sha256=expected
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"wrong bytes", request=request)

    client = _client(handler)
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    item = _item(id=wheel_id, filename="mismatch-1.0-py3-none-any.whl", metadata_sha256=expected)

    dispatch_fetch_item(
        client,
        item,
        dispatcher=dispatcher,
        enqueue=lambda item, data, sha256: pytest.fail("must not enqueue on skip"),
        now=_clock(),
    )

    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.NEED_METADATA)

    actual = hashlib.sha256(b"wrong bytes").hexdigest()
    work_row = reader.execute(
        "SELECT attempts, last_error FROM work WHERE wheel_id = ? AND stage = 'fetch'",
        (wheel_id,),
    ).fetchone()
    assert work_row[0] == 1
    assert expected in work_row[1]
    assert actual in work_row[1]

    skip_count = reader.execute(
        "SELECT COUNT(*) FROM skips WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert skip_count[0] == 0


def test_metadata_hash_mismatch_quarantines_after_max_attempts_with_digests_in_errors(
    db_path, reader, writer
):
    expected = "e" * 64
    wheel_id = _insert_wheel(
        db_path, filename="mismatch-q-1.0-py3-none-any.whl", metadata_sha256=expected
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"wrong bytes", request=request)

    client = _client(handler)
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", max_attempts=1, now=_clock())
    item = _item(id=wheel_id, filename="mismatch-q-1.0-py3-none-any.whl", metadata_sha256=expected)

    for _ in range(2):
        dispatch_fetch_item(
            client,
            item,
            dispatcher=dispatcher,
            enqueue=lambda item, data, sha256: pytest.fail("must not enqueue on skip"),
            now=_clock(),
        )

    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.QUARANTINED)

    actual = hashlib.sha256(b"wrong bytes").hexdigest()
    error_row = reader.execute(
        "SELECT error_category, details FROM errors WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert error_row[0] == "metadata_hash_mismatch"
    assert expected in error_row[1]
    assert actual in error_row[1]


def test_retry_outcome_leaves_state_unchanged(db_path, reader, writer):
    wheel_id = _insert_wheel(db_path, filename="retry-1.0-py3-none-any.whl")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    client = _client(handler)
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    item = _item(id=wheel_id, filename="retry-1.0-py3-none-any.whl")

    dispatch_fetch_item(
        client,
        item,
        dispatcher=dispatcher,
        enqueue=lambda item, data, sha256: pytest.fail("must not enqueue on retry"),
        now=_clock(),
    )

    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.NEED_METADATA)
    work_row = reader.execute(
        "SELECT attempts FROM work WHERE wheel_id = ? AND stage = 'fetch'", (wheel_id,)
    ).fetchone()
    assert work_row[0] == 1


def test_dispatch_fetch_item_enqueues_on_ok(db_path, reader, writer):
    wheel_id = _insert_wheel(db_path, filename="ok-1.0-py3-none-any.whl")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"data", request=request)

    client = _client(handler)
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    enqueued: list = []
    item = _item(id=wheel_id, filename="ok-1.0-py3-none-any.whl")

    outcome = dispatch_fetch_item(
        client,
        item,
        dispatcher=dispatcher,
        enqueue=lambda item, data, sha256: enqueued.append((item, data, sha256)),
        now=_clock(),
    )

    assert isinstance(outcome, FetchOk)
    assert len(enqueued) == 1
    assert enqueued[0][1] == b"data"
    # No terminal outcome applied yet -- the wheel is still NEED_METADATA.
    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.NEED_METADATA)


# ---------------------------------------------------------------------------
# ByteBudgetedQueue
# ---------------------------------------------------------------------------


def test_budget_exceeds_one_maximum_response():
    # 32 MiB is the PyPI client's fetch_metadata size cap; the default
    # budget must exceed it or the pipeline can deadlock on one response.
    max_response_bytes = 32 * 1024 * 1024
    from reroll_sync.fetch import DEFAULT_QUEUE_BUDGET_BYTES

    assert max_response_bytes < DEFAULT_QUEUE_BUDGET_BYTES


def test_a_single_max_size_item_does_not_block_even_over_budget():
    queue = ByteBudgetedQueue(budget_bytes=10)
    queue.put("big-item", size=32 * 1024 * 1024)  # must not block: queue was empty
    assert len(queue) == 1
    assert queue.current_bytes() == 32 * 1024 * 1024


def test_put_blocks_once_budget_exceeded_and_unblocks_on_drain():
    queue = ByteBudgetedQueue(budget_bytes=100)
    queue.put("first", size=90)  # queue empty -> always accepted

    second_put_done = threading.Event()

    def _put_second():
        queue.put("second", size=50)  # 90 + 50 > 100 -> must block
        second_put_done.set()

    thread = threading.Thread(target=_put_second)
    thread.start()
    try:
        assert not second_put_done.wait(timeout=0.2)
        assert queue.get() == "first"  # pops, but bytes stay charged until release
        assert not second_put_done.wait(timeout=0.2)
        queue.release(90)  # now the budget is actually freed
        assert second_put_done.wait(timeout=5)
    finally:
        thread.join(timeout=5)
    assert queue.get() == "second"


def test_get_blocks_until_an_item_is_available():
    queue = ByteBudgetedQueue(budget_bytes=100)
    got: list[object] = []

    def _get():
        got.append(queue.get())

    thread = threading.Thread(target=_get)
    thread.start()
    try:
        assert not thread.is_alive() or got == []
        queue.put("value", size=1)
        thread.join(timeout=5)
    finally:
        thread.join(timeout=5)
    assert got == ["value"]


def test_close_unblocks_a_pending_get_with_none():
    queue = ByteBudgetedQueue(budget_bytes=100)
    result: dict[str, object] = {}
    got_event = threading.Event()

    def _get():
        result["value"] = queue.get()
        got_event.set()

    thread = threading.Thread(target=_get)
    thread.start()
    try:
        queue.close()
        assert got_event.wait(timeout=5)
    finally:
        thread.join(timeout=5)
    assert result["value"] is None


def test_close_unblocks_a_pending_put_by_raising():
    queue = ByteBudgetedQueue(budget_bytes=100)
    queue.put("first", size=90)
    error: dict[str, Exception] = {}
    put_done = threading.Event()

    def _put_second():
        try:
            queue.put("second", size=50)
        except Exception as exc:
            error["exc"] = exc
        finally:
            put_done.set()

    thread = threading.Thread(target=_put_second)
    thread.start()
    try:
        assert not put_done.wait(timeout=0.2)
        queue.close()
        assert put_done.wait(timeout=5)
    finally:
        thread.join(timeout=5)
    assert isinstance(error.get("exc"), QueueClosed)


def test_put_after_close_raises_immediately():
    queue = ByteBudgetedQueue(budget_bytes=100)
    queue.close()
    with pytest.raises(QueueClosed):
        queue.put("value", size=1)


def test_put_after_close_raises_shutdown_error():
    """A closed queue's `put` is a `ShutdownError`: a task root that catches
    the shared base type exits cleanly on it, whichever boundary raised."""
    queue = ByteBudgetedQueue(budget_bytes=100)
    queue.close()
    with pytest.raises(ShutdownError):
        queue.put("value", size=1)


def test_get_after_close_and_drain_returns_none():
    queue = ByteBudgetedQueue(budget_bytes=100)
    queue.put("value", size=1)
    queue.close()
    assert queue.get() == "value"
    assert queue.get() is None


def test_negative_or_zero_budget_raises():
    with pytest.raises(ValueError, match="budget_bytes must be positive"):
        ByteBudgetedQueue(budget_bytes=0)


def test_current_bytes_tracks_puts_gets_and_releases():
    queue = ByteBudgetedQueue(budget_bytes=1000)
    assert queue.current_bytes() == 0
    queue.put("a", size=30)
    assert queue.current_bytes() == 30
    queue.get()
    assert queue.current_bytes() == 30  # still charged: not released yet
    queue.release(30)
    assert queue.current_bytes() == 0


# ---------------------------------------------------------------------------
# ArchiveHandoff: the archive thread's per-item processing
# ---------------------------------------------------------------------------


def _store_conn(db_path) -> sqlite3.Connection:
    """A dedicated connection for ArchiveStore, separate from the Writer's
    main connection -- see fetch.py's module docstring on ArchiveHandoff.
    """
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def store(tmp_path, db_path):
    conn = _store_conn(db_path)
    s = ArchiveStore(tmp_path / "segments", conn)
    yield s
    # Release (without sealing) any writer left open by a test, mirroring
    # test_archive_store.py's `_abandon` helper, to avoid a leaked-fd
    # ResourceWarning under `filterwarnings = ["error"]`.
    if s._current_writer is not None and not s._current_writer._file.closed:
        s._current_writer._file.close()
    conn.close()


def _queue_item(wheel_id: int, *, state=WheelState.NEED_METADATA) -> QueueItem:
    return QueueItem(id=wheel_id, project="widget", lane=0, state=state)


def test_process_one_writes_blob_and_transitions_to_need_convert(db_path, reader, writer, store):
    wheel_id = _insert_wheel(db_path, filename="handoff-1.0-py3-none-any.whl")
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    queue = ByteBudgetedQueue(budget_bytes=1000)
    archived: list[tuple[QueueItem, str, bytes]] = []
    handoff = ArchiveHandoff(
        queue,
        store,
        dispatcher,
        writer,
        lambda item, filename, data: archived.append((item, filename, data)),
    )
    data = b"Metadata-Version: 2.1\nName: widget\n"
    sha256 = hashlib.sha256(data).hexdigest()
    item = HandoffItem(
        queue_item=_queue_item(wheel_id),
        filename="handoff-1.0-py3-none-any.whl",
        data=data,
        sha256=sha256,
    )
    queue.put(item, size=len(data))

    assert handoff.process_one() is True

    row = reader.execute(
        "SELECT state, blob_sha256 FROM wheels WHERE id = ?", (wheel_id,)
    ).fetchone()
    assert row[0] == int(WheelState.NEED_CONVERT)
    assert row[1] == sha256

    blobs_row = reader.execute("SELECT sha256 FROM blobs WHERE sha256 = ?", (sha256,)).fetchone()
    assert blobs_row is not None

    assert len(archived) == 1
    assert archived[0][2] is data  # the identical bytes object, not a re-read copy


def test_process_one_returns_false_once_queue_closed_and_drained(db_path, reader, writer, store):
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    queue = ByteBudgetedQueue(budget_bytes=1000)
    handoff = ArchiveHandoff(queue, store, dispatcher, writer, lambda *a: None)
    queue.close()
    assert handoff.process_one() is False


def test_same_bytes_reach_archive_and_convert_without_rereading(db_path, reader, writer, store):
    wheel_id = _insert_wheel(db_path, filename="noreread-1.0-py3-none-any.whl")
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    queue = ByteBudgetedQueue(budget_bytes=1000)

    decompress_calls: list[int] = []

    def _counting_decompress(payload: bytes) -> bytes:
        decompress_calls.append(1)
        return decompress_block(payload)

    store.reader = SegmentReader(store._directory, decompress=_counting_decompress)

    archived: list[bytes] = []
    handoff = ArchiveHandoff(
        queue, store, dispatcher, writer, lambda item, filename, data: archived.append(data)
    )
    data = b"Metadata-Version: 2.1\nName: noreread\n"
    item = HandoffItem(
        queue_item=_queue_item(wheel_id),
        filename="noreread-1.0-py3-none-any.whl",
        data=data,
        sha256=hashlib.sha256(data).hexdigest(),
    )
    queue.put(item, size=len(data))
    handoff.process_one()

    assert archived == [data]
    assert archived[0] is data
    assert decompress_calls == []  # the archive's own reader was never consulted


def test_multiple_items_all_transition_and_forward(db_path, reader, writer, store):
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    queue = ByteBudgetedQueue(budget_bytes=10_000)
    archived: list[str] = []
    handoff = ArchiveHandoff(
        queue, store, dispatcher, writer, lambda item, filename, data: archived.append(filename)
    )
    wheel_ids = []
    for i in range(5):
        filename = f"multi-{i}-1.0-py3-none-any.whl"
        wheel_id = _insert_wheel(db_path, filename=filename)
        wheel_ids.append(wheel_id)
        data = f"Metadata-Version: 2.1\nName: multi{i}\n".encode()
        queue.put(
            HandoffItem(
                queue_item=_queue_item(wheel_id),
                filename=filename,
                data=data,
                sha256=hashlib.sha256(data).hexdigest(),
            ),
            size=len(data),
        )
    queue.close()
    handoff.run()

    assert len(archived) == 5
    for wheel_id in wheel_ids:
        row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
        assert row[0] == int(WheelState.NEED_CONVERT)


# ---------------------------------------------------------------------------
# Segment rotation
# ---------------------------------------------------------------------------


def test_should_seal_true_triggers_seal_and_new_segment_via_write_op(
    db_path, reader, writer, store
):
    wheel_id = _insert_wheel(db_path, filename="rotate-1.0-py3-none-any.whl")
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    queue = ByteBudgetedQueue(budget_bytes=10_000)
    handoff = ArchiveHandoff(queue, store, dispatcher, writer, lambda *a: None)

    # Force should_seal() to fire: a single small record never crosses
    # the compressed-bytes threshold on its own (blocks flush lazily, on
    # the *next* add), so drive the writer's own counter directly, the
    # same way test_archive_store.py reaches into writer internals.
    first_writer = store.current_writer()
    segment_id_before = first_writer.segment_id
    first_writer._compressed_bytes = first_writer._seal_bytes

    data = b"Metadata-Version: 2.1\nName: rotate\n"
    queue.put(
        HandoffItem(
            queue_item=_queue_item(wheel_id),
            filename="rotate-1.0-py3-none-any.whl",
            data=data,
            sha256=hashlib.sha256(data).hexdigest(),
        ),
        size=len(data),
    )
    handoff.process_one()

    sealed_row = reader.execute(
        "SELECT sealed_at, bytes, records, footer_sha FROM segments WHERE id = ?",
        (segment_id_before,),
    ).fetchone()
    assert sealed_row[0] is not None
    assert sealed_row[1] > 0
    assert sealed_row[2] == 1
    assert sealed_row[3] is not None

    new_writer = store.current_writer()
    assert new_writer.segment_id != segment_id_before


def test_records_written_after_rotation_land_in_the_new_segment_and_are_readable(
    db_path, reader, writer, store
):
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    queue = ByteBudgetedQueue(budget_bytes=10_000)
    handoff = ArchiveHandoff(queue, store, dispatcher, writer, lambda *a: None)

    first_writer = store.current_writer()
    first_segment_id = first_writer.segment_id
    first_writer._compressed_bytes = first_writer._seal_bytes

    wheel_id_1 = _insert_wheel(db_path, filename="rot-a-1.0-py3-none-any.whl")
    data1 = b"Metadata-Version: 2.1\nName: rota\n"
    queue.put(
        HandoffItem(
            queue_item=_queue_item(wheel_id_1),
            filename="rot-a-1.0-py3-none-any.whl",
            data=data1,
            sha256=hashlib.sha256(data1).hexdigest(),
        ),
        size=len(data1),
    )
    handoff.process_one()  # triggers a seal + rotation

    second_segment_id = store.current_writer().segment_id
    assert second_segment_id != first_segment_id

    wheel_id_2 = _insert_wheel(db_path, filename="rot-b-1.0-py3-none-any.whl")
    data2 = b"Metadata-Version: 2.1\nName: rotb\n"
    sha256_2 = hashlib.sha256(data2).hexdigest()
    queue.put(
        HandoffItem(
            queue_item=_queue_item(wheel_id_2),
            filename="rot-b-1.0-py3-none-any.whl",
            data=data2,
            sha256=sha256_2,
        ),
        size=len(data2),
    )
    handoff.process_one()

    location = store.location_for(sha256_2)
    assert location is not None
    assert location.segment_id == second_segment_id
    # The second segment isn't sealed yet -- reading through the store
    # only works for sealed segments, so seal it first to confirm
    # durability, mirroring what a real rotation eventually does.
    store.seal_writer(store.current_writer())
    assert store.get(sha256_2) == data2


def test_rotation_mid_stream_loses_no_records(db_path, reader, writer, store):
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
    queue = ByteBudgetedQueue(budget_bytes=10_000)
    handoff = ArchiveHandoff(queue, store, dispatcher, writer, lambda *a: None)

    sha256s = []
    for i in range(4):
        filename = f"midstream-{i}-1.0-py3-none-any.whl"
        wheel_id = _insert_wheel(db_path, filename=filename)
        data = f"Metadata-Version: 2.1\nName: midstream{i}\n".encode()
        sha256 = hashlib.sha256(data).hexdigest()
        sha256s.append(sha256)
        current = store.current_writer()
        current._compressed_bytes = current._seal_bytes  # force a seal after this add
        queue.put(
            HandoffItem(
                queue_item=_queue_item(wheel_id), filename=filename, data=data, sha256=sha256
            ),
            size=len(data),
        )
        handoff.process_one()

    # Seal whatever segment is still open so every record is on disk.
    store.seal_writer(store.current_writer())
    for sha256 in sha256s:
        assert store.location_for(sha256) is not None
        assert store.get(sha256) is not None


# ---------------------------------------------------------------------------
# Backpressure propagation: a saturated convert pool stops fetch
# ---------------------------------------------------------------------------


def test_fetch_stops_when_convert_pool_saturated(db_path, reader, writer, store):
    """A blocking ``on_archived`` (standing in for a saturated convert pool)
    keeps the archive thread from draining the queue, which fills the
    byte budget, which blocks a fetch worker's ``put`` -- proving
    backpressure propagates end to end through the fused handoff.
    """
    wheel_id_1 = _insert_wheel(db_path, filename="sat-1-1.0-py3-none-any.whl")
    wheel_id_2 = _insert_wheel(db_path, filename="sat-2-1.0-py3-none-any.whl")
    dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())

    # Budget big enough for exactly one item's worth of data.
    data1 = b"x" * 50
    data2 = b"y" * 50
    queue = ByteBudgetedQueue(budget_bytes=len(data1))

    convert_blocked = threading.Event()
    release_convert = threading.Event()

    def _blocking_on_archived(item, filename, data):
        convert_blocked.set()
        assert release_convert.wait(timeout=5)

    handoff = ArchiveHandoff(queue, store, dispatcher, writer, _blocking_on_archived)
    archive_thread = threading.Thread(target=handoff.run)
    archive_thread.start()

    try:
        # First item: archive thread picks it up and blocks inside on_archived.
        queue.put(
            HandoffItem(
                queue_item=_queue_item(wheel_id_1),
                filename="sat-1-1.0-py3-none-any.whl",
                data=data1,
                sha256=hashlib.sha256(data1).hexdigest(),
            ),
            size=len(data1),
        )
        assert convert_blocked.wait(timeout=5)

        # Second producer's put() must block: the archive thread hasn't
        # drained (it's stuck in on_archived), so the queue is still "full"
        # from the first item's perspective once this one is added.
        second_put_done = threading.Event()

        def _put_second():
            queue.put(
                HandoffItem(
                    queue_item=_queue_item(wheel_id_2),
                    filename="sat-2-1.0-py3-none-any.whl",
                    data=data2,
                    sha256=hashlib.sha256(data2).hexdigest(),
                ),
                size=len(data2),
            )
            second_put_done.set()

        producer_thread = threading.Thread(target=_put_second)
        producer_thread.start()
        try:
            assert not second_put_done.wait(timeout=0.2)
        finally:
            release_convert.set()
            producer_thread.join(timeout=5)
        assert second_put_done.is_set()
    finally:
        release_convert.set()
        queue.close()
        archive_thread.join(timeout=5)

    row1 = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id_1,)).fetchone()
    assert row1[0] == int(WheelState.NEED_CONVERT)


# ---------------------------------------------------------------------------
# Crash recovery for an unsealed segment
# ---------------------------------------------------------------------------


def test_recovery_resets_wheels_in_an_unsealed_segment(db_path, reader, writer, store):
    # 5 blobs land in one never-sealed segment.
    wheel_ids = []
    sha256s = []
    for i in range(5):
        filename = f"unsealed-{i}-1.0-py3-none-any.whl"
        wheel_id = _insert_wheel(db_path, filename=filename, state=WheelState.NEED_CONVERT)
        data = f"Metadata-Version: 2.1\nName: unsealed{i}\n".encode()
        location = store.add(data)
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (location.sha256, wheel_id))
        conn.commit()
        conn.close()
        wheel_ids.append(wheel_id)
        sha256s.append(location.sha256)

    segment_id = store.current_writer().segment_id
    # Never sealed -- simulate ArchiveStore's own constructor discovering
    # this segment is unsealed after a crash (the caller's job in the
    # real daemon; here we just call the recovery function directly).
    affected = recover_unsealed_segment(store, writer, segment_id)

    assert affected == 5
    for wheel_id in wheel_ids:
        row = reader.execute(
            "SELECT state, blob_sha256 FROM wheels WHERE id = ?", (wheel_id,)
        ).fetchone()
        assert row == (int(WheelState.NEED_METADATA), None)
    for sha256 in sha256s:
        blob_row = reader.execute("SELECT sha256 FROM blobs WHERE sha256 = ?", (sha256,)).fetchone()
        assert blob_row is None


def test_recovery_clears_stale_wheel_repodata_and_conda_name(db_path, reader, writer, store):
    """A wheel already at READY (wheel_repodata + conda_name set) whose blob
    lived in a since-lost, unsealed segment must not keep either after
    recovery resets it to NEED_METADATA -- fsck's 1b/19 invariants forbid
    both outside READY.
    """
    filename = "ready-1.0-py3-none-any.whl"
    wheel_id = _insert_wheel(db_path, filename=filename, state=WheelState.READY)
    data = b"Metadata-Version: 2.1\nName: ready\n"
    location = store.add(data)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE wheels SET blob_sha256 = ?, conda_name = ? WHERE id = ?",
        (location.sha256, "ready", wheel_id),
    )
    conn.execute(
        "INSERT INTO wheel_repodata (wheel_id, repodata_zst, reroll_version) VALUES (?, ?, ?)",
        (wheel_id, b"fake-repodata", REROLL_VERSION),
    )
    conn.commit()
    conn.close()
    segment_id = store.current_writer().segment_id

    recover_unsealed_segment(store, writer, segment_id)

    row = reader.execute(
        "SELECT state, blob_sha256, conda_name FROM wheels WHERE id = ?", (wheel_id,)
    ).fetchone()
    assert row == (int(WheelState.NEED_METADATA), None, None)

    repodata_row = reader.execute(
        "SELECT COUNT(*) FROM wheel_repodata WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert repodata_row[0] == 0


def test_recovery_does_not_touch_wheels_in_a_sealed_segment(db_path, reader, writer, store):
    filename = "sealed-1.0-py3-none-any.whl"
    wheel_id = _insert_wheel(db_path, filename=filename, state=WheelState.NEED_CONVERT)
    data = b"Metadata-Version: 2.1\nName: sealed\n"
    location = store.add(data)
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (location.sha256, wheel_id))
    conn.commit()
    conn.close()
    sealed_segment_id = store.current_writer().segment_id
    store.seal_writer(store.current_writer())

    # A second, unsealed segment with its own wheel, to recover.
    other_filename = "other-unsealed-1.0-py3-none-any.whl"
    other_wheel_id = _insert_wheel(db_path, filename=other_filename, state=WheelState.NEED_CONVERT)
    other_data = b"Metadata-Version: 2.1\nName: otherunsealed\n"
    other_location = store.add(other_data)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (other_location.sha256, other_wheel_id)
    )
    conn.commit()
    conn.close()
    unsealed_segment_id = store.current_writer().segment_id
    assert unsealed_segment_id != sealed_segment_id

    recover_unsealed_segment(store, writer, unsealed_segment_id)

    sealed_row = reader.execute(
        "SELECT state, blob_sha256 FROM wheels WHERE id = ?", (wheel_id,)
    ).fetchone()
    assert sealed_row == (int(WheelState.NEED_CONVERT), location.sha256)
    sealed_blob = reader.execute(
        "SELECT sha256 FROM blobs WHERE sha256 = ?", (location.sha256,)
    ).fetchone()
    assert sealed_blob is not None

    unsealed_row = reader.execute(
        "SELECT state, blob_sha256 FROM wheels WHERE id = ?", (other_wheel_id,)
    ).fetchone()
    assert unsealed_row == (int(WheelState.NEED_METADATA), None)


def test_recovery_query_uses_ix_blobs_segment(db_path, reader, store):
    # Pins the exact query store.blob_rows_for_segment runs (the one
    # recover_unsealed_segment relies on) to the segment index.
    plan = reader.execute(
        "EXPLAIN QUERY PLAN SELECT sha256, block_no, offset, length FROM blobs "
        "WHERE segment_id = ? ORDER BY sha256",
        (0,),
    ).fetchall()
    details = [row[-1] for row in plan]
    assert any("ix_blobs_segment" in d for d in details)


def test_recovery_is_chunked(db_path, reader, writer, store, monkeypatch):
    for i in range(5):
        filename = f"chunked-{i}-1.0-py3-none-any.whl"
        wheel_id = _insert_wheel(db_path, filename=filename, state=WheelState.NEED_CONVERT)
        data = f"Metadata-Version: 2.1\nName: chunked{i}\n".encode()
        location = store.add(data)
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (location.sha256, wheel_id))
        conn.commit()
        conn.close()
    segment_id = store.current_writer().segment_id

    calls = []
    real_submit_and_wait = writer.submit_and_wait

    def _counting_submit_and_wait(op):
        calls.append(op)
        return real_submit_and_wait(op)

    writer.submit_and_wait = _counting_submit_and_wait
    affected = recover_unsealed_segment(store, writer, segment_id, chunk_size=2)

    assert affected == 5
    assert len(calls) == 3  # ceil(5 / 2)


def test_recovery_with_no_blobs_in_segment_is_a_noop(db_path, writer, store):
    segment_id = store.current_writer().segment_id
    affected = recover_unsealed_segment(store, writer, segment_id)
    assert affected == 0


def test_recovery_never_resurrects_a_deleted_wheel(db_path, reader, writer, store):
    filename = "deleted-1.0-py3-none-any.whl"
    wheel_id = _insert_wheel(db_path, filename=filename, state=WheelState.NEED_CONVERT)
    data = b"Metadata-Version: 2.1\nName: deleted\n"
    location = store.add(data)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE wheels SET blob_sha256 = ?, state = ? WHERE id = ?",
        (location.sha256, int(WheelState.DELETED), wheel_id),
    )
    conn.commit()
    conn.close()
    segment_id = store.current_writer().segment_id

    recover_unsealed_segment(store, writer, segment_id)

    row = reader.execute(
        "SELECT state, blob_sha256 FROM wheels WHERE id = ?", (wheel_id,)
    ).fetchone()
    assert row[0] == int(WheelState.DELETED)


# ---------------------------------------------------------------------------
# BulkConvertSource
# ---------------------------------------------------------------------------


def _sealed_store_with_blobs(tmp_path, db_path, n: int, *, block_target_bytes: int | None = None):
    """Build a sealed segment holding ``n`` distinct blobs, and link each to
    its own ``NEED_CONVERT`` wheel. Returns ``(store, wheel_ids, sha256s)``.
    """
    conn = _store_conn(db_path)
    store = ArchiveStore(tmp_path / "segments", conn)
    if block_target_bytes is not None:
        store.current_writer()._block_target_bytes = block_target_bytes
    wheel_ids = []
    sha256s = []
    for i in range(n):
        filename = f"bulk-{i}-1.0-py3-none-any.whl"
        wheel_id = _insert_wheel(db_path, filename=filename, state=WheelState.NEED_CONVERT)
        data = f"Metadata-Version: 2.1\nName: bulk{i}\n".encode() * 50
        location = store.add(data)
        conn2 = sqlite3.connect(db_path)
        conn2.execute("UPDATE wheels SET blob_sha256 = ? WHERE id = ?", (location.sha256, wheel_id))
        conn2.commit()
        conn2.close()
        wheel_ids.append(wheel_id)
        sha256s.append(location.sha256)
    store.seal_writer(store.current_writer())
    return store, wheel_ids, sha256s, conn


def test_bulk_convert_source_claims_and_reads_back_bytes(db_path, reader, writer, tmp_path):
    store, wheel_ids, sha256s, store_conn = _sealed_store_with_blobs(tmp_path, db_path, 3)
    try:
        dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
        source = BulkConvertSource(store, dispatcher, reader)

        batch = source.claim_batch(10)

        assert len(batch) == 3
        filenames = {item.filename for item in batch}
        assert filenames == {f"bulk-{i}-1.0-py3-none-any.whl" for i in range(3)}
        for item in batch:
            assert item.data.startswith(b"Metadata-Version: 2.1")
    finally:
        store_conn.close()


def test_bulk_convert_decompresses_each_block_exactly_once_per_batch(
    db_path, reader, writer, tmp_path
):
    # 10 items spread across 3+ blocks (a tiny block target forces a new
    # block every couple of records) -- each block must decompress exactly
    # once for the whole batch, no matter how many records it holds.
    store, wheel_ids, sha256s, store_conn = _sealed_store_with_blobs(
        tmp_path, db_path, 10, block_target_bytes=200
    )
    try:
        segment_id = store.sealed_segment_ids()[0]
        footer_blocks = {
            block_no for _sha, block_no, _o, _l in store.reader.footer_records(segment_id)
        }
        assert len(footer_blocks) >= 2  # otherwise this test proves nothing

        decompress_calls: list[int] = []
        from reroll_sync.archive.format import decompress_block

        def _counting_decompress(payload: bytes) -> bytes:
            decompress_calls.append(1)
            return decompress_block(payload)

        counting_reader = SegmentReader(tmp_path / "segments", decompress=_counting_decompress)
        dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
        source = BulkConvertSource(store, dispatcher, reader, reader=counting_reader)
        batch = source.claim_batch(20)

        assert len(batch) == 10
        assert len(decompress_calls) == len(footer_blocks)
    finally:
        store_conn.close()


def test_bulk_convert_groups_correctly_even_when_claim_order_is_scrambled(
    db_path, reader, writer, tmp_path, monkeypatch
):
    """Claimed items are grouped by (segment_id, block_no) regardless of the
    order the dispatcher's own claim query happens to return them in.

    Wheels/blobs are written in block order (0..5), so claiming them in
    plain ascending-id order can't distinguish correct grouping from
    grouping code that merely relies on adjacent claimed items sharing a
    block. Interleaving from both ends of the claimed list breaks that
    adjacency -- item i and item len-1-i are claimed back-to-back even
    though they almost certainly land in different blocks -- so this test
    can actually fail if the grouping regresses to trusting claim order.
    """
    store, wheel_ids, sha256s, store_conn = _sealed_store_with_blobs(
        tmp_path, db_path, 6, block_target_bytes=150
    )
    try:
        decompress_calls: list[int] = []
        from reroll_sync.archive.format import decompress_block

        def _counting_decompress(payload: bytes) -> bytes:
            decompress_calls.append(1)
            return decompress_block(payload)

        segment_id = store.sealed_segment_ids()[0]
        n_blocks = len({b for _s, b, _o, _l in store.reader.footer_records(segment_id)})
        assert n_blocks >= 2  # otherwise scrambling can't prove anything
        counting_reader = SegmentReader(tmp_path / "segments", decompress=_counting_decompress)

        dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
        real_claim = dispatcher.claim

        def _scrambled_claim(stage, limit):
            items = real_claim(stage, limit)
            scrambled: list[QueueItem] = []
            lo, hi = 0, len(items) - 1
            take_from_hi = True
            while lo <= hi:
                if take_from_hi:
                    scrambled.append(items[hi])
                    hi -= 1
                else:
                    scrambled.append(items[lo])
                    lo += 1
                take_from_hi = not take_from_hi
            assert [item.id for item in scrambled] != [item.id for item in items]
            return scrambled

        monkeypatch.setattr(dispatcher, "claim", _scrambled_claim)
        source = BulkConvertSource(store, dispatcher, reader, reader=counting_reader)
        batch = source.claim_batch(20)

        assert len(batch) == 6
        assert len(decompress_calls) == n_blocks
    finally:
        store_conn.close()


def test_bulk_convert_missing_segment_file_yields_retry_not_crash(
    db_path, reader, writer, tmp_path
):
    store, wheel_ids, sha256s, store_conn = _sealed_store_with_blobs(tmp_path, db_path, 1)
    try:
        segment_id = store.sealed_segment_ids()[0]
        segment_path = tmp_path / "segments" / f"{segment_id:06d}.zst"
        segment_path.unlink()

        dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
        source = BulkConvertSource(store, dispatcher, reader)
        batch = source.claim_batch(10)

        assert batch == []
        work_row = reader.execute(
            "SELECT attempts FROM work WHERE wheel_id = ? AND stage = 'convert'", (wheel_ids[0],)
        ).fetchone()
        assert work_row[0] == 1
        wheel_row = reader.execute(
            "SELECT state FROM wheels WHERE id = ?", (wheel_ids[0],)
        ).fetchone()
        assert wheel_row[0] == int(WheelState.NEED_CONVERT)  # Retry never changes state
    finally:
        store_conn.close()


def test_bulk_convert_corrupt_blob_yields_skip_and_does_not_poison_batch(
    db_path, reader, writer, tmp_path
):
    store, wheel_ids, sha256s, store_conn = _sealed_store_with_blobs(tmp_path, db_path, 3)
    try:
        # Corrupt one blob's blobs row: point it at a bogus offset so its
        # sha256 check fails, without touching the other two records.
        bad_sha256 = sha256s[0]
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE blobs SET offset = offset + 1 WHERE sha256 = ?", (bad_sha256,))
        conn.commit()
        conn.close()

        dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
        source = BulkConvertSource(store, dispatcher, reader)
        batch = source.claim_batch(10)

        assert len(batch) == 2  # the other two still come through
        skip_row = reader.execute(
            "SELECT state FROM wheels WHERE id = ?", (wheel_ids[0],)
        ).fetchone()
        assert skip_row[0] == int(WheelState.SKIPPED)
        error_row = reader.execute(
            "SELECT error_category FROM errors WHERE wheel_id = ?", (wheel_ids[0],)
        ).fetchone()
        assert error_row[0] == "corrupt_blob"
    finally:
        store_conn.close()


# ---------------------------------------------------------------------------
# End-to-end (in-process, no network)
# ---------------------------------------------------------------------------


def _metadata_text(name="example", version="1.0", requires_dist=()):
    lines = [f"Name: {name}", f"Version: {version}"]
    for requirement in requires_dist:
        lines.append(f"Requires-Dist: {requirement}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def test_end_to_end_valid_metadata_reaches_ready(db_path, reader, writer, store):
    filename = "example-1.0-py3-none-any.whl"
    wheel_id = _insert_wheel(db_path, filename=filename)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_metadata_text(), request=request)

    dispatcher = Dispatcher(reader, writer, reroll_version=REROLL_VERSION, now=_clock())

    def _on_archived(queue_item, fname, data):
        outcome = convert(data, fname, mappers=_E2E_MAPPERS, reroll_version=REROLL_VERSION)
        adapted = adapt_convert_outcome(outcome, reroll_version=REROLL_VERSION)
        convert_item = _queue_item(queue_item.id, state=WheelState.NEED_CONVERT)
        dispatcher.apply_outcome(Stage.CONVERT, convert_item, adapted)

    client = _client(handler)
    queue = ByteBudgetedQueue(budget_bytes=10_000)
    item = _item(id=wheel_id, filename=filename, url="https://files.pythonhosted.org/x/" + filename)

    def _enqueue(fetch_item, data, sha256):
        queue.put(
            HandoffItem(
                queue_item=_queue_item(fetch_item.id),
                filename=fetch_item.filename,
                data=data,
                sha256=sha256,
            ),
            size=len(data),
        )

    dispatch_fetch_item(
        client,
        item,
        dispatcher=dispatcher,
        enqueue=_enqueue,
        now=_clock(),
    )
    handoff = ArchiveHandoff(queue, store, dispatcher, writer, _on_archived)
    handoff.process_one()

    row = reader.execute(
        "SELECT state, blob_sha256 FROM wheels WHERE id = ?", (wheel_id,)
    ).fetchone()
    assert row[0] == int(WheelState.READY)
    assert row[1] is not None

    repodata_row = reader.execute(
        "SELECT reroll_version FROM wheel_repodata WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert repodata_row == (REROLL_VERSION,)

    blobs_row = reader.execute("SELECT sha256 FROM blobs WHERE sha256 = ?", (row[1],)).fetchone()
    assert blobs_row is not None

    work_row = reader.execute(
        "SELECT COUNT(*) FROM work WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert work_row[0] == 0


def test_end_to_end_rejected_metadata_reaches_skipped(db_path, reader, writer, store):
    filename = "badenc-1.0-py3-none-any.whl"
    wheel_id = _insert_wheel(db_path, filename=filename)

    invalid_utf8 = b"\xff\xfe\x00bad bytes"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=invalid_utf8, request=request)

    dispatcher = Dispatcher(reader, writer, reroll_version=REROLL_VERSION, now=_clock())

    def _on_archived(queue_item, fname, data):
        outcome = convert(data, fname, mappers=_E2E_MAPPERS, reroll_version=REROLL_VERSION)
        adapted = adapt_convert_outcome(outcome, reroll_version=REROLL_VERSION)
        convert_item = _queue_item(queue_item.id, state=WheelState.NEED_CONVERT)
        dispatcher.apply_outcome(Stage.CONVERT, convert_item, adapted)

    client = _client(handler)
    queue = ByteBudgetedQueue(budget_bytes=10_000)
    item = _item(id=wheel_id, filename=filename, url="https://files.pythonhosted.org/x/" + filename)

    def _enqueue(fetch_item, data, sha256):
        queue.put(
            HandoffItem(
                queue_item=_queue_item(fetch_item.id),
                filename=fetch_item.filename,
                data=data,
                sha256=sha256,
            ),
            size=len(data),
        )

    dispatch_fetch_item(
        client,
        item,
        dispatcher=dispatcher,
        enqueue=_enqueue,
        now=_clock(),
    )
    handoff = ArchiveHandoff(queue, store, dispatcher, writer, _on_archived)
    handoff.process_one()

    row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
    assert row[0] == int(WheelState.SKIPPED)

    skip_row = reader.execute(
        "SELECT reason, permanent FROM skips WHERE wheel_id = ? AND stage = 'convert'", (wheel_id,)
    ).fetchone()
    assert skip_row is not None
    assert skip_row[1] == 1

    error_row = reader.execute(
        "SELECT COUNT(*) FROM errors WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert error_row[0] == 1

    repodata_row = reader.execute(
        "SELECT COUNT(*) FROM wheel_repodata WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert repodata_row[0] == 0


def test_end_to_end_prerelease_wheel_sets_requires_prerelease_flag(db_path, reader, writer, store):
    filename = "prerel-1.0-py3-none-any.whl"
    wheel_id = _insert_wheel(db_path, filename=filename)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_metadata_text(name="prerel"), request=request)

    dispatcher = Dispatcher(reader, writer, reroll_version=REROLL_VERSION, now=_clock())

    calls = []

    def _fake_get_wheel_records(metadata, fname, *, allow_pre, **kwargs):
        calls.append(allow_pre)
        if not allow_pre:
            raise UnsupportedPrereleaseError("rejected pre-release version")
        return (
            reroll.WheelRecord(
                name="prerel",
                version="1.0",
                build="py_0",
                build_number=0,
                subdir="noarch",
                fn=fname,
                noarch="python",
                depends=(),
                extra_depends={},
                name_resolutions=(),
            ),
        )

    def _on_archived(queue_item, fname, data):
        outcome = convert(
            data,
            fname,
            mappers=_E2E_MAPPERS,
            reroll_version=REROLL_VERSION,
            get_wheel_records=_fake_get_wheel_records,
        )
        assert isinstance(outcome, ConvertOk)
        adapted = adapt_convert_outcome(outcome, reroll_version=REROLL_VERSION)
        convert_item = _queue_item(queue_item.id, state=WheelState.NEED_CONVERT)
        dispatcher.apply_outcome(Stage.CONVERT, convert_item, adapted)

    client = _client(handler)
    queue = ByteBudgetedQueue(budget_bytes=10_000)
    item = _item(id=wheel_id, filename=filename, url="https://files.pythonhosted.org/x/" + filename)

    def _enqueue(fetch_item, data, sha256):
        queue.put(
            HandoffItem(
                queue_item=_queue_item(fetch_item.id),
                filename=fetch_item.filename,
                data=data,
                sha256=sha256,
            ),
            size=len(data),
        )

    dispatch_fetch_item(
        client,
        item,
        dispatcher=dispatcher,
        enqueue=_enqueue,
        now=_clock(),
    )
    handoff = ArchiveHandoff(queue, store, dispatcher, writer, _on_archived)
    handoff.process_one()

    assert calls == [False, True]
    row = reader.execute(
        "SELECT requires_prerelease FROM wheel_repodata WHERE wheel_id = ?", (wheel_id,)
    ).fetchone()
    assert row == (1,)


# ---------------------------------------------------------------------------
# BulkConvertSource: edge cases for full coverage
# ---------------------------------------------------------------------------


def test_bulk_convert_claim_batch_on_empty_queue_returns_empty_list(
    db_path, reader, writer, tmp_path
):
    conn = _store_conn(db_path)
    store = ArchiveStore(tmp_path / "segments", conn)
    try:
        dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
        source = BulkConvertSource(store, dispatcher, reader)
        assert source.claim_batch(10) == []
    finally:
        conn.close()


def test_bulk_convert_item_with_null_blob_sha256_is_released_and_logged(
    db_path, reader, writer, tmp_path, caplog
):
    _insert_wheel(db_path, filename="nulled-1.0-py3-none-any.whl", state=WheelState.NEED_CONVERT)
    conn = _store_conn(db_path)
    store = ArchiveStore(tmp_path / "segments", conn)
    try:
        dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
        source = BulkConvertSource(store, dispatcher, reader)
        with caplog.at_level(logging.ERROR, logger="reroll_sync.fetch"):
            batch = source.claim_batch(10)
        assert batch == []
        assert any("no blob_sha256" in r.getMessage() for r in caplog.records)
    finally:
        conn.close()


def test_bulk_convert_blob_sha256_with_no_blobs_row_yields_retry(db_path, reader, writer, tmp_path):
    wheel_id = _insert_wheel(
        db_path,
        filename="danglingblob-1.0-py3-none-any.whl",
        state=WheelState.NEED_CONVERT,
        blob_sha256="f" * 64,
    )
    conn = _store_conn(db_path)
    store = ArchiveStore(tmp_path / "segments", conn)
    try:
        dispatcher = Dispatcher(reader, writer, reroll_version="1.0", now=_clock())
        source = BulkConvertSource(store, dispatcher, reader)
        batch = source.claim_batch(10)
        assert batch == []
        work_row = reader.execute(
            "SELECT attempts FROM work WHERE wheel_id = ? AND stage = 'convert'", (wheel_id,)
        ).fetchone()
        assert work_row[0] == 1
    finally:
        conn.close()


def test_byte_budgeted_queue_budget_bytes_property():
    queue = ByteBudgetedQueue(budget_bytes=123)
    assert queue.budget_bytes == 123
