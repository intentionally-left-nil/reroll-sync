import json
import sqlite3

import pytest
from reroll import WheelMetadata
from reroll.errors import NetworkFetchError

from reroll_sync.db import init_db
from reroll_sync.metadata_parse import parse_metadata
from reroll_sync.r2_client import R2Config, R2DownloadError
from reroll_sync.version import REROLL_VERSION

R2_CONFIG = R2Config(
    account_id="acct", access_key_id="key", secret_access_key="secret", bucket="bucket"
)

_VALID_METADATA = "Metadata-Version: 2.1\nName: numpy\nVersion: 1.26.0\n"


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    connection = sqlite3.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()


def _insert_wheel(conn, filename, *, metadata_downloaded_at="2024-01-01T00:00:00+00:00"):
    conn.execute(
        "INSERT INTO wheels (filename, project, pypi_simple, metadata_downloaded_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (filename, "numpy", "{}", metadata_downloaded_at, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    (rowid,) = conn.execute("SELECT rowid FROM wheels WHERE filename = ?", (filename,)).fetchone()
    return rowid


def _wheel_row(conn, filename):
    return conn.execute(
        "SELECT skip_reason, wheel_metadata, metadata_reroll_version FROM wheels "
        "WHERE filename = ?",
        (filename,),
    ).fetchone()


def _errors(conn):
    return conn.execute(
        "SELECT wheel_filename, error_category, error_subcategory, details, reroll_version "
        "FROM errors"
    ).fetchall()


def test_wheel_with_downloaded_metadata_is_parsed_and_stored(conn):
    rowid = _insert_wheel(conn, "numpy-1.26.0-py3-none-any.whl")
    requested_keys: list = []

    def download(config, key):
        requested_keys.append((config, key))
        return _VALID_METADATA.encode("utf-8")

    stats = parse_metadata(conn, R2_CONFIG, download=download)

    assert stats.wheels_considered == 1
    assert stats.wheels_parsed == 1
    assert stats.wheels_failed == 0
    assert requested_keys == [(R2_CONFIG, str(rowid))]

    skip_reason, wheel_metadata, reroll_version = _wheel_row(conn, "numpy-1.26.0-py3-none-any.whl")
    assert skip_reason is None
    assert reroll_version == REROLL_VERSION
    parsed = WheelMetadata.model_validate_json(wheel_metadata)
    assert parsed.name == "numpy"
    assert str(parsed.version) == "1.26.0"


def test_wheels_without_downloaded_metadata_are_not_considered(conn):
    _insert_wheel(conn, "numpy-1.0-py3-none-any.whl", metadata_downloaded_at=None)

    stats = parse_metadata(
        conn, R2_CONFIG, download=lambda config, key: pytest.fail("should not download")
    )

    assert stats.wheels_considered == 0


def test_wheels_already_parsed_are_not_reconsidered(conn):
    conn.execute(
        "INSERT INTO wheels "
        "(filename, project, pypi_simple, metadata_downloaded_at, wheel_metadata, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            "numpy-1.0-py3-none-any.whl",
            "numpy",
            "{}",
            "2024-01-01T00:00:00+00:00",
            '{"name": "numpy", "version": "1.0"}',
            "2024-01-01T00:00:00+00:00",
        ),
    )
    conn.commit()

    stats = parse_metadata(
        conn, R2_CONFIG, download=lambda config, key: pytest.fail("should not download")
    )

    assert stats.wheels_considered == 0


def test_wheels_with_skip_reason_are_not_reconsidered(conn):
    conn.execute(
        "INSERT INTO wheels "
        "(filename, project, pypi_simple, metadata_downloaded_at, skip_reason, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            "numpy-1.0-py3-none-any.whl",
            "numpy",
            "{}",
            "2024-01-01T00:00:00+00:00",
            "invalid_metadata",
            "2024-01-01T00:00:00+00:00",
        ),
    )
    conn.commit()

    stats = parse_metadata(
        conn, R2_CONFIG, download=lambda config, key: pytest.fail("should not download")
    )

    assert stats.wheels_considered == 0


def test_invalid_metadata_is_recorded_as_error_and_permanently_skipped(conn):
    _insert_wheel(conn, "bad-1.0-py3-none-any.whl")

    stats = parse_metadata(conn, R2_CONFIG, download=lambda config, key: b"Metadata-Version: 2.1\n")

    assert stats.wheels_parsed == 0
    assert stats.wheels_failed == 1

    skip_reason, wheel_metadata, reroll_version = _wheel_row(conn, "bad-1.0-py3-none-any.whl")
    assert skip_reason == "invalid_metadata"
    assert wheel_metadata is None
    assert reroll_version is None

    errors = _errors(conn)
    assert len(errors) == 1
    filename, category, subcategory, details, error_reroll_version = errors[0]
    assert filename == "bad-1.0-py3-none-any.whl"
    assert category == "invalid_metadata"
    assert subcategory == "InvalidMetadataError"
    assert "invalid METADATA" in details
    assert error_reroll_version == REROLL_VERSION


def test_non_utf8_metadata_is_recorded_as_error_and_permanently_skipped(conn):
    _insert_wheel(conn, "bad-1.0-py3-none-any.whl")

    stats = parse_metadata(conn, R2_CONFIG, download=lambda config, key: b"\xff\xfe\x00")

    assert stats.wheels_parsed == 0
    assert stats.wheels_failed == 1

    skip_reason, wheel_metadata, reroll_version = _wheel_row(conn, "bad-1.0-py3-none-any.whl")
    assert skip_reason == "invalid_metadata_encoding"
    assert wheel_metadata is None

    errors = _errors(conn)
    assert len(errors) == 1
    filename, category, subcategory, _details, _error_reroll_version = errors[0]
    assert filename == "bad-1.0-py3-none-any.whl"
    assert category == "invalid_metadata"
    assert subcategory == "invalid_metadata_encoding"


def test_reroll_runtime_error_is_left_for_retry(conn):
    _insert_wheel(conn, "numpy-1.0-py3-none-any.whl")

    def parse(text):
        raise NetworkFetchError("could not resolve a name mapper")

    stats = parse_metadata(
        conn,
        R2_CONFIG,
        download=lambda config, key: _VALID_METADATA.encode("utf-8"),
        parse=parse,
    )

    assert stats.wheels_parsed == 0
    assert stats.wheels_failed == 0
    skip_reason, wheel_metadata, _reroll_version = _wheel_row(conn, "numpy-1.0-py3-none-any.whl")
    assert skip_reason is None
    assert wheel_metadata is None
    assert _errors(conn) == []


def test_download_failure_is_left_for_retry(conn):
    _insert_wheel(conn, "numpy-1.0-py3-none-any.whl")

    def download(config, key):
        raise R2DownloadError("boom")

    stats = parse_metadata(conn, R2_CONFIG, download=download)

    assert stats.wheels_parsed == 0
    assert stats.wheels_failed == 0
    skip_reason, wheel_metadata, _reroll_version = _wheel_row(conn, "numpy-1.0-py3-none-any.whl")
    assert skip_reason is None
    assert wheel_metadata is None
    assert _errors(conn) == []


def test_limit_caps_number_of_wheels_processed(conn):
    for i in range(3):
        _insert_wheel(conn, f"pkg{i}-1.0-py3-none-any.whl")
    processed: list = []

    def download(config, key):
        processed.append(key)
        return _VALID_METADATA.encode("utf-8")

    stats = parse_metadata(conn, R2_CONFIG, limit=2, download=download)

    assert stats.wheels_considered == 2
    assert len(processed) == 2


def test_timeout_stops_processing_early(conn, monkeypatch):
    for i in range(2):
        _insert_wheel(conn, f"pkg{i}-1.0-py3-none-any.whl")
    times = iter([0.0, 100.0])
    monkeypatch.setattr("reroll_sync.metadata_parse.time.monotonic", lambda: next(times))
    processed: list = []

    def download(config, key):
        processed.append(key)
        return _VALID_METADATA.encode("utf-8")

    stats = parse_metadata(conn, R2_CONFIG, timeout=5, download=download)

    assert processed == []
    assert stats.wheels_parsed == 0
    assert stats.stopped_early is True


def test_parsed_metadata_json_round_trips_through_model_validate(conn):
    _insert_wheel(conn, "numpy-1.26.0-py3-none-any.whl")

    parse_metadata(conn, R2_CONFIG, download=lambda config, key: _VALID_METADATA.encode("utf-8"))

    (wheel_metadata,) = conn.execute(
        "SELECT wheel_metadata FROM wheels WHERE filename = ?", ("numpy-1.26.0-py3-none-any.whl",)
    ).fetchone()
    assert json.loads(wheel_metadata)["name"] == "numpy"
