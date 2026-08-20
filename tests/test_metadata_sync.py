import hashlib
import json
import sqlite3

import pytest

from reroll_sync.db import init_db
from reroll_sync.metadata_sync import sync_metadata
from reroll_sync.r2_client import R2Config, R2UploadError

R2_CONFIG = R2Config(
    account_id="acct", access_key_id="key", secret_access_key="secret", bucket="bucket"
)


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    connection = sqlite3.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()


def _insert_wheel(conn, filename, raw):
    conn.execute(
        "INSERT INTO wheels (filename, project, pypi_simple, updated_at) VALUES (?, ?, ?, ?)",
        (filename, "numpy", json.dumps(raw), "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    (actual_rowid,) = conn.execute(
        "SELECT rowid FROM wheels WHERE filename = ?", (filename,)
    ).fetchone()
    return actual_rowid


def _wheel_row(conn, filename):
    return conn.execute(
        "SELECT skip_reason, metadata_downloaded_at FROM wheels WHERE filename = ?",
        (filename,),
    ).fetchone()


def _errors(conn):
    return conn.execute("SELECT wheel_filename, error_category, details FROM errors").fetchall()


def test_wheel_without_pep658_metadata_is_skipped(conn):
    _insert_wheel(
        conn,
        "numpy-1.0-py3-none-any.whl",
        {"url": "https://x/numpy.whl", "core-metadata": False, "dist-info-metadata": False},
    )

    def fetch_metadata_bytes(url, timeout):
        raise AssertionError("should not fetch metadata when unavailable")

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=fetch_metadata_bytes,
        upload=lambda config, key, data: pytest.fail("should not upload"),
    )

    assert stats.wheels_considered == 1
    assert stats.wheels_skipped_no_metadata == 1
    assert stats.wheels_uploaded == 0
    skip_reason, downloaded_at = _wheel_row(conn, "numpy-1.0-py3-none-any.whl")
    assert skip_reason == "no_pep658_metadata"
    assert downloaded_at is None


def test_wheel_with_matching_hash_is_uploaded_with_rowid_key(conn):
    data = b"Metadata-Version: 2.1\nName: numpy\n"
    sha256 = hashlib.sha256(data).hexdigest()
    rowid = _insert_wheel(
        conn,
        "numpy-1.0-py3-none-any.whl",
        {"url": "https://x/numpy.whl", "core-metadata": {"sha256": sha256}},
    )
    uploaded: list = []

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=lambda url, timeout: data,
        upload=lambda config, key, body: uploaded.append((config, key, body)),
    )

    assert stats.wheels_uploaded == 1
    assert uploaded == [(R2_CONFIG, str(rowid), data)]
    skip_reason, downloaded_at = _wheel_row(conn, "numpy-1.0-py3-none-any.whl")
    assert skip_reason is None
    assert downloaded_at


def test_wheel_metadata_url_is_wheel_url_plus_metadata_suffix(conn):
    _insert_wheel(
        conn,
        "numpy-1.0-py3-none-any.whl",
        {"url": "https://x/numpy.whl", "core-metadata": {}},
    )
    requested_urls: list = []

    def fetch_metadata_bytes(url, timeout):
        requested_urls.append(url)
        return b"data"

    sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=fetch_metadata_bytes,
        upload=lambda config, key, body: None,
    )

    assert requested_urls == ["https://x/numpy.whl.metadata"]


def test_wheel_with_mismatched_hash_records_error_and_is_not_uploaded(conn):
    _insert_wheel(
        conn,
        "numpy-1.0-py3-none-any.whl",
        {"url": "https://x/numpy.whl", "core-metadata": {"sha256": "expected"}},
    )

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=lambda url, timeout: b"wrong bytes",
        upload=lambda config, key, body: pytest.fail("should not upload on hash mismatch"),
    )

    assert stats.wheels_failed_hash_mismatch == 1
    assert stats.wheels_uploaded == 0
    skip_reason, downloaded_at = _wheel_row(conn, "numpy-1.0-py3-none-any.whl")
    assert skip_reason is None
    assert downloaded_at is None
    errors = _errors(conn)
    assert len(errors) == 1
    filename, category, details = errors[0]
    assert filename == "numpy-1.0-py3-none-any.whl"
    assert category == "metadata_hash_mismatch"
    assert "expected" in details


def test_wheel_with_no_published_hash_is_uploaded_unverified(conn):
    rowid = _insert_wheel(
        conn,
        "numpy-1.0-py3-none-any.whl",
        {"url": "https://x/numpy.whl", "core-metadata": True},
    )
    uploaded: list = []

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=lambda url, timeout: b"unverifiable data",
        upload=lambda config, key, body: uploaded.append((key, body)),
    )

    assert stats.wheels_uploaded == 1
    assert uploaded == [(str(rowid), b"unverifiable data")]


def test_wheels_with_metadata_already_downloaded_are_not_reconsidered(conn):
    conn.execute(
        "INSERT INTO wheels (filename, project, pypi_simple, metadata_downloaded_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            "numpy-1.0-py3-none-any.whl",
            "numpy",
            '{"url": "https://x/numpy.whl", "core-metadata": true}',
            "2024-01-01T00:00:00+00:00",
            "2024-01-01T00:00:00+00:00",
        ),
    )
    conn.commit()

    def fetch_metadata_bytes(url, timeout):
        raise AssertionError("should not fetch already-downloaded metadata")

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=fetch_metadata_bytes,
        upload=lambda config, key, body: pytest.fail("should not upload"),
    )

    assert stats.wheels_considered == 0


def test_wheels_with_skip_reason_are_not_reconsidered(conn):
    conn.execute(
        "INSERT INTO wheels (filename, project, pypi_simple, skip_reason, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            "numpy-1.0-py3-none-any.whl",
            "numpy",
            '{"url": "https://x/numpy.whl", "core-metadata": true}',
            "no_pep658_metadata",
            "2024-01-01T00:00:00+00:00",
        ),
    )
    conn.commit()

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=lambda url, timeout: pytest.fail("should not fetch"),
        upload=lambda config, key, body: pytest.fail("should not upload"),
    )

    assert stats.wheels_considered == 0


def test_download_failure_is_skipped_and_left_for_retry(conn):
    _insert_wheel(
        conn,
        "numpy-1.0-py3-none-any.whl",
        {"url": "https://x/numpy.whl", "core-metadata": {"sha256": "abc"}},
    )

    def fetch_metadata_bytes(url, timeout):
        raise OSError("network error")

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=fetch_metadata_bytes,
        upload=lambda config, key, body: pytest.fail("should not upload"),
    )

    assert stats.wheels_uploaded == 0
    skip_reason, downloaded_at = _wheel_row(conn, "numpy-1.0-py3-none-any.whl")
    assert skip_reason is None
    assert downloaded_at is None


def test_upload_failure_is_skipped_and_left_for_retry(conn):
    _insert_wheel(
        conn,
        "numpy-1.0-py3-none-any.whl",
        {"url": "https://x/numpy.whl", "core-metadata": {}},
    )

    def upload(config, key, body):
        raise R2UploadError("boom")

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        fetch_metadata_bytes=lambda url, timeout: b"data",
        upload=upload,
    )

    assert stats.wheels_uploaded == 0
    skip_reason, downloaded_at = _wheel_row(conn, "numpy-1.0-py3-none-any.whl")
    assert skip_reason is None
    assert downloaded_at is None


def test_limit_caps_number_of_wheels_processed(conn):
    for i in range(3):
        _insert_wheel(
            conn,
            f"pkg{i}-1.0-py3-none-any.whl",
            {"url": f"https://x/pkg{i}.whl", "core-metadata": {}},
        )
    processed: list = []

    def fetch_metadata_bytes(url, timeout):
        processed.append(url)
        return b"data"

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        limit=2,
        fetch_metadata_bytes=fetch_metadata_bytes,
        upload=lambda config, key, body: None,
    )

    assert stats.wheels_considered == 2
    assert len(processed) == 2


def test_timeout_stops_processing_early(conn, monkeypatch):
    for i in range(2):
        _insert_wheel(
            conn,
            f"pkg{i}-1.0-py3-none-any.whl",
            {"url": f"https://x/pkg{i}.whl", "core-metadata": {}},
        )
    times = iter([0.0, 100.0])
    monkeypatch.setattr("reroll_sync.metadata_sync.time.monotonic", lambda: next(times))
    processed: list = []

    def fetch_metadata_bytes(url, timeout):
        processed.append(url)
        return b"data"

    stats = sync_metadata(
        conn,
        R2_CONFIG,
        timeout=5,
        fetch_metadata_bytes=fetch_metadata_bytes,
        upload=lambda config, key, body: None,
    )

    assert processed == []
    assert stats.wheels_uploaded == 0
    assert stats.stopped_early is True
