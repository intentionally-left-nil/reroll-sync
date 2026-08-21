"""Downloads each wheel's ``.dist-info/METADATA`` sidecar and uploads it to R2.

For every ``wheels`` row that hasn't had its metadata downloaded or been
skipped yet, this fetches the file's ``.metadata`` sidecar (PEP 658/714) from
PyPI, verifies it against the sha256 published in the file's simple-index
entry when one is available, and uploads the raw bytes to R2 under a key
equal to the wheel's sqlite ``rowid``.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from .pypi_client import metadata_hashes
from .r2_client import R2Config, R2UploadError, upload_bytes

FetchMetadata = Callable[[str, float | None], bytes]
Upload = Callable[[R2Config, str, bytes], None]


def _fetch_metadata(url: str, timeout: float | None = None) -> bytes:
    """Download and return the raw bytes at ``url``."""
    with httpx.Client(timeout=timeout) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.content


_NOT_A_REROLL_ERROR = "reroll-sync"
"""``errors.reroll_version`` for an error that didn't come from the ``reroll``
library -- the sha256 comparison here is fixed and doesn't vary with it.
"""


@dataclass(frozen=True)
class MetadataSyncStats:
    """Summary of one ``sync_metadata`` run."""

    wheels_considered: int
    wheels_uploaded: int
    wheels_skipped_no_metadata: int
    wheels_failed_hash_mismatch: int
    stopped_early: bool


@dataclass(frozen=True)
class _PendingWheel:
    rowid: int
    filename: str
    url: str
    hashes: dict[str, str] | None


def sync_metadata(
    conn: sqlite3.Connection,
    r2_config: R2Config,
    *,
    timeout: float | None = None,
    limit: int | None = None,
    fetch_metadata_bytes: FetchMetadata = _fetch_metadata,
    upload: Upload = upload_bytes,
) -> MetadataSyncStats:
    """Run one pass of the metadata download/upload algorithm against ``conn``.

    ``timeout`` bounds both the total elapsed time of the run and each
    individual HTTP request; once exceeded, processing stops early and the
    remaining pending wheels are left for the next run. ``limit`` caps how
    many pending wheels are considered in this run.
    """
    start = time.monotonic()
    pending = _pending_wheels(conn, limit)

    wheels_uploaded = 0
    wheels_skipped_no_metadata = 0
    wheels_failed_hash_mismatch = 0
    stopped_early = False

    for wheel in pending:
        if timeout is not None and time.monotonic() - start > timeout:
            stopped_early = True
            break

        if wheel.hashes is None:
            _mark_skipped(conn, wheel.filename, "no_pep658_metadata")
            wheels_skipped_no_metadata += 1
            continue

        try:
            data = fetch_metadata_bytes(f"{wheel.url}.metadata", timeout)
        except OSError:
            continue

        expected_sha256 = wheel.hashes.get("sha256")
        if expected_sha256 is not None:
            actual_sha256 = hashlib.sha256(data).hexdigest()
            if actual_sha256 != expected_sha256:
                _record_hash_mismatch(conn, wheel.filename, expected_sha256, actual_sha256)
                wheels_failed_hash_mismatch += 1
                continue

        try:
            upload(r2_config, str(wheel.rowid), data)
        except R2UploadError:
            continue

        _mark_downloaded(conn, wheel.filename)
        wheels_uploaded += 1

    return MetadataSyncStats(
        wheels_considered=len(pending),
        wheels_uploaded=wheels_uploaded,
        wheels_skipped_no_metadata=wheels_skipped_no_metadata,
        wheels_failed_hash_mismatch=wheels_failed_hash_mismatch,
        stopped_early=stopped_early,
    )


def _pending_wheels(conn: sqlite3.Connection, limit: int | None) -> list[_PendingWheel]:
    query = (
        "SELECT rowid, filename, pypi_simple FROM wheels "
        "WHERE metadata_downloaded_at IS NULL AND skip_reason IS NULL ORDER BY rowid"
    )
    params: tuple[int, ...] = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)

    pending: list[_PendingWheel] = []
    for rowid, filename, pypi_simple in conn.execute(query, params):
        raw: dict[str, Any] = json.loads(pypi_simple) if pypi_simple else {}
        pending.append(
            _PendingWheel(
                rowid=rowid,
                filename=filename,
                url=raw.get("url", ""),
                hashes=metadata_hashes(raw),
            )
        )
    return pending


def _mark_skipped(conn: sqlite3.Connection, filename: str, reason: str) -> None:
    conn.execute("UPDATE wheels SET skip_reason = ? WHERE filename = ?", (reason, filename))
    conn.commit()


def _mark_downloaded(conn: sqlite3.Connection, filename: str) -> None:
    conn.execute(
        "UPDATE wheels SET metadata_downloaded_at = ? WHERE filename = ?",
        (datetime.now(UTC).isoformat(), filename),
    )
    conn.commit()


def _record_hash_mismatch(
    conn: sqlite3.Connection, filename: str, expected_sha256: str, actual_sha256: str
) -> None:
    conn.execute(
        "INSERT INTO errors "
        "(wheel_filename, error_category, error_subcategory, details, reroll_version, "
        "created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            filename,
            "metadata_hash_mismatch",
            None,
            f"expected sha256={expected_sha256}, actual sha256={actual_sha256}",
            _NOT_A_REROLL_ERROR,
            datetime.now(UTC).isoformat(),
        ),
    )
    conn.commit()
