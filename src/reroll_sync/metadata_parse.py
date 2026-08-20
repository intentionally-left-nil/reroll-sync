"""Parses each wheel's downloaded METADATA (stored in R2) with reroll.

For every ``wheels`` row whose METADATA has been downloaded
(``metadata_downloaded_at`` is set) but not yet parsed (``wheel_metadata`` is
NULL) and not yet skipped, this downloads the raw METADATA bytes from R2
(keyed by the wheel's sqlite ``rowid``), decodes them, and parses them with
``reroll.parse_metadata``. A failure that is the wheel's fault -- undecodable
bytes, or any ``RerollError`` other than ``RerollRuntimeError`` -- is recorded
in ``errors`` and marks the wheel permanently skipped so it is never
reprocessed. A ``RerollRuntimeError`` or an R2 download failure says nothing
about the wheel itself, so the row is left untouched for a later retry.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from reroll import WheelMetadata
from reroll import parse_metadata as reroll_parse_metadata
from reroll.errors import RerollError, RerollRuntimeError

from .r2_client import R2Config, R2DownloadError, download_bytes
from .version import REROLL_VERSION

Download = Callable[[R2Config, str], bytes]
Parse = Callable[[str], WheelMetadata]

_INVALID_METADATA_CATEGORY = "invalid_metadata"
_INVALID_METADATA_ENCODING = "invalid_metadata_encoding"


@dataclass(frozen=True)
class ParseMetadataStats:
    """Summary of one ``parse_metadata`` run."""

    wheels_considered: int
    wheels_parsed: int
    wheels_failed: int
    stopped_early: bool


@dataclass(frozen=True)
class _PendingWheel:
    rowid: int
    filename: str


def parse_metadata(
    conn: sqlite3.Connection,
    r2_config: R2Config,
    *,
    timeout: float | None = None,
    limit: int | None = None,
    download: Download = download_bytes,
    parse: Parse = reroll_parse_metadata,
) -> ParseMetadataStats:
    """Run one pass of the metadata parsing algorithm against ``conn``.

    ``timeout`` bounds the total elapsed time of the run; once exceeded,
    processing stops early and the remaining pending wheels are left for the
    next run. ``limit`` caps how many pending wheels are considered in this
    run.
    """
    start = time.monotonic()
    pending = _pending_wheels(conn, limit)

    wheels_parsed = 0
    wheels_failed = 0
    stopped_early = False

    for wheel in pending:
        if timeout is not None and time.monotonic() - start > timeout:
            stopped_early = True
            break

        try:
            data = download(r2_config, str(wheel.rowid))
        except R2DownloadError:
            continue

        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            _record_error(
                conn, wheel.filename, error_subcategory=_INVALID_METADATA_ENCODING, details=str(exc)
            )
            _mark_skipped(conn, wheel.filename, _INVALID_METADATA_ENCODING)
            wheels_failed += 1
            continue

        try:
            metadata = parse(text)
        except RerollRuntimeError:
            continue
        except RerollError as exc:
            _record_error(
                conn, wheel.filename, error_subcategory=type(exc).__name__, details=str(exc)
            )
            _mark_skipped(conn, wheel.filename, _INVALID_METADATA_CATEGORY)
            wheels_failed += 1
            continue

        _mark_parsed(conn, wheel.filename, metadata)
        wheels_parsed += 1

    return ParseMetadataStats(
        wheels_considered=len(pending),
        wheels_parsed=wheels_parsed,
        wheels_failed=wheels_failed,
        stopped_early=stopped_early,
    )


def _pending_wheels(conn: sqlite3.Connection, limit: int | None) -> list[_PendingWheel]:
    query = (
        "SELECT rowid, filename FROM wheels "
        "WHERE metadata_downloaded_at IS NOT NULL "
        "AND wheel_metadata IS NULL AND skip_reason IS NULL ORDER BY rowid"
    )
    params: tuple[int, ...] = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)
    return [
        _PendingWheel(rowid=rowid, filename=filename)
        for rowid, filename in conn.execute(query, params)
    ]


def _mark_skipped(conn: sqlite3.Connection, filename: str, reason: str) -> None:
    conn.execute("UPDATE wheels SET skip_reason = ? WHERE filename = ?", (reason, filename))
    conn.commit()


def _mark_parsed(conn: sqlite3.Connection, filename: str, metadata: WheelMetadata) -> None:
    conn.execute(
        "UPDATE wheels SET wheel_metadata = ?, metadata_reroll_version = ? WHERE filename = ?",
        (metadata.model_dump_json(), REROLL_VERSION, filename),
    )
    conn.commit()


def _record_error(
    conn: sqlite3.Connection, filename: str, *, error_subcategory: str, details: str
) -> None:
    conn.execute(
        "INSERT INTO errors "
        "(wheel_filename, error_category, error_subcategory, details, reroll_version, "
        "created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            filename,
            _INVALID_METADATA_CATEGORY,
            error_subcategory,
            details,
            REROLL_VERSION,
            datetime.now(UTC).isoformat(),
        ),
    )
    conn.commit()
