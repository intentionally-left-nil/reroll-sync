"""Converts each wheel's parsed METADATA into repodata record(s) with reroll.

For every ``wheels`` row whose METADATA has been parsed (``wheel_metadata``
is set) but not yet converted (``repodata`` is NULL) and not yet skipped,
this reconstructs the wheel's ``WheelMetadata`` from the stored JSON and
runs it (plus the wheel's own filename) through
``reroll.stages.get_wheel_records`` to produce its repodata record(s) and
name conversions -- no download is needed, since the parsed METADATA
already lives in ``wheel_metadata``. A failure that is the wheel's fault --
any ``RerollError`` other than ``RerollRuntimeError`` -- is recorded in
``errors`` and marks the wheel permanently skipped so it is never
reprocessed. A ``RerollRuntimeError`` says nothing about the wheel itself,
so the row is left untouched for a later retry.

Building the default name-mapper chain (``reroll.default_mappers``) reloads
its config and network-backed lookup tables from scratch on every call, so
it is built at most once per run -- lazily, the first time a pending wheel
is actually converted -- and reused for every wheel after that, rather than
rebuilt per wheel.

Every wheel is converted with ``allow_pre=False``: reroll rejects a
pre-release wheel version outright rather than guessing whether the caller
wants it.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from reroll import NameMappers, NameResolution, WheelMetadata, WheelRecord, default_mappers
from reroll.errors import RerollError, RerollRuntimeError
from reroll.stages import get_wheel_records as reroll_get_wheel_records

from .version import REROLL_VERSION

GetWheelRecords = Callable[..., tuple[WheelRecord, ...]]

_CONVERSION_ERROR_CATEGORY = "reroll_conversion_failed"


@dataclass(frozen=True)
class SyncRerollStats:
    """Summary of one ``sync_reroll`` run."""

    wheels_considered: int
    wheels_converted: int
    wheels_failed: int
    stopped_early: bool


@dataclass(frozen=True)
class _PendingWheel:
    filename: str
    wheel_metadata: str


def sync_reroll(
    conn: sqlite3.Connection,
    *,
    timeout: float | None = None,
    limit: int | None = None,
    allow_pre: bool = False,
    mappers: NameMappers | None = None,
    get_wheel_records: GetWheelRecords = reroll_get_wheel_records,
) -> SyncRerollStats:
    """Run one pass of the reroll conversion algorithm against ``conn``.

    ``mappers`` defaults to a freshly built ``default_mappers()`` chain,
    built once here -- lazily, only once a pending wheel is actually being
    converted, and never at all if there is none -- and reused for every
    remaining wheel considered in this run. Pass it explicitly to reuse a
    chain built by an earlier run, or to swap in a cheaper chain for tests.

    ``timeout`` bounds the total elapsed time of the run; once exceeded,
    processing stops early and the remaining pending wheels are left for the
    next run. ``limit`` caps how many pending wheels are considered in this
    run.
    """
    start = time.monotonic()
    pending = _pending_wheels(conn, limit)
    resolved_mappers = mappers

    wheels_converted = 0
    wheels_failed = 0
    stopped_early = False

    for wheel in pending:
        if timeout is not None and time.monotonic() - start > timeout:
            stopped_early = True
            break

        if resolved_mappers is None:
            resolved_mappers = default_mappers()

        metadata = WheelMetadata.model_validate_json(wheel.wheel_metadata)

        try:
            records = get_wheel_records(
                metadata, wheel.filename, mappers=resolved_mappers, allow_pre=allow_pre
            )
        except RerollRuntimeError:
            continue
        except RerollError as exc:
            _record_error(
                conn, wheel.filename, error_subcategory=type(exc).__name__, details=str(exc)
            )
            _mark_skipped(conn, wheel.filename, _CONVERSION_ERROR_CATEGORY)
            wheels_failed += 1
            continue

        _mark_converted(conn, wheel.filename, records)
        wheels_converted += 1

    return SyncRerollStats(
        wheels_considered=len(pending),
        wheels_converted=wheels_converted,
        wheels_failed=wheels_failed,
        stopped_early=stopped_early,
    )


def _pending_wheels(conn: sqlite3.Connection, limit: int | None) -> list[_PendingWheel]:
    query = (
        "SELECT filename, wheel_metadata FROM wheels "
        "WHERE wheel_metadata IS NOT NULL "
        "AND repodata IS NULL AND skip_reason IS NULL ORDER BY rowid"
    )
    params: tuple[int, ...] = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)
    return [
        _PendingWheel(filename=filename, wheel_metadata=wheel_metadata)
        for filename, wheel_metadata in conn.execute(query, params)
    ]


def _mark_skipped(conn: sqlite3.Connection, filename: str, reason: str) -> None:
    conn.execute("UPDATE wheels SET skip_reason = ? WHERE filename = ?", (reason, filename))
    conn.commit()


def _mark_converted(
    conn: sqlite3.Connection, filename: str, records: tuple[WheelRecord, ...]
) -> None:
    repodata_json = json.dumps([record.model_dump(mode="json") for record in records])
    name_conversions_json = json.dumps(
        [resolution.model_dump(mode="json") for resolution in _deduped_resolutions(records)]
    )
    conn.execute(
        "UPDATE wheels SET repodata = ?, name_conversions = ?, repodata_reroll_version = ? "
        "WHERE filename = ?",
        (repodata_json, name_conversions_json, REROLL_VERSION, filename),
    )
    conn.commit()


def _deduped_resolutions(records: tuple[WheelRecord, ...]) -> tuple[NameResolution, ...]:
    """One ``NameResolution`` per unique PyPI name resolved across all of
    ``records`` -- deduped, since the same name can be resolved by more
    than one of a wheel's records (e.g. one per supported platform).
    """
    seen: dict[str, NameResolution] = {}
    for record in records:
        for resolution in record.resolutions:
            seen.setdefault(resolution.pypi_name, resolution)
    return tuple(seen[name] for name in sorted(seen))


def _record_error(
    conn: sqlite3.Connection, filename: str, *, error_subcategory: str, details: str
) -> None:
    conn.execute(
        "INSERT INTO errors "
        "(wheel_filename, error_category, error_subcategory, details, reroll_version, "
        "created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            filename,
            _CONVERSION_ERROR_CATEGORY,
            error_subcategory,
            details,
            REROLL_VERSION,
            datetime.now(UTC).isoformat(),
        ),
    )
    conn.commit()
