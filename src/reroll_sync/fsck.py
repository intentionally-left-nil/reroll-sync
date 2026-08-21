"""Read-only invariant checker for the reroll-sync database.

Reports; never repairs -- see ``specs/11-health-and-fsck.md``'s "Deferred"
section for why a ``--repair`` mode is out of scope. Every scan is
chunked and keyset-paginated (never one unbounded query, never a long
read transaction): each chunk is its own bounded
:func:`~reroll_sync.writer.read_txn`, so ``run`` is safe to call against a
live daemon's database.

Twenty invariants, grouped as in the spec: state consistency, skip
attribution, the work table, the archive, sequence numbers, and two
cross-cutting checks. Each produces at most one :class:`Violation` (only
when its count is nonzero); invariant 16 (orphaned blobs) and invariant 20
(tombstoned wheels with a lingering ``wheel_repodata`` row) are marked
``informational`` -- expected, harmless, and never enough on their own to
make :attr:`FsckReport.ok` false.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field

from .archive.errors import CorruptSegmentError
from .archive.store import ArchiveStore
from .schema import WheelState
from .writer import ReadTxnWatchdog, read_txn

DEFAULT_CHUNK_SIZE = 1000
DEFAULT_READ_BUDGET = 0.25
DEFAULT_EXAMPLE_LIMIT = 20

_TERMINAL_FOR_WORK: tuple[WheelState, ...] = (
    WheelState.READY,
    WheelState.SKIPPED,
    WheelState.NO_METADATA,
    WheelState.DELETED,
)
"""States the dispatcher deletes a wheel's `work` row upon reaching.

Distinct from `QUARANTINED`: that state deliberately keeps a `work` row
(invariant 6), so it is not "terminal" in this sense even though nothing
further happens to the wheel without an operator's `unquarantine`.
"""


@dataclass(frozen=True)
class Violation:
    """One invariant's finding: nonzero only, so a clean check contributes nothing."""

    invariant: str
    description: str
    count: int
    example_ids: tuple[object, ...]
    informational: bool = False


@dataclass(frozen=True)
class FsckReport:
    """Every :class:`Violation` found by :func:`run`, empty means clean."""

    violations: tuple[Violation, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        """Whether every violation found (if any) is merely informational."""
        return not any(not v.informational for v in self.violations)


def run(
    reader: sqlite3.Connection,
    *,
    max_attempts: int = 8,
    writer_change_seq: int | None = None,
    archive_store: ArchiveStore | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    example_limit: int = DEFAULT_EXAMPLE_LIMIT,
    read_budget: float = DEFAULT_READ_BUDGET,
    watchdog: ReadTxnWatchdog | None = None,
) -> FsckReport:
    """Run every invariant check against ``reader`` and return a combined report.

    ``reader`` must be a read-only connection. ``writer_change_seq``
    (invariant 18) and ``archive_store`` (invariant 15) are optional: each
    check they gate is skipped -- not reported as a violation -- when its
    input is not supplied, since there is nothing to compare against.
    """
    violations: list[Violation] = []

    for check in _ROW_CHECKS:
        violation = _run_row_check(
            reader,
            check,
            chunk_size=chunk_size,
            example_limit=example_limit,
            budget=read_budget,
            watchdog=watchdog,
        )
        if violation is not None:
            violations.append(violation)

    state_violation = _check_invalid_state(
        reader,
        chunk_size=chunk_size,
        example_limit=example_limit,
        budget=read_budget,
        watchdog=watchdog,
    )
    if state_violation is not None:
        violations.append(state_violation)

    max_attempts_violation = _check_attempts_exceeds_max(
        reader,
        max_attempts=max_attempts,
        chunk_size=chunk_size,
        example_limit=example_limit,
        budget=read_budget,
        watchdog=watchdog,
    )
    if max_attempts_violation is not None:
        violations.append(max_attempts_violation)

    quarantine_violation = _check_max_attempts_without_quarantine(
        reader,
        max_attempts=max_attempts,
        chunk_size=chunk_size,
        example_limit=example_limit,
        budget=read_budget,
        watchdog=watchdog,
    )
    if quarantine_violation is not None:
        violations.append(quarantine_violation)

    change_seq_violation = _check_duplicate_change_seq(
        reader,
        chunk_size=chunk_size,
        example_limit=example_limit,
        budget=read_budget,
        watchdog=watchdog,
    )
    if change_seq_violation is not None:
        violations.append(change_seq_violation)

    if writer_change_seq is not None:
        seq_mismatch = _check_writer_change_seq(
            reader, writer_change_seq, budget=read_budget, watchdog=watchdog
        )
        if seq_mismatch is not None:
            violations.append(seq_mismatch)

    if archive_store is not None:
        archive_violation = _check_sealed_segments(archive_store, example_limit=example_limit)
        if archive_violation is not None:
            violations.append(archive_violation)

    return FsckReport(tuple(violations))


# ---------------------------------------------------------------------------
# The generic row-scan driver
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _RowCheck:
    """One invariant expressible as "rows matching this query are violations"."""

    invariant: str
    description: str
    base_query: Callable[[], tuple[str, Sequence[object]]]
    informational: bool = False


def _chunked_scan(
    reader: sqlite3.Connection,
    base_query: str,
    base_params: Sequence[object],
    *,
    chunk_size: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
    label: str,
) -> Iterator[object]:
    """Yield every `value` matched by `base_query`, one keyset-paginated chunk at a time.

    `base_query` must select exactly two columns aliased `value` and
    `cursor_rowid`, and already carry its own `WHERE` clause (no
    `ORDER BY`/`LIMIT`). Wrapping it as a subquery lets the outer keyset
    predicate be added without disturbing the inner query's own index
    usage -- confirmed by this module's `EXPLAIN QUERY PLAN` tests.

    Each page over-fetches by one row so a page with more than
    ``chunk_size`` rows can tell there is a next page without a trailing
    empty confirmation call: a 1000-row match with ``chunk_size=1000``
    performs exactly 10 read transactions, not 11.
    """
    cursor: object | None = None
    while True:
        params = list(base_params)
        if cursor is None:
            sql = f"SELECT value, cursor_rowid FROM ({base_query}) ORDER BY cursor_rowid LIMIT ?"
        else:
            sql = (
                f"SELECT value, cursor_rowid FROM ({base_query}) "
                "WHERE cursor_rowid > ? ORDER BY cursor_rowid LIMIT ?"
            )
            params.append(cursor)
        params.append(chunk_size + 1)
        with read_txn(reader, budget=budget, label=label, strict=True, watchdog=watchdog):
            rows = reader.execute(sql, params).fetchall()
        if not rows:
            return
        has_more = len(rows) > chunk_size
        page = rows[:chunk_size]
        for value, _rowid in page:
            yield value
        if not has_more:
            return
        cursor = page[-1][1]


def _run_row_check(
    reader: sqlite3.Connection,
    check: _RowCheck,
    *,
    chunk_size: int,
    example_limit: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> Violation | None:
    base_query, base_params = check.base_query()
    count = 0
    examples: list[object] = []
    for value in _chunked_scan(
        reader,
        base_query,
        base_params,
        chunk_size=chunk_size,
        budget=budget,
        watchdog=watchdog,
        label=f"fsck.{check.invariant}",
    ):
        count += 1
        if len(examples) < example_limit:
            examples.append(value)
    if count == 0:
        return None
    return Violation(
        invariant=check.invariant,
        description=check.description,
        count=count,
        example_ids=tuple(examples),
        informational=check.informational,
    )


# ---------------------------------------------------------------------------
# State consistency (invariants 1-8)
# ---------------------------------------------------------------------------


def _ready_without_repodata_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT w.id AS value, w.rowid AS cursor_rowid FROM wheels w "
        "WHERE w.state = ? AND NOT EXISTS "
        "(SELECT 1 FROM wheel_repodata wr WHERE wr.wheel_id = w.id)",
        [int(WheelState.READY)],
    )


def _repodata_without_ready_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT wr.wheel_id AS value, wr.rowid AS cursor_rowid FROM wheel_repodata wr "
        "JOIN wheels w ON w.id = wr.wheel_id WHERE w.state != ?",
        [int(WheelState.READY)],
    )


def _need_convert_without_blob_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT w.id AS value, w.rowid AS cursor_rowid FROM wheels w "
        "WHERE w.state = ? AND (w.blob_sha256 IS NULL OR NOT EXISTS "
        "(SELECT 1 FROM blobs b WHERE b.sha256 = w.blob_sha256))",
        [int(WheelState.NEED_CONVERT)],
    )


def _need_metadata_with_blob_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT id AS value, rowid AS cursor_rowid FROM wheels WHERE state = ? "
        "AND blob_sha256 IS NOT NULL",
        [int(WheelState.NEED_METADATA)],
    )


def _no_metadata_with_metadata_sha256_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT id AS value, rowid AS cursor_rowid FROM wheels WHERE state = ? "
        "AND metadata_sha256 IS NOT NULL",
        [int(WheelState.NO_METADATA)],
    )


def _skipped_without_skips_row_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT w.id AS value, w.rowid AS cursor_rowid FROM wheels w WHERE w.state = ? "
        "AND NOT EXISTS (SELECT 1 FROM skips sk WHERE sk.wheel_id = w.id)",
        [int(WheelState.SKIPPED)],
    )


def _quarantined_without_work_row_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT w.id AS value, w.rowid AS cursor_rowid FROM wheels w WHERE w.state = ? "
        "AND NOT EXISTS "
        "(SELECT 1 FROM work wk WHERE wk.wheel_id = w.id AND wk.quarantined_at IS NOT NULL)",
        [int(WheelState.QUARANTINED)],
    )


def _deleted_without_deleted_at_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT id AS value, rowid AS cursor_rowid FROM wheels WHERE state = ? "
        "AND deleted_at IS NULL",
        [int(WheelState.DELETED)],
    )


def _deleted_at_without_deleted_state_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT id AS value, rowid AS cursor_rowid FROM wheels WHERE deleted_at IS NOT NULL "
        "AND state != ?",
        [int(WheelState.DELETED)],
    )


def _invalid_state_query() -> tuple[str, Sequence[object]]:
    placeholders = ", ".join("?" for _ in WheelState)
    sql = (
        f"SELECT id AS value, rowid AS cursor_rowid FROM wheels WHERE state NOT IN ({placeholders})"
    )
    return sql, [int(state) for state in WheelState]


def _check_invalid_state(
    reader: sqlite3.Connection,
    *,
    chunk_size: int,
    example_limit: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> Violation | None:
    check = _RowCheck(
        invariant="8_state_outside_wheelstate",
        description="wheels.state has a value outside WheelState",
        base_query=_invalid_state_query,
    )
    return _run_row_check(
        reader,
        check,
        chunk_size=chunk_size,
        example_limit=example_limit,
        budget=budget,
        watchdog=watchdog,
    )


# ---------------------------------------------------------------------------
# Skip attribution (invariants 9-10)
# ---------------------------------------------------------------------------


def _permanent_skip_with_reroll_version_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT wheel_id AS value, rowid AS cursor_rowid FROM skips "
        "WHERE permanent = 1 AND reroll_version IS NOT NULL",
        [],
    )


def _non_permanent_skip_without_reroll_version_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT wheel_id AS value, rowid AS cursor_rowid FROM skips "
        "WHERE permanent = 0 AND reroll_version IS NULL",
        [],
    )


def _stale_skip_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT sk.wheel_id AS value, sk.rowid AS cursor_rowid FROM skips sk "
        "JOIN wheels w ON w.id = sk.wheel_id WHERE w.state != ?",
        [int(WheelState.SKIPPED)],
    )


# ---------------------------------------------------------------------------
# Work table (invariants 11-12)
# ---------------------------------------------------------------------------


def _work_row_for_terminal_wheel_query() -> tuple[str, Sequence[object]]:
    placeholders = ", ".join("?" for _ in _TERMINAL_FOR_WORK)
    return (
        "SELECT wk.wheel_id AS value, wk.rowid AS cursor_rowid FROM work wk "
        f"JOIN wheels w ON w.id = wk.wheel_id WHERE w.state IN ({placeholders})",
        [int(state) for state in _TERMINAL_FOR_WORK],
    )


def _check_attempts_exceeds_max(
    reader: sqlite3.Connection,
    *,
    max_attempts: int,
    chunk_size: int,
    example_limit: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> Violation | None:
    def _query() -> tuple[str, Sequence[object]]:
        return (
            "SELECT wheel_id AS value, rowid AS cursor_rowid FROM work WHERE attempts > ?",
            [max_attempts],
        )

    check = _RowCheck(
        invariant="12a_attempts_exceeds_max",
        description=f"work.attempts exceeds max_attempts ({max_attempts})",
        base_query=_query,
    )
    return _run_row_check(
        reader,
        check,
        chunk_size=chunk_size,
        example_limit=example_limit,
        budget=budget,
        watchdog=watchdog,
    )


def _check_max_attempts_without_quarantine(
    reader: sqlite3.Connection,
    *,
    max_attempts: int,
    chunk_size: int,
    example_limit: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> Violation | None:
    def _query() -> tuple[str, Sequence[object]]:
        return (
            "SELECT wheel_id AS value, rowid AS cursor_rowid FROM work "
            "WHERE attempts = ? AND quarantined_at IS NULL",
            [max_attempts],
        )

    check = _RowCheck(
        invariant="12b_max_attempts_without_quarantine",
        description=f"work.attempts = max_attempts ({max_attempts}) without quarantined_at set",
        base_query=_query,
    )
    return _run_row_check(
        reader,
        check,
        chunk_size=chunk_size,
        example_limit=example_limit,
        budget=budget,
        watchdog=watchdog,
    )


# ---------------------------------------------------------------------------
# Archive (invariants 13-16)
# ---------------------------------------------------------------------------


def _blob_sha256_unresolved_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT id AS value, rowid AS cursor_rowid FROM wheels WHERE blob_sha256 IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM blobs b WHERE b.sha256 = wheels.blob_sha256)",
        [],
    )


def _blob_segment_unresolved_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT sha256 AS value, rowid AS cursor_rowid FROM blobs WHERE NOT EXISTS "
        "(SELECT 1 FROM segments s WHERE s.id = blobs.segment_id)",
        [],
    )


def _orphaned_blob_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT sha256 AS value, rowid AS cursor_rowid FROM blobs WHERE NOT EXISTS "
        "(SELECT 1 FROM wheels w WHERE w.blob_sha256 = blobs.sha256)",
        [],
    )


def _check_sealed_segments(archive_store: ArchiveStore, *, example_limit: int) -> Violation | None:
    """Invariant 15: existence and row/file correspondence, not a byte-level pass.

    Reads each sealed segment's footer via `SegmentReader.footer_records` --
    the same lightweight, decompress-nothing-but-the-footer primitive
    `archive.verify.verify_archive` also uses before its own (separate)
    full byte-level hash pass -- and compares its records against the
    `blobs` table. Matching each record's actual bytes to its claimed
    sha256 is `verify_archive`'s job (delegated to spec 02), not this
    one's.
    """
    count = 0
    examples: list[str] = []
    for segment_id in archive_store.sealed_segment_ids():
        for problem in _check_one_sealed_segment(archive_store, segment_id):
            count += 1
            if len(examples) < example_limit:
                examples.append(problem)
    if count == 0:
        return None
    return Violation(
        invariant="15_sealed_segment_integrity",
        description="a sealed segment is missing on disk or its footer/blobs disagree",
        count=count,
        example_ids=tuple(examples),
    )


def _check_one_sealed_segment(archive_store: ArchiveStore, segment_id: int) -> list[str]:
    prefix = f"segment {segment_id:06d}"
    try:
        footer_entries = archive_store.reader.footer_records(segment_id)
    except (CorruptSegmentError, OSError) as exc:
        return [f"{prefix}: {exc}"]

    footer_positions = {
        sha: (block_no, offset, length) for sha, block_no, offset, length in footer_entries
    }
    blob_positions = {
        sha: (block_no, offset, length)
        for sha, block_no, offset, length in archive_store.blob_rows_for_segment(segment_id)
    }

    problems: list[str] = []
    for sha256, position in footer_positions.items():
        if sha256 not in blob_positions:
            problems.append(f"{prefix}: footer record {sha256} has no blobs row")
        elif blob_positions[sha256] != position:
            problems.append(
                f"{prefix}: blobs row for {sha256} is at {blob_positions[sha256]}, "
                f"footer says {position}"
            )
    for sha256 in blob_positions:
        if sha256 not in footer_positions:
            problems.append(f"{prefix}: blobs row {sha256} has no matching footer record")
    return problems


# ---------------------------------------------------------------------------
# Sequences (invariants 17-18)
# ---------------------------------------------------------------------------


def _duplicate_change_seq_window_first_query() -> str:
    """The first page of distinct `change_seq` values, in index order."""
    return "SELECT DISTINCT change_seq FROM wheels ORDER BY change_seq LIMIT ?"


def _duplicate_change_seq_window_next_query() -> str:
    """Every subsequent page: distinct `change_seq` values past the last one seen."""
    return "SELECT DISTINCT change_seq FROM wheels WHERE change_seq > ? ORDER BY change_seq LIMIT ?"


def _duplicate_change_seq_aggregate_query() -> str:
    """Duplicate groups within one bounded `change_seq` window (inclusive)."""
    return (
        "SELECT change_seq, COUNT(*) FROM wheels WHERE change_seq BETWEEN ? AND ? "
        "GROUP BY change_seq HAVING COUNT(*) > 1 AND COUNT(DISTINCT updated_at) > 1"
    )


def _check_duplicate_change_seq(
    reader: sqlite3.Connection,
    *,
    chunk_size: int,
    example_limit: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> Violation | None:
    """Invariant 17: paginated by `change_seq` windows, not by the aggregate's output.

    A plain `GROUP BY change_seq HAVING ...` must finish aggregating every
    row before any output row exists, so pagination applied to that
    query's *output* still scans the whole table on every page. Instead,
    each page first fetches up to `chunk_size` distinct `change_seq`
    values past the last one seen (using `ix_wheels_change_seq`'s order),
    then aggregates only the bounded `change_seq` range those values
    span -- so each `read_txn` touches a window of the table, not all of
    it, and a duplicate-free table still advances page by page rather
    than resolving in one unbounded scan.

    Each page over-fetches its window by one value, the same trick
    `_chunked_scan` uses, so a page with more than `chunk_size` distinct
    values can tell there is a next page without a trailing empty
    confirmation call.
    """
    count = 0
    examples: list[object] = []
    cursor: int | None = None
    while True:
        with read_txn(
            reader,
            budget=budget,
            label="fsck.17_duplicate_change_seq",
            strict=True,
            watchdog=watchdog,
        ):
            if cursor is None:
                fetched = reader.execute(
                    _duplicate_change_seq_window_first_query(), (chunk_size + 1,)
                ).fetchall()
            else:
                fetched = reader.execute(
                    _duplicate_change_seq_window_next_query(), (cursor, chunk_size + 1)
                ).fetchall()
            has_more = len(fetched) > chunk_size
            window = fetched[:chunk_size]
            groups = []
            if window:
                window_min, window_max = window[0][0], window[-1][0]
                groups = reader.execute(
                    _duplicate_change_seq_aggregate_query(), (window_min, window_max)
                ).fetchall()

        if not window:
            break

        for change_seq, group_size in groups:
            with read_txn(
                reader,
                budget=budget,
                label="fsck.17_duplicate_change_seq.members",
                strict=True,
                watchdog=watchdog,
            ):
                member_ids = [
                    row[0]
                    for row in reader.execute(
                        "SELECT id FROM wheels WHERE change_seq = ? LIMIT ?",
                        (change_seq, max(0, example_limit - len(examples))),
                    ).fetchall()
                ]
            count += group_size
            examples.extend(member_ids)

        if not has_more:
            break
        cursor = window[-1][0]

    if count == 0:
        return None
    return Violation(
        invariant="17_duplicate_change_seq",
        description="change_seq is duplicated among wheels rows with differing updated_at",
        count=count,
        example_ids=tuple(examples),
    )


def _check_writer_change_seq(
    reader: sqlite3.Connection,
    writer_change_seq: int,
    *,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> Violation | None:
    with read_txn(
        reader, budget=budget, label="fsck.18_writer_change_seq", strict=True, watchdog=watchdog
    ):
        (db_max,) = reader.execute("SELECT COALESCE(MAX(change_seq), 0) FROM wheels").fetchone()
    if db_max == writer_change_seq:
        return None
    return Violation(
        invariant="18_writer_change_seq_mismatch",
        description="MAX(wheels.change_seq) does not match the writer's change_seq counter",
        count=1,
        example_ids=(f"db_max={db_max} writer={writer_change_seq}",),
    )


# ---------------------------------------------------------------------------
# Cross-cutting (invariants 19-20)
# ---------------------------------------------------------------------------


def _conda_name_outside_ready_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT id AS value, rowid AS cursor_rowid FROM wheels WHERE conda_name IS NOT NULL "
        "AND state != ?",
        [int(WheelState.READY)],
    )


def _tombstoned_with_repodata_query() -> tuple[str, Sequence[object]]:
    return (
        "SELECT w.id AS value, w.rowid AS cursor_rowid FROM wheels w "
        "JOIN wheel_repodata wr ON wr.wheel_id = w.id WHERE w.state = ?",
        [int(WheelState.DELETED)],
    )


_ROW_CHECKS: tuple[_RowCheck, ...] = (
    _RowCheck(
        "1a_ready_without_repodata",
        "state = READY but no wheel_repodata row",
        _ready_without_repodata_query,
    ),
    _RowCheck(
        "1b_repodata_without_ready",
        "wheel_repodata row exists but state != READY",
        _repodata_without_ready_query,
    ),
    _RowCheck(
        "2_need_convert_without_blob",
        "state = NEED_CONVERT but blob_sha256 is unset or unresolved",
        _need_convert_without_blob_query,
    ),
    _RowCheck(
        "3_need_metadata_with_blob",
        "state = NEED_METADATA but blob_sha256 is set",
        _need_metadata_with_blob_query,
    ),
    _RowCheck(
        "4_no_metadata_with_metadata_sha256",
        "state = NO_METADATA but metadata_sha256 is set",
        _no_metadata_with_metadata_sha256_query,
    ),
    _RowCheck(
        "5_skipped_without_skips_row",
        "state = SKIPPED but no skips row exists",
        _skipped_without_skips_row_query,
    ),
    _RowCheck(
        "6_quarantined_without_work_row",
        "state = QUARANTINED but no quarantined work row exists",
        _quarantined_without_work_row_query,
    ),
    _RowCheck(
        "7a_deleted_without_deleted_at",
        "state = DELETED but deleted_at is unset",
        _deleted_without_deleted_at_query,
    ),
    _RowCheck(
        "7b_deleted_at_without_deleted_state",
        "deleted_at is set but state != DELETED",
        _deleted_at_without_deleted_state_query,
    ),
    _RowCheck(
        "9a_permanent_skip_with_reroll_version",
        "skips.permanent = 1 but reroll_version is set",
        _permanent_skip_with_reroll_version_query,
    ),
    _RowCheck(
        "9b_non_permanent_skip_without_reroll_version",
        "skips.permanent = 0 but reroll_version is unset",
        _non_permanent_skip_without_reroll_version_query,
    ),
    _RowCheck("10_stale_skip", "skips row exists for a wheel not in SKIPPED", _stale_skip_query),
    _RowCheck(
        "11_work_row_for_terminal_wheel",
        "work row exists for a wheel in a terminal state",
        _work_row_for_terminal_wheel_query,
    ),
    _RowCheck(
        "13_blob_sha256_unresolved",
        "wheels.blob_sha256 does not resolve to a blobs row",
        _blob_sha256_unresolved_query,
    ),
    _RowCheck(
        "14_blob_segment_unresolved",
        "blobs.segment_id does not resolve to a segments row",
        _blob_segment_unresolved_query,
    ),
    _RowCheck(
        "16_orphaned_blob",
        "blob is referenced by no wheel (expected; blobs are never GC'd)",
        _orphaned_blob_query,
        informational=True,
    ),
    _RowCheck(
        "19_conda_name_outside_ready",
        "conda_name is set for a wheel not in READY",
        _conda_name_outside_ready_query,
    ),
    _RowCheck(
        "20_tombstoned_with_repodata",
        "tombstoned wheel still has a wheel_repodata row",
        _tombstoned_with_repodata_query,
        informational=True,
    ),
)
