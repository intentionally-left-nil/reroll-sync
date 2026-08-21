"""The `unquarantine` control command's write campaign.

``dispatcher.py`` has no campaign for ``QUARANTINED`` wheels -- its own
``reprocess`` targets ``NEED_CONVERT`` and excludes ``DELETED``, neither of
which fits "clear a quarantine and requeue for re-fetch". Rather than grow
``dispatcher.Dispatcher``'s public surface for one control command, this
module implements the campaign directly against ``writer``/a reader
connection, reusing ``dispatcher``'s ``Selector`` union so the control
protocol's ``reprocess``/``unquarantine`` commands share one selector
vocabulary.

``schema.ALLOWED_TRANSITIONS`` has exactly one outbound edge from
``QUARANTINED``: back to ``NEED_METADATA``. So every unquarantine goes
through fetch again, regardless of which stage originally quarantined the
wheel -- a successful fetch naturally carries it on to convert from there.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime

from ..dispatcher import ProjectSelector, Selector, StateSelector
from ..schema import WheelState
from ..writer import WriteOp, Writer, read_txn

DEFAULT_CHUNK_SIZE = 500
DEFAULT_READ_BUDGET = 0.25


class UnsupportedSelectorError(TypeError):
    """Raised for a `Selector` variant that has no meaning for `QUARANTINED` wheels."""


def unquarantine(
    conn: sqlite3.Connection,
    writer: Writer,
    selector: Selector,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    read_budget: float = DEFAULT_READ_BUDGET,
    now: Callable[[], float] = time.time,
) -> int:
    """Reset every `QUARANTINED` wheel matching `selector` to `NEED_METADATA`.

    Supports :class:`~reroll_sync.dispatcher.StateSelector` (only meaningful
    for ``state=QUARANTINED``; any other state matches nothing, since a
    non-``QUARANTINED`` wheel is never touched) and
    :class:`~reroll_sync.dispatcher.ProjectSelector`. Raises
    :class:`UnsupportedSelectorError` for
    :class:`~reroll_sync.dispatcher.RerollVersionBelow`/
    :class:`~reroll_sync.dispatcher.SkippedOnly`, which describe
    ``wheel_repodata``/``skips`` rows a quarantined wheel doesn't have.

    Chunked into one :class:`~reroll_sync.writer.WriteOp` per up to
    ``chunk_size`` matching wheels, mirroring
    ``dispatcher.Dispatcher.reprocess``. Returns the total number of
    ``wheels`` rows updated.
    """
    project = _project_filter(selector)
    total = 0
    for chunk_ids in _quarantined_chunks(conn, project, chunk_size, budget=read_budget):
        op = _unquarantine_chunk_op(chunk_ids, writer.next_seq, lambda: _iso(now()))
        total += writer.submit_and_wait(op)
    return total


def _project_filter(selector: Selector) -> str | None:
    if isinstance(selector, StateSelector):
        if selector.state != WheelState.QUARANTINED:
            return _NO_MATCH_PROJECT
        return None
    if isinstance(selector, ProjectSelector):
        return selector.project
    raise UnsupportedSelectorError(
        f"{type(selector).__name__} has no meaning for QUARANTINED wheels"
    )


_NO_MATCH_PROJECT = "\0no-such-project\0"
"""A project name no real project can have, so a `StateSelector` for any
state other than `QUARANTINED` matches zero rows rather than needing a
separate no-op code path.
"""


def _quarantined_chunks(
    conn: sqlite3.Connection, project: str | None, chunk_size: int, *, budget: float
) -> Iterator[list[int]]:
    """Yield id chunks of currently-`QUARANTINED` wheels, optionally scoped to `project`.

    No keyset cursor: :func:`_unquarantine_chunk_op` moves every matched row
    out of `QUARANTINED` before the next page is fetched, so re-running this
    same bounded query always returns the next remaining batch.
    """
    while True:
        sql, params = _quarantined_chunk_query(project, chunk_size)
        with read_txn(conn, budget=budget, label="control.unquarantine.select"):
            rows = conn.execute(sql, params).fetchall()
        if not rows:
            return
        yield [row[0] for row in rows]
        if len(rows) < chunk_size:
            return


def _quarantined_chunk_query(project: str | None, limit: int) -> tuple[str, list[object]]:
    if project is None:
        return (
            "SELECT id FROM wheels WHERE state = ? LIMIT ?",
            [int(WheelState.QUARANTINED), limit],
        )
    return (
        "SELECT id FROM wheels WHERE state = ? AND project = ? LIMIT ?",
        [int(WheelState.QUARANTINED), project, limit],
    )


def _unquarantine_chunk_op(
    chunk_ids: list[int], next_seq: Callable[[], int], iso_now: Callable[[], str]
) -> WriteOp:
    placeholders = ", ".join("?" for _ in chunk_ids)

    def _apply(conn: sqlite3.Connection) -> int:
        conn.execute(f"DELETE FROM work WHERE wheel_id IN ({placeholders})", chunk_ids)
        now_iso = iso_now()
        affected = 0
        for wheel_id in chunk_ids:
            cursor = conn.execute(
                "UPDATE wheels SET state = ?, change_seq = ?, updated_at = ? "
                "WHERE id = ? AND state = ?",
                (
                    int(WheelState.NEED_METADATA),
                    next_seq(),
                    now_iso,
                    wheel_id,
                    int(WheelState.QUARANTINED),
                ),
            )
            affected += cursor.rowcount
        return affected

    return WriteOp(name="control.unquarantine_chunk", apply=_apply)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()
