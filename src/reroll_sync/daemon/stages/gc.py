"""The `gc` stage: Phase 1's only garbage collection is bounded `errors` retention.

Blobs/segments are never garbage-collected (see `archive/store.py`); the
`errors` table is the one place unbounded growth is worth trimming.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime

from ...writer import WriteOp, Writer

DEFAULT_RETENTION_DAYS = 30
DEFAULT_CHUNK_SIZE = 5000


class GcStage:
    """Deletes `errors` rows older than `retention_days`, one bounded chunk at a time."""

    def __init__(
        self,
        writer: Writer,
        *,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._writer = writer
        self._retention_days = retention_days
        self._chunk_size = chunk_size
        self._now = now

    def iterate(self) -> bool:
        """Delete up to `chunk_size` `errors` rows older than the retention window.

        Returns whether any row was deleted, so a `PollTrigger` keeps
        chunking immediately while there's a backlog and idles once caught up.
        """
        cutoff_iso = _iso(self._now() - self._retention_days * 86400)
        op = _gc_errors_op(cutoff_iso, self._chunk_size)
        affected = self._writer.submit_and_wait(op)
        return affected > 0


def _gc_errors_op(cutoff_iso: str, chunk_size: int) -> WriteOp:
    def _apply(conn: sqlite3.Connection) -> int:
        cursor = conn.execute(
            "DELETE FROM errors WHERE id IN (SELECT id FROM errors WHERE created_at < ? LIMIT ?)",
            (cutoff_iso, chunk_size),
        )
        return cursor.rowcount

    return WriteOp(name="gc.errors", apply=_apply)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()
