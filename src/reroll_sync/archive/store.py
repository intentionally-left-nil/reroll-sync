"""Facade over a segment directory plus the ``segments``/``blobs`` tables.

See ``reroll_sync.archive`` for the on-disk format and ``schema.py`` for the
``segments``/``blobs`` tables this reads and writes.

Blobs are never garbage-collected and segments are never compacted: both
would mean rewriting an already-sealed (and already backed up) segment,
defeating the point of sealing. A wheel deleted from PyPI orphans its blob
permanently -- an accepted trade at the expected ~6 GB scale, not a
deferred feature.
"""

from __future__ import annotations

import re
import shutil
import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from .location import BlobLocation
from .reader import SegmentReader
from .writer import SegmentStats, SegmentWriter

_SEGMENT_FILENAME = re.compile(r"^(\d{6})\.(open|zst)$")


def _default_wall_clock() -> str:
    return datetime.now(UTC).isoformat()


class ArchiveStore:
    """Owns segment file allocation, sealing, and lookups against ``conn``.

    On construction, any ``.open`` file left behind by a crashed writer
    (one whose ``segments`` row has no ``sealed_at``, including a row
    that's missing entirely) is truncated: the footer is written last, so
    a partial segment has no usable index, and it is never salvaged.

    Pass ``recover=False`` for a second, read-only instance opened against
    a directory a live writer (e.g. the daemon) may already own -- an
    in-progress segment looks identical to a crashed writer's leftover
    from the outside (both have ``sealed_at IS NULL``), so recovery must
    never run against a directory this instance doesn't exclusively own.
    """

    def __init__(
        self,
        directory: Path,
        conn: sqlite3.Connection,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], str] = _default_wall_clock,
        recover: bool = True,
    ):
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)
        self._conn = conn
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self.reader = SegmentReader(self._directory)
        self._current_writer: SegmentWriter | None = None
        if recover:
            self._recover_stale_open_segments()

    def location_for(self, sha256: str) -> BlobLocation | None:
        """Look up where ``sha256`` lives, or ``None`` if it isn't stored."""
        row = self._conn.execute(
            "SELECT segment_id, block_no, offset, length FROM blobs WHERE sha256 = ?",
            (sha256,),
        ).fetchone()
        if row is None:
            return None
        segment_id, block_no, offset, length = row
        return BlobLocation(sha256, segment_id, block_no, offset, length)

    def get(self, sha256: str) -> bytes:
        """Return the bytes for ``sha256``. Raises :class:`KeyError` if unknown."""
        location = self.location_for(sha256)
        if location is None:
            raise KeyError(sha256)
        return self.reader.read(location)

    def add(self, data: bytes) -> BlobLocation:
        """Add ``data`` to the current segment and index it in ``blobs``.

        Sealing is the caller's responsibility: check
        ``current_writer().should_seal()`` and call :meth:`seal_writer`.
        """
        writer = self.current_writer()
        location = writer.add(data)
        self._conn.execute(
            "INSERT OR IGNORE INTO blobs (sha256, segment_id, block_no, offset, length) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                location.sha256,
                location.segment_id,
                location.block_no,
                location.offset,
                location.length,
            ),
        )
        self._conn.commit()
        return location

    def open_writer(self) -> SegmentWriter:
        """Allocate a new segment, insert its ``segments`` row, and make it current."""
        segment_id = self._next_segment_id()
        self._conn.execute("INSERT INTO segments (id) VALUES (?)", (segment_id,))
        self._conn.commit()
        writer = SegmentWriter(self._directory, segment_id, now=self._monotonic)
        self._current_writer = writer
        return writer

    def current_writer(self) -> SegmentWriter:
        """Return the open writer, allocating one via :meth:`open_writer` if none is open."""
        if self._current_writer is None:
            return self.open_writer()
        return self._current_writer

    def seal_writer(self, writer: SegmentWriter) -> SegmentStats:
        """Seal ``writer`` and record its stats in the ``segments`` table."""
        stats = writer.seal()
        self._conn.execute(
            "UPDATE segments SET sealed_at = ?, bytes = ?, records = ?, footer_sha = ? "
            "WHERE id = ?",
            (
                self._wall_clock(),
                stats.bytes,
                stats.records,
                stats.footer_sha256,
                stats.segment_id,
            ),
        )
        self._conn.commit()
        if self._current_writer is writer:
            self._current_writer = None
        return stats

    def open_writer_if_any(self) -> SegmentWriter | None:
        """Return the currently open writer, or ``None`` if none is open.

        Unlike :meth:`current_writer`, never allocates one -- safe to call
        from a read-only context (e.g. a health check) that must not
        create a new segment as a side effect of merely inspecting one.
        """
        return self._current_writer

    def disk_free_bytes(self) -> int | None:
        """Return free bytes on the filesystem backing this store's directory.

        Returns `None` if the directory is missing or unmounted (a
        `shutil.disk_usage` `OSError`), so a caller like `health.snapshot()`
        can degrade gracefully rather than crash on an adverse disk
        condition it exists to detect.
        """
        try:
            return shutil.disk_usage(self._directory).free
        except OSError:
            return None

    def sealed_segment_ids(self) -> list[int]:
        """Every segment id whose ``segments`` row has ``sealed_at`` set, in order."""
        rows = self._conn.execute(
            "SELECT id FROM segments WHERE sealed_at IS NOT NULL ORDER BY id"
        ).fetchall()
        return [row[0] for row in rows]

    def blob_rows_for_segment(self, segment_id: int) -> list[tuple[str, int, int, int]]:
        """Every ``(sha256, block_no, offset, length)`` in ``blobs`` for one segment."""
        rows = self._conn.execute(
            "SELECT sha256, block_no, offset, length FROM blobs WHERE segment_id = ? "
            "ORDER BY sha256",
            (segment_id,),
        ).fetchall()
        return [tuple(row) for row in rows]

    def _recover_stale_open_segments(self) -> None:
        for path in sorted(self._directory.glob("*.open")):
            segment_id = int(path.stem)
            if not self._is_sealed(segment_id):
                path.write_bytes(b"")

    def _is_sealed(self, segment_id: int) -> bool:
        row = self._conn.execute(
            "SELECT sealed_at FROM segments WHERE id = ?", (segment_id,)
        ).fetchone()
        return row is not None and row[0] is not None

    def _next_segment_id(self) -> int:
        (max_db,) = self._conn.execute("SELECT COALESCE(MAX(id), -1) FROM segments").fetchone()
        max_file = -1
        for path in self._directory.iterdir():
            match = _SEGMENT_FILENAME.match(path.name)
            if match is not None:
                max_file = max(max_file, int(match.group(1)))
        return max(max_db, max_file) + 1
