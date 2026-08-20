"""Appends content-addressed records to an in-progress archive segment.

See ``reroll_sync.archive`` for the on-disk segment format this writes.
"""

from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from .format import (
    BlockEntry,
    RecordEntry,
    compress_block,
    encode_footer,
    encode_trailer,
    footer_crc32,
)
from .location import BlobLocation

DEFAULT_BLOCK_TARGET_BYTES = 4 * 1024 * 1024
DEFAULT_SEAL_BYTES = 64 * 1024 * 1024
DEFAULT_SEAL_AFTER_SECONDS = 6 * 60 * 60
DEFAULT_LEVEL = 10


@dataclass(frozen=True)
class SegmentStats:
    """Summary of a newly-sealed segment, for updating the ``segments`` table."""

    segment_id: int
    bytes: int
    records: int
    footer_sha256: str


class SegmentWriter:
    """Appends records to ``directory/{segment_id:06d}.open``, sealing it to ``.zst``.

    Records are deduplicated by sha256 within this segment only, for
    idempotency (a wheel re-fetched after a crash stores its bytes once),
    not to save space. The caller is responsible for ordering records
    (e.g. by project) before calling :meth:`add` for the sake of
    compression; the writer streams records into blocks in call order and
    cannot reorder them. Ordering affects the compression ratio, never
    correctness.

    Usable as a context manager purely for handle cleanup: exiting the
    ``with`` block, normally or via an exception, closes the file handle
    but never seals it. Only an explicit :meth:`seal` call renames the
    ``.open`` file to ``.zst``; abandoning a writer without calling it
    leaves the ``.open`` file in place.
    """

    def __init__(
        self,
        directory: Path,
        segment_id: int,
        *,
        block_target_bytes: int = DEFAULT_BLOCK_TARGET_BYTES,
        level: int = DEFAULT_LEVEL,
        seal_bytes: int = DEFAULT_SEAL_BYTES,
        seal_after_seconds: float = DEFAULT_SEAL_AFTER_SECONDS,
        now: Callable[[], float] = time.monotonic,
    ):
        self.segment_id = segment_id
        self.path = Path(directory) / f"{segment_id:06d}.open"
        self._block_target_bytes = block_target_bytes
        self._level = level
        self._seal_bytes = seal_bytes
        self._seal_after_seconds = seal_after_seconds
        self._now = now
        self._start = now()
        self._file = _open_for_exclusive_write(self.path)
        self._position = 0
        self._compressed_bytes = 0
        self._block_no = 0
        self._block_buffer = bytearray()
        self._block_raw_size = 0
        self._pending_records = 0
        self._blocks: list[BlockEntry] = []
        self._records: list[RecordEntry] = []
        self._locations: dict[str, BlobLocation] = {}
        self._sealed = False

    def add(self, data: bytes) -> BlobLocation:
        """Append ``data`` as one record, returning where it now lives.

        Returns the existing location without writing anything if ``data``
        was already added to this segment.
        """
        if self._sealed:
            raise RuntimeError(f"segment {self.segment_id:06d} is already sealed")
        sha256 = hashlib.sha256(data).hexdigest()
        existing = self._locations.get(sha256)
        if existing is not None:
            return existing
        if self._pending_records and self._block_raw_size + len(data) > self._block_target_bytes:
            self._flush_block()
        offset = self._block_raw_size
        self._block_buffer += data
        self._block_raw_size += len(data)
        self._pending_records += 1
        location = BlobLocation(sha256, self.segment_id, self._block_no, offset, len(data))
        self._locations[sha256] = location
        self._records.append(RecordEntry(bytes.fromhex(sha256), self._block_no, offset, len(data)))
        return location

    def should_seal(self) -> bool:
        """True once this segment has crossed its size or age threshold."""
        elapsed = self._now() - self._start
        return self._compressed_bytes >= self._seal_bytes or elapsed >= self._seal_after_seconds

    def seal(self) -> SegmentStats:
        """Flush the open block, write the footer and trailer, and seal the file.

        Renames ``{segment_id:06d}.open`` to ``{segment_id:06d}.zst``
        atomically after an ``fsync``. Raises :class:`RuntimeError` if this
        writer has already been sealed.
        """
        if self._sealed:
            raise RuntimeError(f"segment {self.segment_id:06d} is already sealed")
        if self._pending_records:
            self._flush_block()

        footer_offset = self._position
        footer = encode_footer(self._blocks, self._records, level=self._level)
        self._file.write(footer)
        crc = footer_crc32(footer)
        trailer = encode_trailer(
            footer_offset=footer_offset, footer_length=len(footer), footer_crc32=crc
        )
        self._file.write(trailer)
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()

        sealed_path = self.path.with_suffix(".zst")
        os.rename(self.path, sealed_path)
        self._sealed = True

        return SegmentStats(
            segment_id=self.segment_id,
            bytes=footer_offset + len(footer) + len(trailer),
            records=len(self._records),
            footer_sha256=hashlib.sha256(footer).hexdigest(),
        )

    def __enter__(self) -> SegmentWriter:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if not self._file.closed:
            self._file.close()

    def _flush_block(self) -> None:
        compressed = compress_block(bytes(self._block_buffer), level=self._level)
        self._file.write(compressed)
        self._blocks.append(
            BlockEntry(
                offset=self._position, length=len(compressed), raw_length=self._block_raw_size
            )
        )
        self._position += len(compressed)
        self._compressed_bytes += len(compressed)
        self._block_no += 1
        self._block_buffer = bytearray()
        self._block_raw_size = 0
        self._pending_records = 0


def _open_for_exclusive_write(path: Path) -> IO[bytes]:
    """Open ``path`` for writing, failing if it already exists.

    A tiny wrapper so the writer's long-lived file handle isn't a bare
    ``open()`` call assigned outside a ``with`` statement.
    """
    return open(path, "xb")
