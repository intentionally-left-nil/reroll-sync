"""Reads records back out of sealed archive segment files.

See ``reroll_sync.archive`` for the on-disk segment format.
"""

from __future__ import annotations

import hashlib
import os
from collections import OrderedDict
from collections.abc import Callable, Iterator
from pathlib import Path

from .errors import CorruptSegmentError
from .format import (
    TRAILER_SIZE,
    Footer,
    decode_footer,
    decode_trailer,
    decompress_block,
    footer_crc32,
)
from .location import BlobLocation

DEFAULT_BLOCK_CACHE_SIZE = 4


class SegmentReader:
    """Random-access and streaming reads over sealed segments in ``directory``.

    Caches each segment's footer after the first read, and keeps the most
    recently decompressed blocks (an LRU of ``block_cache_size``) so that
    sequential reads within one block decompress it only once.
    """

    def __init__(
        self,
        directory: Path,
        *,
        block_cache_size: int = DEFAULT_BLOCK_CACHE_SIZE,
        decompress: Callable[[bytes], bytes] = decompress_block,
    ):
        self._directory = Path(directory)
        self._block_cache_size = block_cache_size
        self._decompress = decompress
        self._footers: dict[int, Footer] = {}
        self._blocks: OrderedDict[tuple[int, int], bytes] = OrderedDict()

    def read(self, location: BlobLocation) -> bytes:
        """Return the bytes at ``location``, verified against its sha256.

        Raises :class:`CorruptSegmentError` if the bytes at that location
        do not hash to ``location.sha256``.
        """
        block = self._block_bytes(location.segment_id, location.block_no)
        data = block[location.offset : location.offset + location.length]
        if hashlib.sha256(data).hexdigest() != location.sha256:
            raise CorruptSegmentError(
                location.segment_id,
                f"record at block {location.block_no} offset {location.offset} "
                "failed its sha256 check",
            )
        return data

    def iter_records(self, segment_id: int) -> Iterator[tuple[str, bytes]]:
        """Yield every ``(sha256, bytes)`` in a segment, in file order.

        Decompresses each block exactly once, streaming rather than caching.
        """
        footer = self._footer_for(segment_id)
        by_block: dict[int, list] = {}
        for record in footer.records:
            by_block.setdefault(record.block_no, []).append(record)

        with open(self._segment_path(segment_id), "rb") as handle:
            for block_no, block_entry in enumerate(footer.blocks):
                handle.seek(block_entry.offset)
                compressed = handle.read(block_entry.length)
                payload = self._decompress(compressed)
                for record in by_block.get(block_no, []):
                    data = payload[record.offset : record.offset + record.length]
                    yield record.sha256.hex(), data

    def footer_records(self, segment_id: int) -> tuple[tuple[str, int, int, int], ...]:
        """Return every ``(sha256, block_no, offset, length)`` in a segment's footer."""
        footer = self._footer_for(segment_id)
        return tuple(
            (record.sha256.hex(), record.block_no, record.offset, record.length)
            for record in footer.records
        )

    def _footer_for(self, segment_id: int) -> Footer:
        cached = self._footers.get(segment_id)
        if cached is not None:
            return cached

        path = self._segment_path(segment_id)
        size = path.stat().st_size
        if size < TRAILER_SIZE:
            raise CorruptSegmentError(
                segment_id, f"file is {size} bytes, shorter than the {TRAILER_SIZE}-byte trailer"
            )

        with open(path, "rb") as handle:
            handle.seek(-TRAILER_SIZE, os.SEEK_END)
            trailer = handle.read(TRAILER_SIZE)
            footer_offset, footer_length, expected_crc = decode_trailer(
                trailer, segment_id=segment_id
            )
            if footer_offset + footer_length + TRAILER_SIZE > size:
                raise CorruptSegmentError(segment_id, "trailer points outside the file")
            handle.seek(footer_offset)
            compressed_footer = handle.read(footer_length)

        if footer_crc32(compressed_footer) != expected_crc:
            raise CorruptSegmentError(segment_id, "footer crc32 mismatch")

        footer = decode_footer(compressed_footer, segment_id=segment_id)
        self._footers[segment_id] = footer
        return footer

    def _segment_path(self, segment_id: int) -> Path:
        return self._directory / f"{segment_id:06d}.zst"

    def _block_bytes(self, segment_id: int, block_no: int) -> bytes:
        key = (segment_id, block_no)
        cached = self._blocks.get(key)
        if cached is not None:
            self._blocks.move_to_end(key)
            return cached

        footer = self._footer_for(segment_id)
        entry = footer.blocks[block_no]
        with open(self._segment_path(segment_id), "rb") as handle:
            handle.seek(entry.offset)
            compressed = handle.read(entry.length)
        data = self._decompress(compressed)

        self._blocks[key] = data
        if len(self._blocks) > self._block_cache_size:
            self._blocks.popitem(last=False)
        return data
