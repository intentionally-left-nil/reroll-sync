"""Exceptions raised by the archive package."""

from __future__ import annotations


class CorruptSegmentError(Exception):
    """A segment file's on-disk bytes do not match the expected format.

    Raised by :class:`~reroll_sync.archive.reader.SegmentReader` when a
    segment's trailer, footer, or a record's bytes fail an integrity check.
    """

    def __init__(self, segment_id: int, reason: str):
        self.segment_id = segment_id
        self.reason = reason
        super().__init__(f"segment {segment_id:06d}: {reason}")
