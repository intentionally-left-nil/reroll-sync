"""The location of one content-addressed blob within a segment file."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BlobLocation:
    """Where one record's bytes live: its segment, block, and byte range.

    ``sha256`` is the hex digest of the record's raw bytes, matching the
    ``blobs.sha256`` column so a location round-trips into that table
    without conversion.
    """

    sha256: str
    segment_id: int
    block_no: int
    offset: int
    length: int
