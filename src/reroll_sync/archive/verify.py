"""Read-only integrity pass over an archive: :func:`verify_archive`.

Never repairs; only reports discrepancies between segment footers, the
files on disk, and the ``blobs`` table. Streams one segment at a time and
never decompresses more than the block currently being checked, and chunks
``blobs`` lookups by ``segment_id``, so it never holds a long-lived read
transaction.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from .errors import CorruptSegmentError
from .store import ArchiveStore


@dataclass(frozen=True)
class VerifyReport:
    """Discrepancies found by :func:`verify_archive`; empty means clean."""

    problems: tuple[str, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        return not self.problems


def verify_archive(store: ArchiveStore) -> VerifyReport:
    """Check every sealed segment's footer against disk and the ``blobs`` table."""
    problems: list[str] = []
    for segment_id in store.sealed_segment_ids():
        problems.extend(_verify_segment(store, segment_id))
    return VerifyReport(tuple(problems))


def _verify_segment(store: ArchiveStore, segment_id: int) -> list[str]:
    prefix = f"segment {segment_id:06d}"
    problems: list[str] = []
    try:
        footer_entries = store.reader.footer_records(segment_id)
        for claimed_sha256, data in store.reader.iter_records(segment_id):
            actual_sha256 = hashlib.sha256(data).hexdigest()
            if actual_sha256 != claimed_sha256:
                problems.append(
                    f"{prefix}: footer record {claimed_sha256} does not match its bytes "
                    f"(actual sha256 {actual_sha256})"
                )
    except (CorruptSegmentError, OSError) as exc:
        return [f"{prefix}: {exc}"]

    footer_positions = {
        sha: (block_no, offset, length) for sha, block_no, offset, length in footer_entries
    }
    blob_positions = {
        sha: (block_no, offset, length)
        for sha, block_no, offset, length in store.blob_rows_for_segment(segment_id)
    }

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
