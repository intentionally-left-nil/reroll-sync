"""Pauses fetch/archive when free disk space on the segments volume runs low.

Guards against filling a shared volume that also holds other data (e.g.
Docker images): the segment store only ever grows, so this is the one place
that can push back before disk fills up.
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

_DiskUsage = tuple[int, int, int]
"""``(total, used, free)`` in bytes -- what ``shutil.disk_usage`` returns."""


class DiskGuardLike(Protocol):
    """The one method callers outside this module need from a disk guard.

    Lets `FetchStage` and `_DiskBreakerGuardedStore` (``daemon/service.py``,
    ``daemon/stages/fetch.py``) accept a test fake without it needing to
    subclass :class:`DiskGuard`.
    """

    def is_paused(self) -> bool:
        raise NotImplementedError


class DiskGuardPausedError(Exception):
    """Raised in place of an archive append while the disk guard is paused.

    Not a subclass of :class:`OSError`: this is a deliberate, expected
    refusal to write (free space below the floor), not an I/O failure, so
    it must never be mistaken for one by code that pattern-matches on
    ``OSError``.
    """


class DiskGuard:
    """Tracks whether ``path``'s free space is below ``floor_bytes``.

    Resumes only once free space climbs back above ``floor_bytes *
    hysteresis`` (default 20% headroom), so a volume hovering near the
    floor doesn't flap fetch/archive on and off every check.
    """

    def __init__(
        self,
        path: Path,
        floor_bytes: int,
        *,
        disk_usage: Callable[[Path], _DiskUsage] = shutil.disk_usage,
        hysteresis: float = 1.2,
        logger: logging.Logger | None = None,
    ) -> None:
        self._path = path
        self._floor_bytes = floor_bytes
        self._disk_usage = disk_usage
        self._hysteresis = hysteresis
        self._logger = logger if logger is not None else logging.getLogger("reroll_sync.disk_guard")
        self._paused = False

    def check(self) -> bool:
        """Refresh paused state from current free space. Returns the new state."""
        _total, _used, free = self._disk_usage(self._path)
        if self._paused:
            if free >= self._floor_bytes * self._hysteresis:
                self._paused = False
                self._logger.warning(
                    "disk guard: %d bytes free on %s, above floor %d bytes x %.2f hysteresis; "
                    "resuming fetch and archive",
                    free,
                    self._path,
                    self._floor_bytes,
                    self._hysteresis,
                )
        elif free < self._floor_bytes:
            self._paused = True
            self._logger.error(
                "disk guard: %d bytes free on %s, below floor %d bytes; pausing fetch and archive",
                free,
                self._path,
                self._floor_bytes,
            )
        return self._paused

    def is_paused(self) -> bool:
        return self._paused
