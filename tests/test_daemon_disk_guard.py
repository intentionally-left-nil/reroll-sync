"""Tests for the disk-space guard: pause below the floor, resume with hysteresis."""

from __future__ import annotations

import logging
from pathlib import Path

from reroll_sync.daemon.disk_guard import DiskGuard


def _guard(free_bytes: list[int], *, floor_bytes: int = 1000, hysteresis: float = 1.2) -> DiskGuard:
    calls = iter(free_bytes)

    def _disk_usage(path: Path) -> tuple[int, int, int]:
        return (0, 0, next(calls))

    return DiskGuard(
        Path("/tmp/segments"), floor_bytes, disk_usage=_disk_usage, hysteresis=hysteresis
    )


def test_default_disk_usage_and_logger_are_used_when_not_overridden():
    """Exercises the real `shutil.disk_usage`-backed default and the
    default logger, not just the fakes every other test injects.
    """
    guard = DiskGuard(Path("/"), floor_bytes=1)  # any real filesystem has > 1 byte free
    assert guard.check() is False


def test_starts_not_paused_when_space_is_plentiful():
    guard = _guard([10_000])
    assert guard.check() is False
    assert guard.is_paused() is False


def test_pauses_below_the_floor(caplog):
    guard = _guard([500])
    with caplog.at_level(logging.ERROR):
        paused = guard.check()
    assert paused is True
    assert guard.is_paused() is True
    assert any(record.levelno == logging.ERROR for record in caplog.records)


def test_resumes_above_floor_times_hysteresis():
    guard = _guard([500, 1_201])
    guard.check()
    assert guard.is_paused() is True
    paused = guard.check()
    assert paused is False
    assert guard.is_paused() is False


def test_does_not_resume_at_exactly_the_floor():
    guard = _guard([500, 1_000])
    guard.check()
    guard.check()
    assert guard.is_paused() is True


def test_does_not_flap_in_the_hysteresis_band_while_already_paused():
    guard = _guard([500, 1_100, 1_150, 1_199])
    guard.check()
    assert guard.is_paused() is True
    guard.check()
    assert guard.is_paused() is True
    guard.check()
    assert guard.is_paused() is True
    guard.check()
    assert guard.is_paused() is True


def test_does_not_flap_in_the_hysteresis_band_while_not_paused():
    guard = _guard([10_000, 1_100, 1_050])
    guard.check()
    assert guard.is_paused() is False
    guard.check()
    assert guard.is_paused() is False
    guard.check()
    assert guard.is_paused() is False


def test_resumes_exactly_at_floor_times_hysteresis():
    guard = _guard([500, 1_200])
    guard.check()
    guard.check()
    assert guard.is_paused() is False


def test_repeated_pauses_below_floor_do_not_log_more_than_once_per_transition(caplog):
    guard = _guard([500, 400])
    with caplog.at_level(logging.ERROR):
        guard.check()
        first_count = len(caplog.records)
        guard.check()
        second_count = len(caplog.records)
    assert first_count == 1
    assert second_count == 1


def test_a_stale_in_flight_check_cannot_undo_a_newer_pause():
    """Two `check()` calls overlap (the daemon's timer stage vs. a manual
    call): the first read disk usage *before* the second ran but applies
    its result *after*. Without serialization, the stale "plenty free"
    result unpauses the guard right after the newer "empty disk" check
    paused it -- the CI flake in
    `test_disk_guard_pause_blocks_fetch_defers_archive_append_then_resumes`,
    where the stage's startup tick was in flight across the test's
    `_disk_usage` patch.

    The newer check's own fake-`disk_usage` call is what releases the stale
    one, so the stale result always lands mid-pause when `check()` is not
    serialized. When it *is* serialized, the newer check blocks until the
    stale one finishes (the 0.5s stall bounds that wait), then pauses.
    """
    import threading

    started = threading.Event()
    release_stale = threading.Event()
    calls = []

    def _disk_usage(_path: Path) -> tuple[int, int, int]:
        calls.append(1)
        if len(calls) == 1:
            started.set()
            release_stale.wait(timeout=0.5)  # the "slow disk stat" stall
            return (0, 0, 10_000)  # stale read: plenty free, from before the pause
        release_stale.set()
        return (0, 0, 0)  # the newer read: empty disk

    guard = DiskGuard(Path("/tmp/segments"), 1000, disk_usage=_disk_usage)
    stale = threading.Thread(target=guard.check)
    stale.start()
    assert started.wait(timeout=5.0)  # the stale check is now inside `disk_usage`

    assert guard.check() is True  # the newer check pauses
    stale.join(timeout=5.0)

    assert guard.is_paused() is True
