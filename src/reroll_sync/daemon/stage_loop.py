"""A supervised loop that runs one pipeline stage on its own schedule.

Every stage in the daemon -- ``index_poll``, ``project_sync``, ``fetch``,
``convert``, ``gc`` -- is a :class:`StageLoop` wrapping that stage's own
claim/process function (see ``daemon/stages.py``). This module knows
nothing about sqlite, HTTP, or any other dependency: only when to run,
whether it's paused, and how to recover from ``iterate``'s exceptions.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol


class Trigger(Protocol):
    """Decides whether a :class:`StageLoop` iteration is due, and how long to wait if not."""

    def due(self) -> bool:
        """Return whether an iteration should run now."""
        raise NotImplementedError

    def wait_seconds(self) -> float:
        """Return how long :meth:`StageLoop.run_forever` should wait before re-checking."""
        raise NotImplementedError

    def mark_run(self, did_work: bool) -> None:
        """Record that an iteration just ran, and whether it found work to do."""
        raise NotImplementedError


class IntervalTrigger:
    """Due once every ``interval`` seconds, regardless of whether there was work.

    Used by timer-driven stages (``index_poll``, ``gc``).
    """

    def __init__(self, interval: float, *, now: Callable[[], float] = time.monotonic) -> None:
        self._interval = interval
        self._now = now
        self._last_run_at: float | None = None

    def due(self) -> bool:
        if self._last_run_at is None:
            return True
        return self._now() - self._last_run_at >= self._interval

    def wait_seconds(self) -> float:
        if self._last_run_at is None:
            return 0.0
        remaining = self._interval - (self._now() - self._last_run_at)
        return max(0.0, remaining)

    def mark_run(self, did_work: bool) -> None:
        del did_work
        self._last_run_at = self._now()


class PollTrigger:
    """Due immediately after finding work; otherwise waits ``idle_interval``.

    Used by queue-driven stages (``project_sync``, ``fetch``, ``convert``):
    "due" here means "worth checking the queue again", not "the queue is
    known to be non-empty" -- that check is `iterate`'s own job, reported
    back to `mark_run` as `did_work`.
    """

    def __init__(self, idle_interval: float) -> None:
        self._idle_interval = idle_interval
        self._busy = True

    def due(self) -> bool:
        return True

    def wait_seconds(self) -> float:
        return 0.0 if self._busy else self._idle_interval

    def mark_run(self, did_work: bool) -> None:
        self._busy = did_work


@dataclass(frozen=True)
class StageLoopStats:
    """A point-in-time snapshot of one :class:`StageLoop`'s health counters."""

    last_run_at: float | None
    last_success_at: float | None
    consecutive_failures: int
    paused: bool


class StageLoop:
    """Runs ``iterate`` on ``trigger``'s schedule until ``shutdown_event`` is set.

    ``iterate`` returns whether it found work to do, which triggers like
    :class:`PollTrigger` use to decide whether to run again immediately or
    wait; a stage with nothing meaningful to report can always return
    ``False``.

    Exceptions of a type in ``fatal_exceptions`` -- a stage's own narrow,
    documented whitelist of configuration/programming errors -- propagate
    out of :meth:`run_once`/:meth:`run_forever` uncaught, so the thread
    dies and the daemon can crash loudly instead of limping along with a
    broken stage. Every other exception is caught, logged, and counted;
    the loop continues.
    """

    def __init__(
        self,
        name: str,
        iterate: Callable[[], bool],
        trigger: Trigger,
        shutdown_event: threading.Event,
        *,
        now: Callable[[], float] = time.monotonic,
        logger: logging.Logger | None = None,
        fatal_exceptions: tuple[type[BaseException], ...] = (),
    ) -> None:
        self.name = name
        self._iterate = iterate
        self._trigger = trigger
        self._shutdown_event = shutdown_event
        self._now = now
        self._logger = logger if logger is not None else logging.getLogger(f"reroll_sync.{name}")
        self._fatal_exceptions = fatal_exceptions
        self._lock = threading.Lock()
        self._paused = False
        self._last_run_at: float | None = None
        self._last_success_at: float | None = None
        self._consecutive_failures = 0

    def pause(self) -> None:
        """Stop claiming new work; work already in flight is unaffected."""
        with self._lock:
            self._paused = True

    def resume(self) -> None:
        """Resume claiming new work."""
        with self._lock:
            self._paused = False

    def is_paused(self) -> bool:
        with self._lock:
            return self._paused

    def stats(self) -> StageLoopStats:
        """Return a snapshot of this loop's last-run/last-success/failure counters."""
        with self._lock:
            return StageLoopStats(
                last_run_at=self._last_run_at,
                last_success_at=self._last_success_at,
                consecutive_failures=self._consecutive_failures,
                paused=self._paused,
            )

    def run_once(self) -> bool:
        """Run one iteration if not paused. Returns whether work was done.

        A paused stage claims nothing: `iterate` is not called at all, so
        it never even attempts a claim, and returns ``False``.
        """
        with self._lock:
            self._last_run_at = self._now()
            paused = self._paused
        if paused:
            self._trigger.mark_run(False)
            return False

        try:
            did_work = self._iterate()
        except self._fatal_exceptions:
            raise
        except Exception:
            with self._lock:
                self._consecutive_failures += 1
            self._logger.error(
                "stage %r: unhandled exception in iteration", self.name, exc_info=True
            )
            self._trigger.mark_run(False)
            return False

        with self._lock:
            self._last_success_at = self._now()
            self._consecutive_failures = 0
        self._trigger.mark_run(bool(did_work))
        return bool(did_work)

    def run_forever(self) -> None:
        """Run :meth:`run_once` on ``trigger``'s schedule until ``shutdown_event`` is set.

        Every wait is bounded (``shutdown_event.wait(timeout=...)``), so an
        already-set event always stops this promptly, even mid-interval.
        """
        while not self._shutdown_event.is_set():
            if self._trigger.due():
                self.run_once()
            wait = self._trigger.wait_seconds()
            if self._shutdown_event.wait(timeout=wait):
                break
