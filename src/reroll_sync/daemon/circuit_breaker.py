"""Per-dependency circuit breaker: closed -> open -> half-open -> closed/open.

One instance per external dependency (``pypi.org``, ``files.pythonhosted.org``,
local disk), not per stage: a stage that depends on more than one of these
checks each breaker it actually calls, so an outage in one dependency pauses
only the stages that call it -- see ``daemon/stages.py``.
"""

from __future__ import annotations

import enum
import threading
import time
from collections.abc import Callable
from typing import Any


class CircuitState(enum.Enum):
    """One breaker's current state."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(Exception):
    """Raised by :meth:`CircuitBreaker.call` when no call may proceed right now."""


class CircuitBreaker:
    """Tracks one dependency's health and gates calls to it.

    ``failure_threshold`` consecutive failures open the breaker; after
    ``recovery_timeout`` seconds it moves to half-open and allows exactly one
    trial call. A successful trial closes it; a failed one reopens it (and
    restarts the recovery timer). Thread-safe: call from every stage thread
    that depends on this dependency.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        *,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._now = now
        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._half_open_trial_in_flight = False

    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def consecutive_failures(self) -> int:
        """Return the current streak of consecutive failures since the last success."""
        with self._lock:
            return self._consecutive_failures

    def next_trial_at(self) -> float | None:
        """Return when (per the injected clock) the next half-open trial becomes due.

        ``None`` unless the breaker is currently ``OPEN``.
        """
        with self._lock:
            if self._state != CircuitState.OPEN or self._opened_at is None:
                return None
            return self._opened_at + self._recovery_timeout

    def allow(self) -> bool:
        """Return whether a call may proceed now.

        Advances ``OPEN`` to ``HALF_OPEN`` once ``recovery_timeout`` has
        elapsed. In ``HALF_OPEN``, grants exactly one trial at a time:
        a second call before the first trial resolves is refused.
        """
        with self._lock:
            if self._state == CircuitState.OPEN:
                if not self._recovery_due_locked():
                    return False
                self._state = CircuitState.HALF_OPEN
                self._half_open_trial_in_flight = False
            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_trial_in_flight:
                    return False
                self._half_open_trial_in_flight = True
                return True
            return True

    def record_success(self) -> None:
        """Close the breaker and reset its failure count."""
        with self._lock:
            self._consecutive_failures = 0
            self._state = CircuitState.CLOSED
            self._opened_at = None
            self._half_open_trial_in_flight = False

    def record_failure(self) -> None:
        """Count a failure, opening the breaker if it just crossed the threshold.

        A failure while half-open always reopens it immediately (a failed
        trial), regardless of ``failure_threshold``.
        """
        with self._lock:
            self._consecutive_failures += 1
            if self._state == CircuitState.HALF_OPEN or (
                self._consecutive_failures >= self._failure_threshold
            ):
                self._state = CircuitState.OPEN
                self._opened_at = self._now()
                self._half_open_trial_in_flight = False

    def call(self, fn: Callable[[], Any], *, exempt: tuple[type[BaseException], ...] = ()) -> Any:
        """Call ``fn`` if the breaker allows it, updating state from the outcome.

        Raises :class:`CircuitBreakerOpenError` (without calling ``fn`` at
        all) when the breaker refuses. An exception whose type is in
        ``exempt`` (e.g. ``PyPIRateLimited``: throttling is expected
        behaviour, not a dependency failure) propagates without being
        counted as either a success or a failure, releasing a half-open
        trial slot so the next call may retry.
        """
        if not self.allow():
            raise CircuitBreakerOpenError("circuit breaker is open")
        try:
            result = fn()
        except exempt:
            self._release_trial()
            raise
        except Exception:
            self.record_failure()
            raise
        else:
            self.record_success()
            return result

    def _recovery_due_locked(self) -> bool:
        assert self._opened_at is not None
        return self._now() - self._opened_at >= self._recovery_timeout

    def _release_trial(self) -> None:
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_trial_in_flight = False
