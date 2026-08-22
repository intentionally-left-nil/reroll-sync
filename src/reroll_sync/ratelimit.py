"""Thread-safe token bucket rate limiting, plain and domain-hierarchical."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass


class TokenBucket:
    """A lazily-refilling token bucket.

    Tokens are computed from elapsed time on each call rather than by a
    background timer. ``penalize`` enforces a hard deadline (for a
    ``Retry-After`` response) distinct from ``drain``, since a drained
    bucket still refills on the next tick.

    Pass ``condition`` to share one lock across several buckets (as
    :class:`HierarchicalLimiter` does for its global and child buckets); a
    standalone bucket left to create its own condition is thread-safe on
    its own.
    """

    def __init__(
        self,
        rate_per_minute: float,
        burst: float,
        now: Callable[[], float],
        *,
        condition: threading.Condition | None = None,
    ) -> None:
        self._rate_per_second = rate_per_minute / 60.0
        self._burst = burst
        self._now = now
        self._condition = condition if condition is not None else threading.Condition()
        self._tokens = burst
        self._last_refill = now()
        self._penalty_deadline = float("-inf")

    def try_acquire(self, n: float = 1) -> bool:
        """Attempt a non-blocking acquisition of ``n`` tokens."""
        with self._condition:
            return self._try_take_locked(n)

    def acquire(self, n: float = 1, timeout: float | None = None) -> bool:
        """Block until ``n`` tokens are available, or ``timeout`` elapses.

        Waits using ``threading.Condition.wait`` with a timeout computed
        from the current deficit and refill rate, then re-checks; it never
        busy-loops. Returns ``False`` immediately if ``n`` exceeds the
        bucket's burst, since that could never be satisfied.
        """
        deadline = None if timeout is None else self._now() + timeout
        with self._condition:
            while True:
                if self._try_take_locked(n):
                    return True
                wait_for = self._time_until_available_locked(n)
                if wait_for is None:
                    return False
                if deadline is not None:
                    remaining = deadline - self._now()
                    if remaining <= 0:
                        return False
                    wait_for = min(wait_for, remaining)
                self._condition.wait(timeout=wait_for)

    def available(self) -> float:
        """Return the current token count, refilling first."""
        with self._condition:
            self._refill_locked()
            return self._tokens

    def drain(self) -> None:
        """Set the token count to zero.

        Also resets the refill clock to now, so idle time accrued before
        the drain is not credited back once refilling resumes.
        """
        with self._condition:
            self._tokens = 0.0
            self._last_refill = self._now()

    def penalize(self, seconds: float) -> None:
        """Refuse all acquisitions for ``seconds``.

        A call while a penalty is already active only ever extends the
        deadline, never shortens it.
        """
        with self._condition:
            deadline = self._now() + seconds
            if deadline > self._penalty_deadline:
                self._penalty_deadline = deadline

    def penalty_deadline(self) -> float:
        """Return the timestamp (per ``now``) until which acquisitions are refused."""
        with self._condition:
            return self._penalty_deadline

    def is_penalized(self) -> bool:
        """Return whether a ``penalize`` deadline is currently active."""
        with self._condition:
            return self._now() < self._penalty_deadline

    def time_until_available(self, n: float = 1) -> float | None:
        """Return seconds until ``n`` tokens would be available.

        Returns ``None`` if ``n`` exceeds ``burst`` and could never be
        satisfied.
        """
        with self._condition:
            return self._time_until_available_locked(n)

    def return_tokens(self, n: float) -> None:
        """Return ``n`` previously-acquired tokens, capped at ``burst``."""
        with self._condition:
            self._tokens = min(self._burst, self._tokens + n)
            self._condition.notify_all()

    def _refill_locked(self) -> None:
        now = self._now()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate_per_second)
            self._last_refill = now

    def _try_take_locked(self, n: float) -> bool:
        now = self._now()
        if now < self._penalty_deadline:
            return False
        self._refill_locked()
        if n > self._burst or self._tokens < n:
            return False
        self._tokens -= n
        return True

    def _time_until_available_locked(self, n: float) -> float | None:
        if n > self._burst:
            return None
        wait_for_penalty = max(0.0, self._penalty_deadline - self._now())
        self._refill_locked()
        deficit = n - self._tokens
        if deficit <= 0:
            return wait_for_penalty
        if self._rate_per_second <= 0:
            return None
        return max(wait_for_penalty, deficit / self._rate_per_second)


@dataclass(frozen=True)
class ChildLimiterSnapshot:
    """A single child bucket's state within a :class:`HierarchicalLimiter`."""

    available: float
    acquired: int
    denied: int
    penalty_deadline: float


@dataclass(frozen=True)
class LimiterSnapshot:
    """The full state of a :class:`HierarchicalLimiter` at a point in time."""

    global_available: float
    children: Mapping[str, ChildLimiterSnapshot]


class HierarchicalLimiter:
    """A global rate cap shared by named child buckets, each with a reserve.

    Every acquisition draws from both the named child bucket and the shared
    global bucket, atomically: if the global bucket cannot satisfy the
    request, tokens already taken from the child are returned. A child may
    also draw directly from the global bucket once its own reserve is
    exhausted, but only while every other child has gone unrequested for at
    least as long as one of its own reserve tokens takes to refill. Every
    grant, reserved or borrowed, counts against the same global bucket, so
    the sum of grants across all children can never exceed the global rate.
    """

    def __init__(
        self,
        global_rate_per_minute: float,
        children: Mapping[str, float],
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._now = now
        self._condition = threading.Condition()
        self._global = TokenBucket(
            global_rate_per_minute,
            _one_second_burst(global_rate_per_minute),
            now,
            condition=self._condition,
        )
        self._children = {
            name: TokenBucket(rate, _one_second_burst(rate), now, condition=self._condition)
            for name, rate in children.items()
        }
        self._reserve_rates: dict[str, float] = dict(children)
        self._last_attempt: dict[str, float] = dict.fromkeys(children, float("-inf"))
        self._acquired: dict[str, int] = dict.fromkeys(children, 0)
        self._denied: dict[str, int] = dict.fromkeys(children, 0)

    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        """Acquire ``n`` tokens from ``child_name`` and the global bucket atomically.

        Raises ``KeyError`` if ``child_name`` is not a configured child.
        """
        child = self._children[child_name]
        deadline = None if timeout is None else self._now() + timeout
        with self._condition:
            self._last_attempt[child_name] = self._now()
            while True:
                if self._try_take_locked(child_name, child, n):
                    self._acquired[child_name] += 1
                    return True
                wait_for = self._time_until_locked(child_name, child, n)
                if wait_for is None:
                    self._denied[child_name] += 1
                    return False
                if deadline is not None:
                    remaining = deadline - self._now()
                    if remaining <= 0:
                        self._denied[child_name] += 1
                        return False
                    wait_for = min(wait_for, remaining)
                self._condition.wait(timeout=wait_for)

    def penalize(self, child_name: str, seconds: float) -> None:
        """Refuse acquisitions for ``child_name`` for ``seconds``; other children are unaffected."""
        self._children[child_name].penalize(seconds)

    def snapshot(self) -> LimiterSnapshot:
        """Return per-child and global available tokens, counters, and penalty deadlines."""
        with self._condition:
            children = {
                name: ChildLimiterSnapshot(
                    available=bucket.available(),
                    acquired=self._acquired[name],
                    denied=self._denied[name],
                    penalty_deadline=bucket.penalty_deadline(),
                )
                for name, bucket in self._children.items()
            }
            return LimiterSnapshot(global_available=self._global.available(), children=children)

    def _try_take_locked(self, child_name: str, child: TokenBucket, n: float) -> bool:
        if child.is_penalized():
            return False
        if child.try_acquire(n):
            if self._global.try_acquire(n):
                return True
            child.return_tokens(n)
            return False
        if not self._siblings_idle_locked(child_name):
            return False
        return self._global.try_acquire(n)

    def _siblings_idle_locked(self, child_name: str) -> bool:
        return self._time_until_siblings_idle_locked(child_name) <= 0.0

    def _time_until_siblings_idle_locked(self, child_name: str) -> float:
        """Return seconds until every other child has gone idle.

        A sibling is idle once ``idle_threshold`` seconds (the time one of
        its own reserve tokens takes to refill) have passed since its last
        acquisition attempt. Returns ``0.0`` if all siblings are already
        idle.
        """
        now = self._now()
        wait = 0.0
        for name in self._children:
            if name == child_name:
                continue
            rate = self._reserve_rates[name]
            idle_threshold = 60.0 / rate if rate > 0 else 0.0
            remaining = idle_threshold - (now - self._last_attempt[name])
            wait = max(wait, remaining)
        return wait

    def _time_until_locked(self, child_name: str, child: TokenBucket, n: float) -> float | None:
        """Return seconds until ``child`` could acquire ``n`` tokens, or ``None`` if never.

        Mirrors the branches of ``_try_take_locked``: if the child's own
        reserve already has ``n`` tokens, the only gate left is the global
        bucket's refill. Otherwise the child must wait for its own refill
        or for the sibling-idle borrowing gate to open (whichever comes
        first), and either way the global bucket must also have ``n``
        tokens.
        """
        own_wait = child.time_until_available(n)
        global_wait = self._global.time_until_available(n)
        if global_wait is None:
            return None
        if own_wait == 0.0:
            return global_wait
        borrow_wait = max(self._time_until_siblings_idle_locked(child_name), global_wait)
        if own_wait is None:
            return borrow_wait
        return min(own_wait, borrow_wait)


def _one_second_burst(rate_per_minute: float) -> float:
    """Return the token-bucket burst for a configured rate.

    One second's worth of the rate, floored at ``1.0``: a bucket's cap
    also bounds how many tokens refilling can ever accumulate, so a rate
    below 60/minute would otherwise never hold a whole token and could
    never satisfy even a single-token request from its own reserve --
    permanently starving it whenever a sibling never goes idle long enough
    to borrow from (see ``HierarchicalLimiter._siblings_idle_locked``).
    """
    return max(1.0, rate_per_minute / 60.0)
