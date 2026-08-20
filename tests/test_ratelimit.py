"""Tests for TokenBucket and HierarchicalLimiter."""

from __future__ import annotations

import threading
from typing import Protocol

import pytest

from reroll_sync.ratelimit import HierarchicalLimiter, TokenBucket


class Clock(Protocol):
    """A clock usable by :class:`TokenBucket` and :class:`HierarchicalLimiter`."""

    def now(self) -> float: ...


class FakeClock:
    """A manually-advanced clock for deterministic rate-limiter tests."""

    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class AutoAdvanceClock:
    """A clock that ticks forward by ``step`` on every read.

    Simulates time passing as blocked code repeatedly checks the clock,
    without any thread sleeping for real wall-clock time.
    """

    def __init__(self, start: float = 0.0, step: float = 0.01) -> None:
        self.value = start
        self.step = step

    def now(self) -> float:
        self.value += self.step
        return self.value


# --- TokenBucket -------------------------------------------------------


def test_fresh_bucket_permits_burst_then_refuses():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=3, now=clock.now)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_refill_after_elapsed_time_grants_one_token():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=1, now=clock.now)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False
    clock.advance(60 / 60)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_tokens_never_exceed_burst():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=5, now=clock.now)
    clock.advance(10_000)
    assert bucket.available() == 5


def test_try_acquire_more_than_burst_always_false():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=5, now=clock.now)
    clock.advance(10_000)
    assert bucket.try_acquire(6) is False
    assert bucket.available() == 5


def test_acquire_timeout_zero_on_empty_returns_false_immediately():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=1, now=clock.now)
    bucket.drain()
    assert bucket.acquire(timeout=0) is False


def test_acquire_with_n_exceeding_burst_returns_false_without_blocking():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=1, now=clock.now)
    assert bucket.acquire(2, timeout=5) is False


def test_acquire_blocks_then_succeeds_once_clock_advances_enough():
    clock = AutoAdvanceClock(step=0.005)
    bucket = TokenBucket(rate_per_minute=600, burst=1, now=clock.now)
    assert bucket.try_acquire() is True
    assert bucket.acquire(timeout=5) is True


def test_bucket_acquire_with_no_timeout_blocks_then_succeeds():
    clock = AutoAdvanceClock(step=0.005)
    bucket = TokenBucket(rate_per_minute=600, burst=1, now=clock.now)
    assert bucket.try_acquire() is True
    assert bucket.acquire() is True


def test_acquire_blocking_from_second_thread_advancing_clock():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=6000, burst=1, now=clock.now)
    assert bucket.try_acquire() is True

    def _advance_soon() -> None:
        clock.advance(1.0)

    timer = threading.Timer(0.01, _advance_soon)
    timer.start()
    try:
        assert bucket.acquire(timeout=2) is True
    finally:
        timer.cancel()


def test_drain_empties_bucket_then_refill_still_works():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=3, now=clock.now)
    bucket.drain()
    assert bucket.try_acquire() is False
    clock.advance(60 / 60)
    assert bucket.try_acquire() is True


def test_drain_after_idle_period_does_not_refill_for_the_idle_time():
    # The bucket accrues no *tracked* refill while nothing calls a
    # refill-triggering method, so draining after a long idle period must
    # not credit that idle time once refilling resumes -- drain resets the
    # refill clock, not just the token count.
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=3, now=clock.now)  # 1/sec
    clock.advance(50)  # long idle period with no calls into the bucket
    bucket.drain()
    clock.advance(1)  # only one real second has passed since draining
    assert bucket.available() == pytest.approx(1.0)


def test_penalize_refuses_full_bucket_until_deadline():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=3, now=clock.now)
    bucket.penalize(60)
    assert bucket.try_acquire() is False
    clock.advance(59.999)
    assert bucket.try_acquire() is False
    clock.advance(0.002)
    assert bucket.try_acquire() is True


def test_penalize_while_penalized_extends_but_never_shortens():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=3, now=clock.now)
    bucket.penalize(60)
    bucket.penalize(10)
    assert bucket.penalty_deadline() == 60
    bucket.penalize(120)
    assert bucket.penalty_deadline() == 120


def test_fractional_rate_does_not_divide_by_zero_or_refill_negative():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=0.5, burst=1, now=clock.now)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False
    clock.advance(0.001)
    assert bucket.available() >= 0


def test_zero_rate_never_refills_and_acquire_does_not_hang():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=0, burst=1, now=clock.now)
    assert bucket.try_acquire() is True
    clock.advance(1_000_000)
    assert bucket.try_acquire() is False
    assert bucket.acquire(timeout=1) is False


def test_clock_going_backwards_does_not_create_tokens():
    clock = FakeClock(start=100.0)
    bucket = TokenBucket(rate_per_minute=60, burst=1, now=clock.now)
    assert bucket.try_acquire() is True
    clock.value = 0.0
    assert bucket.try_acquire() is False
    clock.value = 101.0
    assert bucket.try_acquire() is True


def test_available_reflects_lazy_refill():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=1, burst=2, now=clock.now)
    bucket.drain()
    assert bucket.available() == 0
    clock.advance(30)
    assert bucket.available() == pytest.approx(0.5)


def test_return_tokens_caps_at_burst_and_wakes_waiters():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=2, now=clock.now)
    bucket.return_tokens(100)
    assert bucket.available() == 2

    released = threading.Event()

    def _release() -> None:
        released.wait(timeout=2)
        bucket.return_tokens(1)

    bucket.drain()
    thread = threading.Thread(target=_release)
    thread.start()
    released.set()
    try:
        assert bucket.acquire(timeout=2) is True
    finally:
        thread.join()


def test_time_until_available_for_n_exceeding_burst_is_none():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=1, now=clock.now)
    assert bucket.time_until_available(2) is None


def test_time_until_available_accounts_for_active_penalty():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_minute=60, burst=1, now=clock.now)
    bucket.penalize(30)
    assert bucket.time_until_available() == pytest.approx(30)


# --- HierarchicalLimiter -------------------------------------------------


def _limiter(clock: Clock, global_rate=2000, children=None) -> HierarchicalLimiter:
    if children is None:
        children = {"pypi.org": 200, "files.pythonhosted.org": 1800}
    return HierarchicalLimiter(global_rate, children, now=clock.now)


def test_acquire_decrements_both_child_and_global():
    clock = FakeClock()
    limiter = _limiter(clock)
    before = limiter.snapshot()
    assert limiter.acquire("pypi.org") is True
    after = limiter.snapshot()
    assert after.children["pypi.org"].available == before.children["pypi.org"].available - 1
    assert after.global_available == before.global_available - 1
    assert after.children["files.pythonhosted.org"].available == (
        before.children["files.pythonhosted.org"].available
    )


def test_atomic_rollback_when_global_empty_and_child_full():
    clock = FakeClock()
    # Child reserve dwarfs the global rate so the child bucket stays full
    # while the global bucket is drained directly -- this is the only way
    # to force "child has tokens, global does not" and actually exercise
    # the rollback branch, rather than draining both buckets in lockstep.
    limiter = _limiter(clock, global_rate=60, children={"pypi.org": 6000})
    limiter._global.drain()
    before = limiter.snapshot().children["pypi.org"].available
    assert before > 0  # sanity: the child bucket genuinely still has tokens
    assert limiter.acquire("pypi.org", timeout=0) is False
    after = limiter.snapshot().children["pypi.org"].available
    assert after == before


def test_one_child_idle_active_child_approaches_global_rate():
    clock = FakeClock()
    limiter = _limiter(clock)
    window_seconds = 120.0
    step = 0.001  # attempt far faster than either bucket's rate to force borrowing
    grants = 0
    ticks = int(window_seconds / step)
    for _ in range(ticks):
        clock.advance(step)
        if limiter.acquire("files.pythonhosted.org", timeout=0):
            grants += 1
    rate_per_minute = grants / (window_seconds / 60)
    assert rate_per_minute > 1800  # exceeds its own reserve by borrowing pypi.org's slack
    global_burst = 2000 / 60
    max_allowed = 2000 + global_burst / (window_seconds / 60)  # burst amortized over the window
    assert rate_per_minute <= max_allowed


def test_both_children_saturated_each_gets_at_least_reserve():
    clock = FakeClock()
    limiter = _limiter(clock)
    window_seconds = 120.0
    step = 0.005
    ticks = int(window_seconds / step)
    for _ in range(ticks):
        clock.advance(step)
        limiter.acquire("pypi.org", timeout=0)
        limiter.acquire("files.pythonhosted.org", timeout=0)
    snapshot = limiter.snapshot()
    window_minutes = window_seconds / 60
    assert snapshot.children["pypi.org"].acquired >= 200 * window_minutes * 0.9
    assert snapshot.children["files.pythonhosted.org"].acquired >= 1800 * window_minutes * 0.9


def test_sum_of_grants_never_exceeds_global_rate_over_ten_minute_window():
    clock = FakeClock()
    limiter = _limiter(clock)
    window_seconds = 10 * 60.0
    step = 0.01
    ticks = int(window_seconds / step)
    for _ in range(ticks):
        clock.advance(step)
        limiter.acquire("pypi.org", timeout=0)
        limiter.acquire("files.pythonhosted.org", timeout=0)
    snapshot = limiter.snapshot()
    total_grants = sum(child.acquired for child in snapshot.children.values())
    window_minutes = window_seconds / 60
    max_allowed = 2000 * window_minutes + 34  # + global burst slack
    assert total_grants <= max_allowed


def test_penalize_blocks_only_that_child():
    clock = FakeClock()
    limiter = _limiter(clock)
    limiter.penalize("files.pythonhosted.org", 60)
    assert limiter.acquire("files.pythonhosted.org", timeout=0) is False
    assert limiter.acquire("pypi.org", timeout=0) is True


def test_unknown_child_raises_key_error_on_acquire():
    clock = FakeClock()
    limiter = _limiter(clock)
    with pytest.raises(KeyError):
        limiter.acquire("example.com")


def test_unknown_child_raises_key_error_on_penalize():
    clock = FakeClock()
    limiter = _limiter(clock)
    with pytest.raises(KeyError):
        limiter.penalize("example.com", 60)


def test_snapshot_counters_reflect_grants_and_denials():
    clock = FakeClock()
    limiter = _limiter(clock, global_rate=60, children={"pypi.org": 60})
    assert limiter.acquire("pypi.org") is True
    assert limiter.acquire("pypi.org", timeout=0) is False
    snapshot = limiter.snapshot()
    assert snapshot.children["pypi.org"].acquired == 1
    assert snapshot.children["pypi.org"].denied == 1


def test_snapshot_reports_global_available_and_penalty_deadline():
    clock = FakeClock()
    limiter = _limiter(clock)
    limiter.penalize("pypi.org", 45)
    snapshot = limiter.snapshot()
    assert snapshot.global_available == pytest.approx(2000 / 60)
    assert snapshot.children["pypi.org"].penalty_deadline == 45


def test_acquire_blocks_then_succeeds_once_clock_advances():
    clock = AutoAdvanceClock(step=0.05)
    limiter = _limiter(clock, global_rate=60, children={"pypi.org": 60})
    assert limiter.acquire("pypi.org") is True
    assert limiter.acquire("pypi.org", timeout=5) is True


def test_limiter_acquire_with_no_timeout_blocks_then_succeeds():
    clock = AutoAdvanceClock(step=0.05)
    limiter = _limiter(clock, global_rate=60, children={"pypi.org": 60})
    assert limiter.acquire("pypi.org") is True
    assert limiter.acquire("pypi.org") is True


def test_acquire_returns_false_when_n_exceeds_burst_of_either_bucket():
    clock = FakeClock()
    limiter = _limiter(clock, global_rate=60, children={"pypi.org": 60})
    assert limiter.acquire("pypi.org", n=1_000_000, timeout=1) is False


def test_concurrency_smoke_real_threads_fake_clock_advanced_by_test():
    clock = FakeClock()
    limiter = _limiter(clock)
    thread_count = 8
    attempts_per_thread = 2000
    start = threading.Barrier(thread_count + 1)
    names = ["pypi.org", "files.pythonhosted.org"]

    def _worker(name: str) -> None:
        start.wait()
        for _ in range(attempts_per_thread):
            limiter.acquire(name, timeout=0)

    threads = [
        threading.Thread(target=_worker, args=(names[i % len(names)],)) for i in range(thread_count)
    ]
    for thread in threads:
        thread.start()
    start.wait()
    window_seconds = 120.0
    step = 0.01
    for _ in range(int(window_seconds / step)):
        clock.advance(step)
    for thread in threads:
        thread.join()

    snapshot = limiter.snapshot()
    total_grants = sum(child.acquired for child in snapshot.children.values())
    window_minutes = window_seconds / 60
    max_allowed = 2000 * window_minutes + 34
    assert total_grants <= max_allowed


def test_borrowing_succeeds_when_n_exceeds_child_burst_but_fits_global_burst():
    # A child's own reserve burst can be smaller than a requested `n` while
    # the global burst is not -- borrowing goes straight to the global
    # bucket in that case, bypassing the child bucket's own burst cap
    # entirely, so `_time_until_locked` must not report `None` (which would
    # mean "can never succeed") just because the child's own burst is too
    # small for `n`.
    clock = FakeClock()
    limiter = _limiter(clock, global_rate=6000, children={"pypi.org": 200})
    limiter._global.drain()
    assert limiter.acquire("pypi.org", n=10, timeout=0) is False
    clock.advance(1.0)  # global refills at 100 tokens/sec
    assert limiter.acquire("pypi.org", n=10, timeout=0) is True


def test_borrow_blocked_by_active_sibling_does_not_busy_loop_condition_wait(monkeypatch):
    # Regression test for a busy loop: a child whose own reserve is
    # exhausted, blocked from borrowing only by an active sibling that
    # hasn't gone idle yet, must compute a real wait that accounts for the
    # sibling-idle gate. It must never spin `Condition.wait` with a
    # near-zero timeout just because the global bucket looks available.
    clock = FakeClock()
    limiter = _limiter(
        clock, global_rate=2000, children={"pypi.org": 200, "files.pythonhosted.org": 1800}
    )

    # Mark pypi.org as recently active so files.pythonhosted.org's borrow
    # is gated by the sibling-idle check, not by token availability.
    assert limiter.acquire("pypi.org", timeout=0) is True

    # Drain files.pythonhosted.org's own reserve; the global bucket still
    # has spare tokens, which is exactly the scenario that used to make the
    # wait computation report an almost-zero wait.
    limiter._children["files.pythonhosted.org"].drain()

    wait_calls: list[float | None] = []

    def counting_wait(timeout: float | None = None) -> None:
        wait_calls.append(timeout)
        if len(wait_calls) > 500:
            # Escape hatch: fail fast on a regression rather than hang the
            # suite -- force the sibling gate open and let the assertion
            # below report the busy loop instead of a test timeout.
            clock.advance(1.0)
            return
        clock.advance(timeout or 0.0)

    monkeypatch.setattr(limiter._condition, "wait", counting_wait)

    assert limiter.acquire("files.pythonhosted.org", timeout=100) is True
    assert len(wait_calls) < 20
