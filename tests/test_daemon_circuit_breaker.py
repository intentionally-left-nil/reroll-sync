"""Tests for the per-dependency circuit breaker: closed/open/half-open
transitions, and the "PyPIRateLimited doesn't count" exemption.
"""

from __future__ import annotations

import pytest

from reroll_sync.daemon.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _Boom(Exception):
    pass


class _RateLimited(Exception):
    pass


def _breaker(**kwargs) -> tuple[CircuitBreaker, FakeClock]:
    clock = FakeClock()
    breaker = CircuitBreaker(
        failure_threshold=kwargs.pop("failure_threshold", 5),
        recovery_timeout=kwargs.pop("recovery_timeout", 60.0),
        now=clock.now,
        **kwargs,
    )
    return breaker, clock


def test_starts_closed_and_allows():
    breaker, _ = _breaker()
    assert breaker.state() == CircuitState.CLOSED
    assert breaker.allow()


def test_five_consecutive_failures_open_the_breaker():
    breaker, _ = _breaker()
    for _ in range(5):
        breaker.record_failure()
    assert breaker.state() == CircuitState.OPEN


def test_fewer_than_threshold_failures_stay_closed():
    breaker, _ = _breaker()
    for _ in range(4):
        breaker.record_failure()
    assert breaker.state() == CircuitState.CLOSED
    assert breaker.allow()


def test_open_breaker_rejects_without_calling_dependency():
    breaker, _ = _breaker()
    calls = []

    def _dependency():
        calls.append(1)
        raise _Boom()

    for _ in range(5):
        with pytest.raises(_Boom):
            breaker.call(_dependency)
    assert breaker.state() == CircuitState.OPEN
    assert len(calls) == 5

    with pytest.raises(CircuitBreakerOpenError):
        breaker.call(_dependency)
    assert len(calls) == 5  # not called again


def test_allow_returns_false_when_open():
    breaker, _ = _breaker()
    for _ in range(5):
        breaker.record_failure()
    assert breaker.allow() is False


def test_after_recovery_timeout_breaker_allows_exactly_one_trial():
    breaker, clock = _breaker(recovery_timeout=60.0)
    for _ in range(5):
        breaker.record_failure()
    clock.advance(60.0)
    assert breaker.allow() is True
    assert breaker.state() == CircuitState.HALF_OPEN
    # A second concurrent trial is refused while the first is outstanding.
    assert breaker.allow() is False


def test_breaker_stays_open_before_recovery_timeout_elapses():
    breaker, clock = _breaker(recovery_timeout=60.0)
    for _ in range(5):
        breaker.record_failure()
    clock.advance(59.99)
    assert breaker.allow() is False
    assert breaker.state() == CircuitState.OPEN


def test_successful_trial_closes_the_breaker():
    breaker, clock = _breaker(recovery_timeout=60.0)
    for _ in range(5):
        breaker.record_failure()
    clock.advance(60.0)
    assert breaker.allow() is True
    breaker.record_success()
    assert breaker.state() == CircuitState.CLOSED
    assert breaker.allow() is True


def test_failed_trial_reopens_the_breaker():
    breaker, clock = _breaker(recovery_timeout=60.0)
    for _ in range(5):
        breaker.record_failure()
    clock.advance(60.0)
    assert breaker.allow() is True
    breaker.record_failure()
    assert breaker.state() == CircuitState.OPEN
    # The reopened breaker's recovery timer restarts from the trial's failure.
    clock.advance(59.99)
    assert breaker.allow() is False
    clock.advance(0.01)
    assert breaker.allow() is True


def test_success_before_threshold_resets_the_failure_count():
    breaker, _ = _breaker(failure_threshold=5)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_failure()
    # Only 4 failures since the reset -- one short of the threshold.
    assert breaker.state() == CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state() == CircuitState.OPEN


def test_call_records_success_on_a_clean_call():
    breaker, _ = _breaker()
    result = breaker.call(lambda: "ok")
    assert result == "ok"
    assert breaker.state() == CircuitState.CLOSED


def test_call_records_failure_on_a_raised_exception():
    breaker, _ = _breaker(failure_threshold=1)
    with pytest.raises(_Boom):
        breaker.call(lambda: (_ for _ in ()).throw(_Boom()))
    assert breaker.state() == CircuitState.OPEN


def test_exempt_exception_does_not_count_toward_the_threshold():
    """PyPIRateLimited (or any exempted exception) must not open the breaker:
    throttling is expected behaviour, not a dependency failure.
    """
    breaker, _ = _breaker(failure_threshold=5)
    for _ in range(10):
        with pytest.raises(_RateLimited):
            breaker.call(lambda: (_ for _ in ()).throw(_RateLimited()), exempt=(_RateLimited,))
    assert breaker.state() == CircuitState.CLOSED
    assert breaker.allow() is True


def test_exempt_exception_during_half_open_trial_does_not_reopen():
    breaker, clock = _breaker(recovery_timeout=60.0, failure_threshold=5)
    for _ in range(5):
        breaker.record_failure()
    clock.advance(60.0)
    with pytest.raises(_RateLimited):
        breaker.call(lambda: (_ for _ in ()).throw(_RateLimited()), exempt=(_RateLimited,))
    # Exempted during the trial: state doesn't advance either way, and the
    # trial slot is released so a fresh one is allowed once called again.
    assert breaker.state() == CircuitState.HALF_OPEN
    assert breaker.allow() is True


def test_call_raises_circuit_breaker_open_error_without_invoking_fn():
    breaker, _ = _breaker(failure_threshold=1)
    with pytest.raises(_Boom):
        breaker.call(lambda: (_ for _ in ()).throw(_Boom()))
    calls = []
    with pytest.raises(CircuitBreakerOpenError):
        breaker.call(lambda: calls.append(1))
    assert calls == []
