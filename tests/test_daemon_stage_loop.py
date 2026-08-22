"""Tests for the generic stage-loop supervisor: triggers, pause/resume,
exception handling, and shutdown -- exercised with fake stages, not real
network/database work. No test in this module sleeps.
"""

from __future__ import annotations

import logging
import threading

import pytest

from reroll_sync.daemon.stage_loop import IntervalTrigger, PollTrigger, StageLoop
from reroll_sync.shutdown import ShutdownError


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


# ---------------------------------------------------------------------------
# IntervalTrigger
# ---------------------------------------------------------------------------


def test_interval_trigger_is_due_immediately_before_first_run():
    clock = FakeClock()
    trigger = IntervalTrigger(10.0, now=clock.now)
    assert trigger.due()


def test_interval_trigger_not_due_again_until_interval_elapses():
    clock = FakeClock()
    trigger = IntervalTrigger(10.0, now=clock.now)
    trigger.mark_run(did_work=True)
    assert not trigger.due()
    clock.advance(9.99)
    assert not trigger.due()
    clock.advance(0.01)
    assert trigger.due()


def test_interval_trigger_wait_seconds_reflects_remaining_time():
    clock = FakeClock()
    trigger = IntervalTrigger(10.0, now=clock.now)
    trigger.mark_run(did_work=True)
    assert trigger.wait_seconds() == pytest.approx(10.0)
    clock.advance(4.0)
    assert trigger.wait_seconds() == pytest.approx(6.0)


def test_interval_trigger_wait_seconds_never_negative():
    clock = FakeClock()
    trigger = IntervalTrigger(10.0, now=clock.now)
    trigger.mark_run(did_work=True)
    clock.advance(50.0)
    assert trigger.wait_seconds() == 0.0


def test_interval_trigger_wait_seconds_zero_before_first_run():
    clock = FakeClock()
    trigger = IntervalTrigger(10.0, now=clock.now)
    assert trigger.wait_seconds() == 0.0


# ---------------------------------------------------------------------------
# PollTrigger
# ---------------------------------------------------------------------------


def test_poll_trigger_always_due():
    trigger = PollTrigger(idle_interval=5.0)
    assert trigger.due()
    trigger.mark_run(did_work=False)
    assert trigger.due()


def test_poll_trigger_waits_when_idle():
    trigger = PollTrigger(idle_interval=5.0)
    trigger.mark_run(did_work=False)
    assert trigger.wait_seconds() == 5.0


def test_poll_trigger_does_not_wait_when_busy():
    trigger = PollTrigger(idle_interval=5.0)
    trigger.mark_run(did_work=True)
    assert trigger.wait_seconds() == 0.0


# ---------------------------------------------------------------------------
# StageLoop.run_once
# ---------------------------------------------------------------------------


def _loop(iterate, *, fatal_exceptions=(), trigger=None, now=None) -> StageLoop:
    clock = FakeClock()
    resolved_now = now if now is not None else clock.now
    return StageLoop(
        "fake_stage",
        iterate,
        trigger if trigger is not None else PollTrigger(idle_interval=1.0),
        threading.Event(),
        now=resolved_now,
        fatal_exceptions=fatal_exceptions,
    )


def test_run_once_calls_iterate_and_reports_work_done():
    calls = []
    loop = _loop(lambda: (calls.append(1), True)[1])
    did_work = loop.run_once()
    assert did_work is True
    assert calls == [1]
    stats = loop.stats()
    assert stats.last_run_at is not None
    assert stats.last_success_at is not None
    assert stats.consecutive_failures == 0


def test_run_once_reports_no_work_done():
    loop = _loop(lambda: False)
    assert loop.run_once() is False


def test_paused_stage_does_not_call_iterate():
    calls = []
    loop = _loop(lambda: (calls.append(1), True)[1])
    loop.pause()
    did_work = loop.run_once()
    assert did_work is False
    assert calls == []
    assert loop.is_paused()


def test_resume_restores_claiming():
    calls = []
    loop = _loop(lambda: (calls.append(1), True)[1])
    loop.pause()
    loop.run_once()
    loop.resume()
    loop.run_once()
    assert calls == [1]
    assert not loop.is_paused()


def test_exception_in_iteration_is_logged_and_loop_continues(caplog):
    attempts = []

    def _iterate():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("boom")
        return True

    loop = _loop(_iterate)
    with caplog.at_level("ERROR"):
        did_work = loop.run_once()
    assert did_work is False
    assert loop.stats().consecutive_failures == 1
    assert any(
        "boom" in record.message or "unhandled exception" in record.message
        for record in caplog.records
    )

    did_work = loop.run_once()
    assert did_work is True
    assert loop.stats().consecutive_failures == 0


def test_consecutive_failures_accumulate_across_repeated_exceptions():
    loop = _loop(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    loop.run_once()
    loop.run_once()
    loop.run_once()
    assert loop.stats().consecutive_failures == 3


def test_fatal_exception_propagates_instead_of_being_caught():
    class ProgrammingError(Exception):
        pass

    loop = _loop(
        lambda: (_ for _ in ()).throw(ProgrammingError("bug")),
        fatal_exceptions=(ProgrammingError,),
    )
    with pytest.raises(ProgrammingError):
        loop.run_once()


def test_non_fatal_exception_type_not_in_whitelist_is_still_caught():
    class ProgrammingError(Exception):
        pass

    loop = _loop(
        lambda: (_ for _ in ()).throw(RuntimeError("transient")),
        fatal_exceptions=(ProgrammingError,),
    )
    did_work = loop.run_once()
    assert did_work is False
    assert loop.stats().consecutive_failures == 1


def test_run_once_propagates_shutdown_error_without_counting_a_failure():
    """A `ShutdownError` out of `iterate` is a cross-thread boundary going
    away mid-shutdown, not a stage failure: it propagates like a fatal
    exception, but with no failure accounting (the stage is fine; the
    process is ending)."""
    loop = _loop(lambda: (_ for _ in ()).throw(ShutdownError("writer went away")))
    with pytest.raises(ShutdownError):
        loop.run_once()
    assert loop.stats().consecutive_failures == 0


def test_run_once_raises_shutdown_error_without_calling_iterate_once_shutdown_set():
    shutdown_event = threading.Event()
    shutdown_event.set()
    calls = []
    loop = StageLoop(
        "fake_stage",
        lambda: (calls.append(1), True)[1],
        PollTrigger(idle_interval=1.0),
        shutdown_event,
    )
    with pytest.raises(ShutdownError):
        loop.run_once()
    assert calls == []


# ---------------------------------------------------------------------------
# StageLoop.run_forever
# ---------------------------------------------------------------------------


def test_run_forever_stops_promptly_once_shutdown_event_is_set():
    """Even with a large interval trigger, setting the shutdown event from
    inside `iterate` must stop `run_forever` without an actual sleep: once
    an `Event` is set, `.wait(timeout=...)` returns immediately regardless
    of the timeout value.
    """
    clock = FakeClock()
    shutdown_event = threading.Event()
    calls = []

    def _iterate():
        calls.append(1)
        shutdown_event.set()
        return True

    loop = StageLoop(
        "fake_stage",
        _iterate,
        IntervalTrigger(1000.0, now=clock.now),
        shutdown_event,
        now=clock.now,
    )
    loop.run_forever()
    assert calls == [1]


def test_run_forever_never_runs_if_shutdown_already_set():
    shutdown_event = threading.Event()
    shutdown_event.set()
    calls = []
    loop = StageLoop(
        "fake_stage",
        lambda: (calls.append(1), True)[1],
        PollTrigger(idle_interval=0.0),
        shutdown_event,
    )
    loop.run_forever()
    assert calls == []


def test_run_forever_exits_cleanly_when_iterate_raises_shutdown_error(caplog):
    """The shutdown event is never even set here: the exit is driven purely
    by the boundary exception, proving `run_forever` treats `ShutdownError`
    as a clean exit (no error log, no failure count) rather than a crash or
    a retryable failure."""
    shutdown_event = threading.Event()

    def _iterate():
        raise ShutdownError("writer went away mid-batch")

    loop = StageLoop("fake_stage", _iterate, PollTrigger(idle_interval=0.0), shutdown_event)
    with caplog.at_level(logging.INFO):
        loop.run_forever()
    assert loop.stats().consecutive_failures == 0
    assert "unhandled exception" not in caplog.text
    assert "exiting cleanly" in caplog.text


def test_run_forever_runs_repeatedly_while_trigger_stays_due():
    shutdown_event = threading.Event()
    calls = []

    def _iterate():
        calls.append(1)
        if len(calls) >= 3:
            shutdown_event.set()
        return True

    loop = StageLoop(
        "fake_stage",
        _iterate,
        PollTrigger(idle_interval=0.0),
        shutdown_event,
    )
    loop.run_forever()
    assert len(calls) == 3


class _NeverDueTrigger:
    """A `Trigger` that is never due; signals shutdown from `wait_seconds`
    so `run_forever` exits after checking `due` exactly once, without an
    actual sleep."""

    def __init__(self, shutdown_event: threading.Event) -> None:
        self._shutdown_event = shutdown_event

    def due(self) -> bool:
        return False

    def wait_seconds(self) -> float:
        self._shutdown_event.set()
        return 0.0

    def mark_run(self, did_work: bool) -> None:
        raise AssertionError("mark_run must not be called when due() is False")


def test_run_forever_skips_run_once_when_trigger_not_due():
    shutdown_event = threading.Event()
    calls = []
    loop = StageLoop(
        "fake_stage",
        lambda: (calls.append(1), True)[1],
        _NeverDueTrigger(shutdown_event),
        shutdown_event,
    )
    loop.run_forever()
    assert calls == []
