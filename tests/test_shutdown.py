"""Tests for the shutdown contract: one exception type for every cross-thread
boundary abandoned mid-shutdown, plus the task-root runner that treats it as
a clean exit.
"""

from __future__ import annotations

import logging
import threading

import pytest

from reroll_sync.shutdown import ShutdownError, run_task, throw_if_shutting_down


def test_throw_if_shutting_down_is_a_noop_while_unset():
    event = threading.Event()
    throw_if_shutting_down(event, "a task")  # returns normally, no raise


def test_throw_if_shutting_down_raises_once_set():
    event = threading.Event()
    event.set()
    with pytest.raises(ShutdownError, match="a task"):
        throw_if_shutting_down(event, "a task")


def test_run_task_swallows_shutdown_error_as_a_clean_exit(caplog):
    def _fn() -> None:
        raise ShutdownError("writer went away")

    logger = logging.getLogger("test.task")
    with caplog.at_level(logging.INFO, logger="test.task"):
        run_task("a task", _fn, logger=logger)

    assert "a task" in caplog.text
    assert "exiting cleanly" in caplog.text


def test_run_task_propagates_any_other_exception():
    def _fn() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        run_task("a task", _fn, logger=logging.getLogger("test.task"))
