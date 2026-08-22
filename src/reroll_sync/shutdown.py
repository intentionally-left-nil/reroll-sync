"""The daemon's crash-with-cleanup shutdown contract.

``ShutdownError`` is the one exception every cross-thread boundary raises
once its far end has gone away for the rest of this run (shutdown, or the
writer's own fatal stop -- either way no further progress is possible).
Work abandoned this way stays durable and re-claimable: an outcome that
was never applied never happened, and an unsealed segment is truncated by
the next startup's recovery. Task roots therefore treat ``ShutdownError``
as a clean exit, never a crash; cleanup that must always run lives in
``finally`` blocks, not ``except ShutdownError`` handlers.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable


class ShutdownError(RuntimeError):
    """A cross-thread handoff failed because its far end is gone for the rest of this run."""


def throw_if_shutting_down(shutdown_event: threading.Event, what: str) -> None:
    """Raise ``ShutdownError`` if shutdown has been signaled.

    Advisory only: the authoritative signal is the boundary primitive
    itself raising, since the far end can stop between this check and the
    handoff it precedes. Use to abandon a long-running loop early, not to
    guard an individual handoff.
    """
    if shutdown_event.is_set():
        raise ShutdownError(f"{what}: shutdown in progress")


def run_task(name: str, fn: Callable[[], None], *, logger: logging.Logger) -> None:
    """Run one background task's whole body, treating ``ShutdownError`` as a clean exit.

    Any other exception propagates: crash policy (log-and-reraise vs. an
    unlogged thread death) is each task's own decision.
    """
    try:
        fn()
    except ShutdownError:
        logger.info("task %r: shutdown mid-operation; exiting cleanly", name)
