"""The long-lived daemon process: startup, shutdown, and the glue that
wires specs 01-09's modules into the stage loops from `stage_loop.py` and
`stages/`, plus the control socket from `control.py`.

**Archive-recovery integration** (spec 09's `ArchiveStore`/`fetch.py` meet
spec 10's startup sequence): `ArchiveStore.__init__` itself truncates any
stale `.open` segment file left by a crashed previous run, but reports
nothing about *which* segment id(s) it truncated -- callers must reset the
wheels whose blobs lived there via `fetch.recover_unsealed_segment`, which
takes an explicit `segment_id`. This module determines that id by listing
`*.open` files in `segments_dir` *before* constructing `ArchiveStore`,
then, once it's constructed, taking the difference against
`sealed_segment_ids()` (a truncated `.open` file is never sealed, so this
recovers exactly the ones that got truncated and nothing else).

**Startup-sequence deviation**: spec 10 lists "open the writer; recover
`change_seq`" and "start the writer thread" as two separate steps (3 and
5), with archive recovery in between. `writer.Writer.start()` does both in
one call -- there is no API to recover `change_seq` without also spawning
the background thread -- and archive recovery's `WriteOp`s need that
thread already running (`submit_and_wait` blocks on it). So this module
collapses steps 3 and 5 into one `Writer.start()` call before archive
recovery, which still satisfies the ordering that actually matters: no
stage claims work before recovery completes.

**Threading and sqlite connections**: every stage object is built eagerly
on the main thread during `start()`, then handed to a dedicated
`StageLoop` thread. A sqlite3 connection is normally usable only from the
thread that created it (`check_same_thread` defaults to `True`), which
would break that pattern, so every read-only connection this module opens
uses `check_same_thread=False` instead of `db.connect_reader` -- safe here
because each one is still only ever touched by one thread at a time in
practice (its owning stage's thread), sqlite serializes writers via its
own locking, and WAL readers never block each other.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import signal
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Protocol, cast

from .. import db, health, metrics
from ..archive.location import BlobLocation
from ..archive.store import ArchiveStore
from ..control import ControlHandlers, ControlServer
from ..convert import worker_init
from ..dispatcher import Dispatcher, Selector, Stage
from ..fetch import ArchiveHandoff, BulkConvertSource, ByteBudgetedQueue, recover_unsealed_segment
from ..ingest import ProjectBackoff
from ..metrics_server import MetricsServer
from ..pypi_client import PyPIClient
from ..ratelimit import HierarchicalLimiter
from ..version import REROLL_VERSION
from ..writer import Writer
from .circuit_breaker import CircuitBreaker
from .config import Config
from .disk_guard import DiskGuard, DiskGuardLike, DiskGuardPausedError
from .logging_setup import configure_logging
from .stage_loop import IntervalTrigger, PollTrigger, StageLoop
from .stages.convert import ConvertStage
from .stages.fetch import FetchStage
from .stages.gc import GcStage
from .stages.index_poll import IndexPollStage
from .stages.project_sync import ProjectSyncStage
from .unquarantine import unquarantine as run_unquarantine_campaign

logger = logging.getLogger("reroll_sync.daemon")

SHUTDOWN_GRACE_SECONDS = 30.0
GC_INTERVAL_SECONDS = 86400.0
IDLE_POLL_SECONDS = 1.0
DISK_GUARD_INTERVAL_SECONDS = 30.0
"""How often the `disk_guard` stage's own timer re-checks free space on
`segments_dir`, independent of any other stage's activity. Spec 10 asks
for "before each segment append, and on a timer"; 30s is frequent enough
to notice a fill-up quickly without making free-space checks (a `stat`
syscall) a measurable cost.
"""
ARCHIVE_POLL_SECONDS = 0.05
"""Bounded poll granularity for the archive thread's dispatch loop: how
often it rechecks `disk_guard.is_paused()` and the handoff queue's length
before deciding whether to call `ArchiveHandoff.process_one` (which itself
blocks on the queue). Short enough to add negligible latency to normal
archive throughput (dwarfed by the network fetch latency this pipeline is
actually bottlenecked on); long enough not to spin. A bounded wait via
`shutdown_event.wait(timeout=...)`, not a plain sleep: an already-set
shutdown event still returns immediately.
"""
PYPI_ORG = "pypi.org"
FILES_PYTHONHOSTED_ORG = "files.pythonhosted.org"
LOCAL_DISK = "local_disk"

PROGRAMMING_ERROR_EXCEPTIONS: tuple[type[BaseException], ...] = (
    TypeError,
    AttributeError,
    NameError,
    KeyError,
)
"""`StageLoop`'s `fatal_exceptions` for every real stage: a narrow set of
exception types that only a coding mistake -- a stage's own code indexing
or attribute-accessing something that should not be missing -- would ever
raise, never an ordinary operational condition. Deliberately excludes:

- `PyPITransientError`/`PyPIProtocolError`/`PyPIRateLimited`/`PyPINotFound`/
  `MetadataHashMismatch`: every real stage already turns these into an
  `Outcome` or a caught-and-logged branch before they could reach
  `StageLoop`'s exception handler at all (see `fetch.fetch_one`,
  `stages.index_poll.IndexPollStage.iterate`, etc.) -- ordinary,
  expected-to-happen PyPI conditions, not bugs.
- Generic `OSError`/`sqlite3.OperationalError`: a locked db or a network
  blip is a transient operational condition the loop should keep retrying
  through, not crash on.
- `db.SchemaMismatchError`/`SchemaVersionError`/`AutoVacuumError`: all
  three are raised only from `db.init_db`, which runs once during
  `Daemon.start()` before any `StageLoop` exists -- unreachable from a
  running stage's `iterate()`, so there is nothing for a stage loop's
  `fatal_exceptions` to do with them.
"""


class Daemon:
    """Owns every connection, stage loop, and the control socket for one run.

    Construction does no I/O. Call `start()` to run the startup sequence
    and `shutdown()` to run the shutdown sequence (directly, or via
    `install_signal_handlers`/`run_forever` for a real process).
    """

    def __init__(
        self,
        config: Config,
        *,
        now: Callable[[], float] = time.time,
        pypi_client: PyPIClient | None = None,
    ) -> None:
        self.config = config
        self._now = now
        self._injected_pypi_client = pypi_client
        self.shutdown_event = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._shutting_down = False
        self._stopped_event = threading.Event()
        self._started_event = threading.Event()
        """Set once `start()` has fully completed (stages started, control
        socket up). A test synchronizes on this instead of polling
        `control_server is not None` in a sleep loop.
        """

        self.stage_loops: dict[str, StageLoop] = {}
        self.stage_start_errors: dict[str, str] = {}
        self._stage_threads: dict[str, threading.Thread] = {}
        self.archive_thread: threading.Thread | None = None
        self.recovered_segment_ids: list[int] = []
        self.control_server: ControlServer | None = None
        self._project_sync_stage: ProjectSyncStage | None = None
        self._index_poll_stage: IndexPollStage | None = None
        self._convert_stage: ConvertStage | None = None
        self._fetch_stage: FetchStage | None = None
        self._owned_conns: list[sqlite3.Connection] = []
        self.metrics_server: MetricsServer | None = None
        self._on_archive_disk_pause: Callable[[], None] | None = None
        """Test-only hook: called every time the archive thread notices
        `disk_guard` is paused and is about to wait instead of popping the
        next handoff item. `None` in production; a test wires this to an
        `Event.set` to synchronize deterministically instead of polling.
        """
        self._on_archive_item_processed: Callable[[], None] | None = None
        """Test-only hook: called every time the archive thread finishes a
        `process_one()` call (an item was actually archived and handed to
        convert). `None` in production; a test wires this to an
        `Event.set` to synchronize deterministically instead of polling.
        """

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Run the startup sequence. Raises on a schema/version mismatch."""
        configure_logging()
        db.init_db(self.config.db_path)

        self.writer_conn = db.connect_writer(self.config.db_path)
        self.writer = Writer(
            self.writer_conn,
            batch_size=self.config.batch_size,
            batch_interval=self.config.batch_interval,
            checkpoint_interval=self.config.checkpoint_interval,
            vacuum_interval=self.config.vacuum_interval,
        )
        self.writer.start()

        self._recover_archive()

        self.limiter = HierarchicalLimiter(self.config.global_rate, self.config.domain_reserves)
        self.breakers = {
            PYPI_ORG: CircuitBreaker(now=self._now),
            FILES_PYTHONHOSTED_ORG: CircuitBreaker(now=self._now),
            LOCAL_DISK: CircuitBreaker(now=self._now),
        }
        self.disk_guard = DiskGuard(
            Path(self.config.segments_dir), self.config.disk_free_floor_bytes
        )
        self.pypi_client = (
            self._injected_pypi_client
            if self._injected_pypi_client is not None
            else PyPIClient(self.limiter, user_agent=self.config.user_agent)
        )
        self.reader_conn = self._new_reader_conn()
        self.dispatcher = Dispatcher(
            self.reader_conn,
            self.writer,
            reroll_version=REROLL_VERSION,
            limiter=self.limiter,
            max_attempts=self.config.max_attempts,
        )
        self.handoff_queue = ByteBudgetedQueue(budget_bytes=self.config.handoff_budget_bytes)
        self.project_backoff = ProjectBackoff(now=self._now, max_attempts=self.config.max_attempts)

        self._start_control_server()
        self._start_stages()
        self._start_metrics_server()
        self._started_event.set()

    def _recover_archive(self) -> None:
        """Truncate any unsealed `.open` segment and reset the wheels it held.

        Must run before any stage starts claiming work; see this module's
        docstring for how the affected segment id(s) are determined.
        """
        segments_dir = Path(self.config.segments_dir)
        stale_before = _open_segment_ids(segments_dir)
        self.archive_conn = db.connect_writer(self.config.db_path)
        self.archive_store = ArchiveStore(segments_dir, self.archive_conn)
        sealed = set(self.archive_store.sealed_segment_ids())
        self.recovered_segment_ids = sorted(stale_before - sealed)
        for segment_id in self.recovered_segment_ids:
            recover_unsealed_segment(self.archive_store, self.writer, segment_id)

    def _start_metrics_server(self) -> None:
        """Start the localhost-only `/metrics` endpoint, if configured.

        Disabled (no server, `self.metrics_server` stays `None`) unless
        `Config.metrics_port` is set -- most callers (tests, offline
        tools) have no use for a listening socket at all.
        """
        if self.config.metrics_port is None:
            return
        server = MetricsServer(self.config.metrics_port, self._render_metrics)
        server.start()
        self.metrics_server = server

    def _stage_inputs(self) -> dict[str, health.StageInput]:
        """Build `health.snapshot()`'s `stages` argument from live daemon state."""
        queue_by_stage = {"fetch": Stage.FETCH, "convert": Stage.CONVERT}
        inputs: dict[str, health.StageInput] = {}
        for name, loop in self.stage_loops.items():
            queue_metrics = None
            if name in queue_by_stage:
                queue_metrics = self.dispatcher.metrics(queue_by_stage[name])
            remote_last_serial = None
            last_change_at = None
            if name == "index_poll" and self._index_poll_stage is not None:
                poll_snapshot = self._index_poll_stage.snapshot()
                remote_last_serial = poll_snapshot.last_remote_serial
                last_change_at = poll_snapshot.last_change_at
            inputs[name] = health.StageInput(
                loop=loop.stats(),
                queue=queue_metrics,
                remote_last_serial=remote_last_serial,
                last_change_at=last_change_at,
            )
        return inputs

    def _render_metrics(self) -> str:
        """Render a fresh `health.snapshot()` as Prometheus text. Called per scrape."""
        snapshot = health.snapshot(
            self.reader_conn,
            self.writer,
            self.limiter,
            self.breakers,
            self._stage_inputs(),
            archive_store=self.archive_store,
            now=self._now,
        )
        return metrics.render_metrics(snapshot)

    def _start_control_server(self) -> None:
        """Start the control socket before any stage, so `status` answers regardless."""
        handlers = ControlHandlers(
            status=self.status,
            pause=self.pause_stage,
            resume=self.resume_stage,
            drain=self.drain,
            reprocess=self.reprocess,
            unquarantine=self.unquarantine,
            shutdown=self.request_shutdown,
        )
        control_server = ControlServer(self.config.socket_path, handlers)
        control_server.start()
        self.control_server = control_server

    def _start_stages(self) -> None:
        for name, builder in (
            ("project_sync", self._build_project_sync),
            ("index_poll", self._build_index_poll),
            ("convert", self._build_convert),
            ("fetch", self._build_fetch),
            ("gc", self._build_gc),
            ("disk_guard", self._build_disk_guard),
        ):
            self._start_one_stage(name, builder)
        # The archive thread only has anything to do once fetch's build
        # actually created it (fetch's build is what wires ArchiveHandoff).
        if "fetch" in self.stage_loops:
            self.archive_thread = threading.Thread(
                target=self._run_archive_thread, name="reroll-sync-archive", daemon=True
            )
            self.archive_thread.start()
        else:
            self.archive_thread = None

    def _start_one_stage(self, name: str, builder: Callable[[], StageLoop]) -> None:
        try:
            loop = builder()
        except Exception as exc:
            logger.error("stage %r failed to start: %s", name, exc, exc_info=True)
            self.stage_start_errors[name] = str(exc)
            return
        self.stage_loops[name] = loop
        thread = threading.Thread(target=loop.run_forever, name=f"reroll-sync-{name}", daemon=True)
        self._stage_threads[name] = thread
        thread.start()

    def _build_project_sync(self) -> StageLoop:
        db_path = self.config.db_path
        stage = ProjectSyncStage(
            self.pypi_client,
            lambda: sqlite3.connect(str(db_path)),
            self.writer,
            self.limiter,
            self.breakers[PYPI_ORG],
            max_workers=self.config.project_workers,
            backoff=self.project_backoff,
            now=self._now,
        )
        self._project_sync_stage = stage
        return StageLoop(
            "project_sync",
            stage.iterate,
            PollTrigger(IDLE_POLL_SECONDS),
            self.shutdown_event,
            now=self._now,
            fatal_exceptions=PROGRAMMING_ERROR_EXCEPTIONS,
        )

    def _build_index_poll(self) -> StageLoop:
        if self._project_sync_stage is None:
            raise RuntimeError("project_sync stage did not start; index_poll has nothing to feed")
        conn = self._new_reader_conn()
        stage = IndexPollStage(
            self.pypi_client,
            conn,
            self.breakers[PYPI_ORG],
            enqueue=self._project_sync_stage.enqueue,
            now=self._now,
        )
        self._index_poll_stage = stage
        return StageLoop(
            "index_poll",
            stage.iterate,
            IntervalTrigger(self.config.index_poll_interval, now=self._now),
            self.shutdown_event,
            now=self._now,
            fatal_exceptions=PROGRAMMING_ERROR_EXCEPTIONS,
        )

    def _build_convert(self) -> StageLoop:
        conn = self._new_reader_conn()
        bulk_source = BulkConvertSource(self.archive_store, self.dispatcher, conn)
        self.convert_pool = ProcessPoolExecutor(
            max_workers=self.config.convert_workers,
            initializer=worker_init,
            initargs=(REROLL_VERSION,),
        )
        stage = ConvertStage(
            self.dispatcher, bulk_source, self.convert_pool, reroll_version=REROLL_VERSION
        )
        self._convert_stage = stage
        return StageLoop(
            "convert",
            stage.iterate,
            PollTrigger(IDLE_POLL_SECONDS),
            self.shutdown_event,
            now=self._now,
            fatal_exceptions=PROGRAMMING_ERROR_EXCEPTIONS,
        )

    def _build_fetch(self) -> StageLoop:
        if self._convert_stage is None:
            raise RuntimeError("convert stage did not start; fetch has nowhere to hand off to")
        conn = self._new_reader_conn()
        self.fetch_pool = ThreadPoolExecutor(max_workers=self.config.fetch_workers)
        guarded_store = _DiskBreakerGuardedStore(
            self.archive_store,
            self.breakers[LOCAL_DISK],
            disk_guard=self.disk_guard,
            shutdown_event=self.shutdown_event,
        )
        self.archive_handoff = ArchiveHandoff(
            self.handoff_queue,
            # _DiskBreakerGuardedStore proxies every ArchiveStore attribute
            # via __getattr__ (see its docstring), so it's structurally
            # compatible even though it isn't nominally an ArchiveStore.
            cast(ArchiveStore, guarded_store),
            self.dispatcher,
            self.writer,
            self._convert_stage.on_archived,
        )
        stage = FetchStage(
            self.pypi_client,
            self.dispatcher,
            conn,
            self.handoff_queue,
            self.breakers[FILES_PYTHONHOSTED_ORG],
            pool=self.fetch_pool,
            disk_guard=self.disk_guard,
        )
        self._fetch_stage = stage
        return StageLoop(
            "fetch",
            stage.iterate,
            PollTrigger(IDLE_POLL_SECONDS),
            self.shutdown_event,
            now=self._now,
            fatal_exceptions=PROGRAMMING_ERROR_EXCEPTIONS,
        )

    def _build_gc(self) -> StageLoop:
        stage = GcStage(self.writer, now=self._now)
        return StageLoop(
            "gc",
            stage.iterate,
            IntervalTrigger(GC_INTERVAL_SECONDS, now=self._now),
            self.shutdown_event,
            now=self._now,
            fatal_exceptions=PROGRAMMING_ERROR_EXCEPTIONS,
        )

    def _build_disk_guard(self) -> StageLoop:
        """A small timer-driven stage whose only job is `disk_guard.check()`.

        Spec 10: "Before each segment append, and on a timer, check free
        space on `segments_dir`." The per-append check lives in
        `_DiskBreakerGuardedStore.add` (below) and `FetchStage.iterate`'s
        own `disk_guard.is_paused()` gate; this is the timer half.
        `disk_guard.check()` itself does the actual pause/resume logging
        (see `disk_guard.py`), so this stage's `iterate` has nothing else
        to report and always returns `False`.
        """

        def _iterate() -> bool:
            self.disk_guard.check()
            return False

        return StageLoop(
            "disk_guard",
            _iterate,
            IntervalTrigger(DISK_GUARD_INTERVAL_SECONDS, now=self._now),
            self.shutdown_event,
            now=self._now,
            fatal_exceptions=PROGRAMMING_ERROR_EXCEPTIONS,
        )

    def _run_archive_thread(self) -> None:
        """Drain the handoff queue via `ArchiveHandoff.process_one`, deferring
        (never popping) while `disk_guard` is paused.

        Reimplements `ArchiveHandoff.run`'s own `while process_one(): pass`
        loop with two additions -- a `disk_guard.is_paused()` check and a
        peek at the queue's length -- before each pop, rather than changing
        `run`/`ByteBudgetedQueue` themselves (spec 09's module): `get` has
        no non-blocking or timed variant, so this loop never calls
        `process_one` at all unless it has *first* confirmed (a)
        fetch/archive isn't paused and (b) an item is actually already
        queued -- otherwise `process_one` would block indefinitely inside
        `get`, immune to `disk_guard` flipping to paused for as long as
        that block lasts. Once (a) and (b) both hold, `process_one` always
        returns `True` (this thread is the queue's only consumer, so
        nothing else can have drained the item counted by the length check
        just above).

        `handoff_queue` closing -- fully drained, or (while paused) shutdown
        being signaled with no way to safely drain further -- is this
        loop's only exit condition; `_archive_thread_step`'s return value
        is what tracks it, not a separate check here, so a `shutdown_event`
        set while items are still queued (or still arriving from an
        in-flight fetch) never cuts a real drain short.
        """
        try:
            while self._archive_thread_step():
                pass
        except Exception:
            logger.critical("archive thread crashed", exc_info=True)
            raise

    def _archive_thread_step(self) -> bool:
        """Run one iteration of the archive thread's dispatch loop.

        Returns whether the loop should keep going. `False` means either
        `handoff_queue` is closed and fully drained, or `disk_guard` is
        paused and shutdown has been signaled -- draining further would
        mean writing to disk while paused, which never happens. Split out
        from `_run_archive_thread` so a test can drive exactly one
        iteration deterministically without racing the real background
        thread for it.
        """
        if self.disk_guard.is_paused():
            if self._on_archive_disk_pause is not None:
                self._on_archive_disk_pause()
            if self.shutdown_event.is_set():
                return False
            self.shutdown_event.wait(timeout=ARCHIVE_POLL_SECONDS)
            return True
        if len(self.handoff_queue) == 0:
            if self.handoff_queue._closed:
                return False
            self.shutdown_event.wait(timeout=ARCHIVE_POLL_SECONDS)
            return True
        self.archive_handoff.process_one()
        if self._on_archive_item_processed is not None:
            self._on_archive_item_processed()
        return True

    # ------------------------------------------------------------------
    # Control-command handlers
    # ------------------------------------------------------------------

    def status(self) -> dict[str, Any]:
        """A health snapshot: stage counters, breaker states, writer/WAL stats.

        A placeholder for spec 11's real health module -- everything
        available today, with an obvious spot (a new top-level key) for
        that module to extend without changing this method's existing keys.
        """
        return {
            "stages": {
                name: dataclasses.asdict(loop.stats()) for name, loop in self.stage_loops.items()
            },
            "stage_start_errors": dict(self.stage_start_errors),
            "circuit_breakers": {
                name: breaker.state().value for name, breaker in self.breakers.items()
            },
            "disk_guard": {"paused": self.disk_guard.is_paused()},
            "writer": {
                "wal_bytes": self.writer.wal_bytes(),
                "freelist_count": self.writer.freelist_count(),
                "failed_ops": self.writer.failed_ops(),
                "last_truncate_checkpoint_at": self.writer.last_truncate_checkpoint_at(),
                "last_truncate_checkpoint_ok": self.writer.last_truncate_checkpoint_ok(),
                "consecutive_checkpoint_failures": self.writer.consecutive_checkpoint_failures(),
            },
            "dispatcher": {
                "fetch": dataclasses.asdict(self.dispatcher.metrics(Stage.FETCH)),
                "convert": dataclasses.asdict(self.dispatcher.metrics(Stage.CONVERT)),
            },
        }

    def pause_stage(self, name: str) -> None:
        self._require_stage(name).pause()

    def resume_stage(self, name: str) -> None:
        self._require_stage(name).resume()

    def drain(self) -> None:
        """Pause every stage's claiming; in-flight work finishes on its own."""
        for loop in self.stage_loops.values():
            loop.pause()

    def reprocess(self, selector: Selector) -> int:
        return self.dispatcher.reprocess(selector)

    def unquarantine(self, selector: Selector) -> int:
        return run_unquarantine_campaign(self.reader_conn, self.writer, selector)

    def request_shutdown(self) -> None:
        """Trigger `shutdown` on a background thread; returns immediately.

        Called from the control socket's own request-handling thread pool:
        running the full sequence (which stops that same pool) synchronously
        here would deadlock the pool waiting on itself.
        """
        threading.Thread(target=self.shutdown, name="reroll-sync-shutdown", daemon=True).start()

    def _new_reader_conn(self) -> sqlite3.Connection:
        conn = _shared_reader_conn(self.config.db_path)
        self._owned_conns.append(conn)
        return conn

    def _require_stage(self, name: str) -> StageLoop:
        loop = self.stage_loops.get(name)
        if loop is None:
            raise ValueError(f"unknown stage {name!r}; valid stages: {sorted(self.stage_loops)}")
        return loop

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def install_signal_handlers(self) -> None:
        """Install `SIGTERM`/`SIGINT` handlers: first signal shuts down gracefully,
        a second (received while already shutting down) exits immediately.
        """

        def _handle(signum: int, _frame: object) -> None:
            if self._shutting_down:
                logger.critical("second signal %d during shutdown; exiting immediately", signum)
                os._exit(1)
            logger.info("received signal %d; shutting down", signum)
            threading.Thread(target=self.shutdown, name="reroll-sync-shutdown", daemon=True).start()

        signal.signal(signal.SIGTERM, _handle)
        signal.signal(signal.SIGINT, _handle)

    def run_forever(self) -> None:
        """Start the daemon and block the calling thread until shutdown completes.

        Does not install signal handlers itself -- `signal.signal` only
        works from the main thread, and `run_forever` may reasonably be
        called from another one (e.g. under test). Call
        `install_signal_handlers` separately from the main thread first.
        """
        self.start()
        self._stopped_event.wait()

    def shutdown(self) -> None:
        """Stop accepting new work, drain in-flight work, then stop the writer.

        Idempotent: a second call while one is already running returns
        immediately without re-entering the sequence.
        """
        with self._shutdown_lock:
            if self._shutting_down:
                return
            self._shutting_down = True

        if self.control_server is not None:
            self.control_server.restrict_to_status_only()
        self.shutdown_event.set()
        deadline = self._now() + SHUTDOWN_GRACE_SECONDS
        # `fetch` is the only stage that ever calls `handoff_queue.put`, so its
        # thread must be joined -- meaning it's done submitting, not just told
        # to stop claiming -- before the queue closes underneath an in-flight
        # `put`, which would otherwise raise `QueueClosed` and drop an
        # already-downloaded batch on the floor.
        self._join_named_stage_threads(("fetch",), deadline=deadline)
        self.handoff_queue.close()  # lets the archive thread's process_one loop exit
        self._join_named_stage_threads(
            (name for name in self._stage_threads if name != "fetch"), deadline=deadline
        )
        if self.archive_thread is not None:
            self.archive_thread.join(timeout=max(0.0, deadline - self._now()))

        # Deliberately not sealing the archive's open segment: sealing
        # under time pressure risks a partial footer, and spec 09's own
        # startup recovery already handles a `.open` file left behind.
        self.writer.stop(drain=True)

        if self.control_server is not None:
            self.control_server.stop()
        if self.metrics_server is not None:
            self.metrics_server.stop()
        self.pypi_client.close()
        if hasattr(self, "fetch_pool"):
            self.fetch_pool.shutdown(wait=False)
        if hasattr(self, "convert_pool"):
            self.convert_pool.shutdown(wait=False)
        current_writer = self.archive_store._current_writer
        if current_writer is not None and not current_writer._file.closed:
            current_writer._file.close()
        for conn in self._owned_conns:
            conn.close()
        self.archive_conn.close()
        self._stopped_event.set()

    def _join_named_stage_threads(self, names: Iterable[str], *, deadline: float) -> None:
        for name in names:
            thread = self._stage_threads.get(name)
            if thread is None:
                continue
            thread.join(timeout=max(0.0, deadline - self._now()))


class _AddsBlobs(Protocol):
    """The methods `_DiskBreakerGuardedStore` calls directly on its wrapped store."""

    def add(self, data: bytes) -> BlobLocation:
        raise NotImplementedError

    def current_writer(self) -> Any:
        raise NotImplementedError


class _DiskBreakerGuardedStore:
    """Gates `ArchiveStore.add` through the `local_disk` circuit breaker and
    `disk_guard`, and makes `current_writer()` shutdown-aware.

    Only `add` performs a segment write; every other method (`open_writer`,
    `seal_writer`, `reader`, ...) passes straight through, since the
    breaker's Phase-1 scope is "segment writes" specifically. Typed against
    `_AddsBlobs` rather than `ArchiveStore` so a test fake (or any other
    structurally-compatible object) doesn't need to subclass it.

    `disk_guard` gating here is defense in depth, not the primary
    mechanism: `Daemon._run_archive_thread` already checks
    `disk_guard.is_paused()` before ever popping the next handoff item, so
    in practice `add` is never called while paused. This check exists for
    the same reason the `local_disk` breaker already wraps `add` (a
    dependency-health signal gating the one method that touches the
    disk) -- a narrow TOCTOU window between that pre-pop check and this
    call, which, like an `OSError` from a real write failure, propagates
    out of `ArchiveHandoff.process_one` uncaught (see
    `Daemon._run_archive_thread`'s own `except Exception` boundary).

    `current_writer`, when constructed with a `shutdown_event`, wraps the
    real `SegmentWriter` in `_NoSealDuringShutdownWriter` so `_maybe_seal`
    (spec 09's `fetch.ArchiveHandoff`, not modified here) never seals a
    segment it happens to drain past its threshold during the shutdown
    sequence's in-flight-work grace period -- spec 10's shutdown step 4
    ("seal nothing") applies to *any* seal attempt, not just the one
    `shutdown()` itself doesn't make. `disk_guard`/`shutdown_event` both
    default to `None` so existing constructions (and tests) that only care
    about the breaker are unaffected.
    """

    def __init__(
        self,
        store: _AddsBlobs,
        breaker: CircuitBreaker,
        *,
        disk_guard: DiskGuardLike | None = None,
        shutdown_event: threading.Event | None = None,
    ) -> None:
        self._store = store
        self._breaker = breaker
        self._disk_guard = disk_guard
        self._shutdown_event = shutdown_event

    def add(self, data: bytes) -> BlobLocation:
        if self._disk_guard is not None and self._disk_guard.is_paused():
            raise DiskGuardPausedError(
                "disk guard is paused (free space below floor); skipping archive append"
            )
        return self._breaker.call(lambda: self._store.add(data))

    def current_writer(self) -> Any:
        writer = self._store.current_writer()
        if self._shutdown_event is None:
            return writer
        return _NoSealDuringShutdownWriter(writer, self._shutdown_event)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)


class _NoSealDuringShutdownWriter:
    """Wraps a `SegmentWriter` so `should_seal()` reports `False` once
    `shutdown_event` is set, regardless of the real segment's size/age.

    Every other attribute (`add`, `seal`, `segment_id`, ...) passes
    straight through via `__getattr__`, so a seal already in progress
    before shutdown was signaled, or a normal (non-shutdown) seal, behaves
    exactly as if this wrapper weren't there at all.
    """

    def __init__(self, writer: Any, shutdown_event: threading.Event) -> None:
        self._writer = writer
        self._shutdown_event = shutdown_event

    def should_seal(self) -> bool:
        if self._shutdown_event.is_set():
            return False
        return bool(self._writer.should_seal())

    def __getattr__(self, name: str) -> Any:
        return getattr(self._writer, name)


def _open_segment_ids(segments_dir: Path) -> set[int]:
    if not segments_dir.exists():
        return set()
    return {int(path.stem) for path in segments_dir.glob("*.open")}


def _shared_reader_conn(db_path: Path) -> sqlite3.Connection:
    """Open a read-only connection safe to use from any one thread at a time.

    `db.connect_reader` binds its connection to the creating thread; every
    caller here instead hands the connection to a dedicated stage thread
    that outlives `start()`, so it needs `check_same_thread=False`.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA cache_size = -262144")
    conn.execute("PRAGMA mmap_size = 4294967296")
    conn.execute("PRAGMA query_only = ON")
    return conn
