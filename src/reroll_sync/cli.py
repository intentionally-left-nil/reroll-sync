"""Command-line interface for reroll-sync: a launcher for the daemon, and a
client for operating it.

Every subcommand belongs to exactly one of three classes, declared in
:data:`COMMANDS` (never inferred from the command's name):

* ``init`` -- a writable, exclusive open; refuses if the daemon is running.
* ``read_only`` -- a plain :func:`~reroll_sync.db.connect_reader`; never
  calls :func:`~reroll_sync.db.init_db`; works whether or not the daemon
  is running.
* ``mutating`` -- no direct database access at all; goes over the
  daemon's control socket (see ``control_client.py``), so it requires a
  running daemon. Its registry entry holds a :class:`MutatingCommand`, not
  an arbitrary handler function, and :func:`_dispatch_mutating` is the
  only code path that ever calls
  :func:`~reroll_sync.control_client.send_control_command` -- structural,
  not a matter of every handler happening to follow the same convention.

``run`` is its own thing: it *is* the daemon. ``reprocess --dry-run`` is a
declared exception within ``mutating``: it is dispatched before
:func:`_dispatch_mutating` ever runs, straight to a read-only preview.
"""

from __future__ import annotations

import argparse
import dataclasses
import enum
import json
import os
import socket
import sqlite3
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, NoReturn

from . import fsck
from .archive.store import ArchiveStore
from .archive.verify import verify_archive
from .control_client import ControlClientError, send_control_command
from .daemon.config import ConfigError, config_from_env
from .daemon.service import Daemon
from .daemon.stage_loop import StageLoopStats
from .db import (
    AutoVacuumError,
    SchemaMismatchError,
    SchemaVersionError,
    connect_reader,
    init_db,
)
from .dispatcher import (
    ProjectSelector,
    RerollVersionBelow,
    Selector,
    SkippedOnly,
    Stage,
    StageMetrics,
    StateSelector,
    preview_selector,
)
from .health import Health, StageInput, alarms, snapshot
from .schema import WheelState

DEFAULT_DB_PATH = "reroll_sync.db"
DEFAULT_SOCKET_PATH = "reroll_sync.sock"
DEFAULT_SEGMENTS_DIR = "segments"
DEFAULT_ERRORS_LIMIT = 100
DEFAULT_LIVE_SOCKET_TIMEOUT = 1.0
CLI_CONNECT_TIMEOUT = 5.0
CLI_READ_TIMEOUT = 10.0
"""Bounded timeouts for a mutating command's control-socket round trip. Deliberately short
relative to `control_client.py`'s own defaults: a human waiting at a terminal for
`pause`/`drain`/etc. should get a clear error quickly, not wait half a minute to
find out the daemon isn't answering.
"""

_BYTE_UNITS = ("KB", "MB", "GB", "TB", "PB")


class CommandClass(enum.Enum):
    """A subcommand's database-access/daemon-requirement behavior. See module docstring."""

    INIT = "init"
    RUN = "run"
    READ_ONLY = "read_only"
    MUTATING = "mutating"


@dataclass(frozen=True)
class MutatingCommand:
    """A :data:`COMMANDS` entry's handler for :attr:`CommandClass.MUTATING`.

    Declares WHAT a mutating command sends over the control socket --
    ``command`` plus how to build its ``args`` from the parsed CLI
    namespace -- rather than an arbitrary handler function. This is what
    makes "every mutating command goes over the socket" structural:
    :func:`_dispatch_mutating` is the only code path that ever calls
    :func:`~reroll_sync.control_client.send_control_command`, so a mutating
    registry entry has no way to reach the database directly even if a
    future edit tried.
    """

    command: str
    build_args: Callable[[argparse.Namespace], Mapping[str, Any]]
    format_success: Callable[[Any], str]


@dataclass(frozen=True)
class CommandEntry:
    """One :data:`COMMANDS` entry: a subcommand's class and its handler.

    ``handler`` is a plain callable for every class except
    :attr:`CommandClass.MUTATING`, whose entries hold a
    :class:`MutatingCommand` instead -- see :func:`_dispatch_mutating`.
    """

    command_class: CommandClass
    handler: Callable[..., int] | MutatingCommand


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    entry = COMMANDS[args.command]
    if entry.command_class is CommandClass.MUTATING:
        # `reprocess --dry-run` is a structurally-distinct, explicitly-declared
        # exception: it reads the database directly and never touches the
        # control socket, so it is dispatched *before* the generic mutating
        # wrapper -- the one and only path to `send_control_command` -- ever
        # runs, rather than pretending to be a normal mutating command.
        if args.command == "reprocess" and args.dry_run:
            return _dispatch_reprocess_dry_run(args)
        assert isinstance(entry.handler, MutatingCommand)
        return _dispatch_mutating(args, entry.handler)
    assert not isinstance(entry.handler, MutatingCommand)
    if entry.command_class is CommandClass.READ_ONLY:
        return _dispatch_read_only(args, entry.handler)
    return entry.handler(args)


def build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="reroll-sync", description="Launch and operate the reroll-sync daemon."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    db_default = os.environ.get("REROLL_SYNC_DB", DEFAULT_DB_PATH)
    socket_default = os.environ.get("REROLL_SYNC_SOCKET_PATH", DEFAULT_SOCKET_PATH)
    segments_default = os.environ.get("REROLL_SYNC_SEGMENTS_DIR", DEFAULT_SEGMENTS_DIR)

    def _add_db_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--db",
            default=db_default,
            help=(
                f"Path to the sqlite database file (env REROLL_SYNC_DB; default {DEFAULT_DB_PATH})."
            ),
        )

    def _add_socket_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--socket",
            default=socket_default,
            help=(
                "Path to the daemon's control socket "
                f"(env REROLL_SYNC_SOCKET_PATH; default {DEFAULT_SOCKET_PATH})."
            ),
        )

    def _add_segments_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--segments-dir",
            default=segments_default,
            help=f"Path to the archive segments directory (env REROLL_SYNC_SEGMENTS_DIR; "
            f"default {DEFAULT_SEGMENTS_DIR}).",
        )

    init_help = (
        "Create or verify the sqlite database. Does not require a running daemon; "
        "refuses to run if one already is."
    )
    init_parser = subparsers.add_parser("init", help=init_help, description=init_help)
    _add_db_arg(init_parser)
    _add_socket_arg(init_parser)

    run_help = (
        "Run the daemon in the foreground. This process becomes the daemon: it does "
        "not require another daemon already running, and refuses to start if one is."
    )
    run_parser = subparsers.add_parser("run", help=run_help, description=run_help)
    run_parser.add_argument(
        "--config-from-env",
        action="store_true",
        help="Load configuration from REROLL_SYNC_* environment variables "
        "(currently the only supported source).",
    )

    status_help = "Show a health snapshot. Read-only; safe whether or not the daemon is running."
    status_parser = subparsers.add_parser("status", help=status_help, description=status_help)
    _add_db_arg(status_parser)
    _add_segments_arg(status_parser)
    status_parser.add_argument(
        "--json", action="store_true", help="Emit the full health snapshot as JSON."
    )

    errors_help = "List recent errors. Read-only; safe whether or not the daemon is running."
    errors_parser = subparsers.add_parser("errors", help=errors_help, description=errors_help)
    _add_db_arg(errors_parser)
    errors_parser.add_argument("--category", default=None, help="Only this error category.")
    errors_parser.add_argument(
        "--since", default=None, help="ISO 8601 date/time; only errors at or after this."
    )
    errors_parser.add_argument("--limit", type=int, default=DEFAULT_ERRORS_LIMIT)

    fsck_help = "Check database invariants. Read-only; safe whether or not the daemon is running."
    fsck_parser = subparsers.add_parser("fsck", help=fsck_help, description=fsck_help)
    _add_db_arg(fsck_parser)
    fsck_parser.add_argument("--chunk", type=int, default=fsck.DEFAULT_CHUNK_SIZE)

    verify_help = (
        "Check archive segment integrity. Read-only; safe whether or not the daemon is running."
    )
    verify_parser = subparsers.add_parser(
        "verify-archive", help=verify_help, description=verify_help
    )
    _add_db_arg(verify_parser)
    _add_segments_arg(verify_parser)
    verify_parser.add_argument(
        "--segment",
        type=int,
        default=None,
        help="Restrict to one segment id (default: every sealed segment).",
    )

    queue_help = "Show derived-queue depth. Read-only; safe whether or not the daemon is running."
    queue_parser = subparsers.add_parser("queue", help=queue_help, description=queue_help)
    _add_db_arg(queue_parser)
    queue_parser.add_argument(
        "--stage", choices=[s.value for s in Stage], default=None, help="Only this stage's queue."
    )

    pause_help = (
        "Pause a stage's claiming of new work. Requires a running daemon "
        "(goes over the control socket)."
    )
    pause_parser = subparsers.add_parser("pause", help=pause_help, description=pause_help)
    _add_socket_arg(pause_parser)
    pause_parser.add_argument("stage")

    resume_help = "Resume a paused stage. Requires a running daemon (goes over the control socket)."
    resume_parser = subparsers.add_parser("resume", help=resume_help, description=resume_help)
    _add_socket_arg(resume_parser)
    resume_parser.add_argument("stage")

    drain_help = (
        "Pause every stage's claiming; let in-flight work finish. Requires a running "
        "daemon (goes over the control socket)."
    )
    drain_parser = subparsers.add_parser("drain", help=drain_help, description=drain_help)
    _add_socket_arg(drain_parser)

    reprocess_help = (
        "Requeue wheels matching a selector. Requires a running daemon (goes over the "
        "control socket) unless --dry-run, which reads the database directly instead."
    )
    reprocess_parser = subparsers.add_parser(
        "reprocess", help=reprocess_help, description=reprocess_help
    )
    _add_socket_arg(reprocess_parser)
    _add_db_arg(reprocess_parser)
    reprocess_selector = reprocess_parser.add_mutually_exclusive_group(required=True)
    reprocess_selector.add_argument("--reroll-version-below", metavar="VERSION", default=None)
    reprocess_selector.add_argument("--state", choices=[s.name for s in WheelState], default=None)
    reprocess_selector.add_argument("--project", default=None)
    reprocess_selector.add_argument("--skipped-only", action="store_true")
    reprocess_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the affected wheel count and skips that would be cleared; write nothing; "
        "does not require a running daemon.",
    )

    unquarantine_help = (
        "Clear quarantine and requeue for re-fetch. Requires a running daemon "
        "(goes over the control socket)."
    )
    unquarantine_parser = subparsers.add_parser(
        "unquarantine", help=unquarantine_help, description=unquarantine_help
    )
    _add_socket_arg(unquarantine_parser)
    unquarantine_selector = unquarantine_parser.add_mutually_exclusive_group(required=True)
    unquarantine_selector.add_argument(
        "--stage", choices=[s.name for s in WheelState], default=None
    )
    unquarantine_selector.add_argument("--project", default=None)
    unquarantine_selector.add_argument("--all", action="store_true")

    shutdown_help = (
        "Gracefully stop the daemon. Requires a running daemon (goes over the control socket)."
    )
    shutdown_parser = subparsers.add_parser(
        "shutdown", help=shutdown_help, description=shutdown_help
    )
    _add_socket_arg(shutdown_parser)

    return parser


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


def _cmd_init(args: argparse.Namespace) -> int:
    socket_path = Path(args.socket)
    if _socket_is_live(socket_path):
        print(
            f"refusing to init: a daemon is already running (control socket "
            f"'{socket_path}' is live)",
            file=sys.stderr,
        )
        return 1
    try:
        init_db(args.db)
    except (SchemaMismatchError, SchemaVersionError, AutoVacuumError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"Initialized database at '{args.db}'")
    return 0


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def _cmd_run(args: argparse.Namespace) -> int:
    del args  # --config-from-env is the only supported source already
    try:
        config = config_from_env()
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if _socket_is_live(config.socket_path):
        print(
            f"refusing to start: a daemon is already running (control socket "
            f"'{config.socket_path}' is live)",
            file=sys.stderr,
        )
        return 1
    daemon = Daemon(config)
    daemon.install_signal_handlers()
    daemon.run_forever()
    return 0


# ---------------------------------------------------------------------------
# Read-only commands
# ---------------------------------------------------------------------------


def _cmd_status(args: argparse.Namespace, reader: sqlite3.Connection) -> int:
    """Print a health snapshot (human-readable, or `--json`).

    Known, permanent limitation: RATE always reports "not available" from
    this command. `limiter`/`breakers` (the daemon's in-process
    rate-limiter/circuit-breaker state) exist only in a live daemon's
    memory and cannot be reconstructed from a separate, read-only CLI
    process; wiring the control socket into `status` to blend in
    live-only figures (e.g. uptime, rate-limiter state) when a daemon
    happens to be running is a larger design question, deliberately not
    addressed here.
    """
    archive_store = _status_archive_store(reader, Path(args.segments_dir))
    health = snapshot(reader, stages=_empty_stage_inputs(), archive_store=archive_store)
    found_alarms = alarms(health)
    if args.json:
        print(json.dumps(dataclasses.asdict(health)))
    else:
        print(_format_status(health, found_alarms))
    return 3 if any(alarm.severity == "critical" for alarm in found_alarms) else 0


def _status_archive_store(reader: sqlite3.Connection, segments_dir: Path) -> ArchiveStore | None:
    """A read-only `ArchiveStore` for `segments_dir`, or `None` if it doesn't exist.

    Mirrors `_cmd_verify_archive`'s `recover=False` pattern: never touches
    (or creates) a directory a live daemon may already own. Unlike
    `verify-archive`, a missing directory is not an error here -- `status`
    degrades to the same zeroed ARCHIVE fields it always reported before
    this was wired in, rather than failing a read-only health check.
    """
    if not segments_dir.exists():
        return None
    return ArchiveStore(segments_dir, reader, recover=False)


def _cmd_errors(args: argparse.Namespace, reader: sqlite3.Connection) -> int:
    conditions: list[str] = []
    params: list[Any] = []
    if args.category is not None:
        conditions.append("error_category = ?")
        params.append(args.category)
    if args.since is not None:
        try:
            since = datetime.fromisoformat(args.since)
        except ValueError as exc:
            print(f"invalid --since value {args.since!r}: {exc}", file=sys.stderr)
            return 1
        conditions.append("created_at >= ?")
        params.append(since.isoformat())
    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = (
        "SELECT created_at, wheel_id, error_category, error_subcat, details, reroll_version "
        f"FROM errors {where_sql} ORDER BY created_at DESC LIMIT ?"
    )
    params.append(args.limit)
    rows = reader.execute(sql, params).fetchall()
    if not rows:
        print("no matching errors")
        return 0
    for created_at, wheel_id, category, subcat, details, reroll_version in rows:
        print(
            f"{created_at}  wheel={wheel_id}  {category}/{subcat or '-'}  "
            f"{details or ''}  (reroll {reroll_version})"
        )
    return 0


def _cmd_fsck(args: argparse.Namespace, reader: sqlite3.Connection) -> int:
    report = fsck.run(reader, chunk_size=args.chunk)
    if not report.violations:
        print("fsck: clean")
        return 0
    for violation in report.violations:
        marker = "info" if violation.informational else "FAIL"
        print(
            f"[{marker}] {violation.invariant}: {violation.description} (count={violation.count})"
        )
        print(f"         examples: {list(violation.example_ids[:5])}")
    return 0 if report.ok else 2


def _cmd_verify_archive(args: argparse.Namespace, reader: sqlite3.Connection) -> int:
    segments_dir = Path(args.segments_dir)
    if not segments_dir.exists():
        print(f"segments directory '{segments_dir}' does not exist", file=sys.stderr)
        return 1
    store = ArchiveStore(segments_dir, reader, recover=False)
    report = verify_archive(store, segment_id=args.segment)
    if report.ok:
        print("verify-archive: clean")
        return 0
    for problem in report.problems:
        print(problem)
    return 2


def _cmd_queue(args: argparse.Namespace, reader: sqlite3.Connection) -> int:
    """`--stage` is restricted by argparse to `Stage`'s values, both of which
    `_empty_stage_inputs()` always supplies, so every name here is always
    present in `health.queues`.
    """
    health = snapshot(reader, stages=_empty_stage_inputs())
    names = [args.stage] if args.stage is not None else sorted(health.queues)
    for name in names:
        queue = health.queues[name]
        lanes = ", ".join(
            f"lane{lane}={count:,}" for lane, count in sorted(queue.depth_by_lane.items())
        )
        suffix = f" ({lanes})" if lanes else ""
        print(f"{name}: depth={queue.depth:,}{suffix}")
    return 0


# ---------------------------------------------------------------------------
# Mutating commands
#
# Every entry below is a `MutatingCommand`, not a handler function: see
# `MutatingCommand`/`_dispatch_mutating` for why that is what makes "goes
# over the control socket" structural rather than a matter of convention.
# ---------------------------------------------------------------------------


def _dispatch_reprocess_dry_run(args: argparse.Namespace) -> int:
    """`reprocess --dry-run`'s explicitly-declared exception: reads the
    database directly (like a read-only command), never the control socket.
    """
    selector = _reprocess_selector_from_args(args)
    return _dispatch_read_only(
        args, lambda a, reader: _print_reprocess_preview(reader, a, selector)
    )


def _print_reprocess_preview(
    reader: sqlite3.Connection, args: argparse.Namespace, selector: Selector
) -> int:
    del args
    preview = preview_selector(reader, selector)
    print(
        f"dry run: {preview.wheel_count} wheel(s) would be affected, "
        f"{preview.skips_to_clear_count} skips row(s) would be cleared"
    )
    return 0


COMMANDS: Mapping[str, CommandEntry] = {
    "init": CommandEntry(CommandClass.INIT, _cmd_init),
    "run": CommandEntry(CommandClass.RUN, _cmd_run),
    "status": CommandEntry(CommandClass.READ_ONLY, _cmd_status),
    "errors": CommandEntry(CommandClass.READ_ONLY, _cmd_errors),
    "fsck": CommandEntry(CommandClass.READ_ONLY, _cmd_fsck),
    "verify-archive": CommandEntry(CommandClass.READ_ONLY, _cmd_verify_archive),
    "queue": CommandEntry(CommandClass.READ_ONLY, _cmd_queue),
    "pause": CommandEntry(
        CommandClass.MUTATING,
        MutatingCommand(
            command="pause",
            build_args=lambda a: {"stage": a.stage},
            format_success=lambda r: f"paused stage {r['stage']!r}",
        ),
    ),
    "resume": CommandEntry(
        CommandClass.MUTATING,
        MutatingCommand(
            command="resume",
            build_args=lambda a: {"stage": a.stage},
            format_success=lambda r: f"resumed stage {r['stage']!r}",
        ),
    ),
    "drain": CommandEntry(
        CommandClass.MUTATING,
        MutatingCommand(
            command="drain",
            build_args=lambda _a: {},
            format_success=lambda _r: "draining",
        ),
    ),
    "reprocess": CommandEntry(
        CommandClass.MUTATING,
        MutatingCommand(
            command="reprocess",
            build_args=lambda a: _selector_to_args(_reprocess_selector_from_args(a)),
            format_success=lambda r: f"reprocess submitted: {r['affected']} wheel(s) affected",
        ),
    ),
    "unquarantine": CommandEntry(
        CommandClass.MUTATING,
        MutatingCommand(
            command="unquarantine",
            build_args=lambda a: _selector_to_args(_unquarantine_selector_from_args(a)),
            format_success=lambda r: f"unquarantine submitted: {r['affected']} wheel(s) affected",
        ),
    ),
    "shutdown": CommandEntry(
        CommandClass.MUTATING,
        MutatingCommand(
            command="shutdown",
            build_args=lambda _a: {},
            format_success=lambda _r: "shutdown requested",
        ),
    ),
}


# ---------------------------------------------------------------------------
# Shared dispatch/formatting helpers
# ---------------------------------------------------------------------------


class _ArgumentParser(argparse.ArgumentParser):
    """Exits 1 (not argparse's default 2) on a usage error.

    Spec 12's exit-code table reserves 2 for ``fsck``/``verify-archive``
    violations; a bad CLI invocation is a command error (1), the same
    bucket as a missing database or no daemon.
    """

    def error(self, message: str) -> NoReturn:
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")


def _dispatch_read_only(
    args: argparse.Namespace, handler: Callable[[argparse.Namespace, sqlite3.Connection], int]
) -> int:
    try:
        reader = connect_reader(args.db)
    except sqlite3.OperationalError as exc:
        print(f"cannot read database '{args.db}': {exc}", file=sys.stderr)
        return 1
    try:
        return handler(args, reader)
    finally:
        reader.close()


def _dispatch_mutating(args: argparse.Namespace, spec: MutatingCommand) -> int:
    """The only code path that calls :func:`_control`/`send_control_command`.

    Every ``MUTATING`` registry entry is a :class:`MutatingCommand`
    describing what to send; this function is what actually sends it, so a
    mutating command has no way to reach the database directly.
    """
    return _control(args, spec.command, spec.build_args(args), spec.format_success)


def _control(
    args: argparse.Namespace,
    command: str,
    cmd_args: Mapping[str, Any],
    format_success: Callable[[Any], str],
) -> int:
    try:
        result = send_control_command(
            Path(args.socket),
            command,
            cmd_args,
            connect_timeout=CLI_CONNECT_TIMEOUT,
            read_timeout=CLI_READ_TIMEOUT,
        )
    except ControlClientError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(format_success(result))
    return 0


def _selector_to_args(selector: Selector) -> dict[str, Any]:
    if isinstance(selector, RerollVersionBelow):
        return {"type": "reroll_version_below", "version": selector.version}
    if isinstance(selector, ProjectSelector):
        return {"type": "project", "project": selector.project}
    if isinstance(selector, StateSelector):
        return {"type": "state", "state": selector.state.name}
    return {"type": "skipped_only"}


def _reprocess_selector_from_args(args: argparse.Namespace) -> Selector:
    if args.reroll_version_below is not None:
        return RerollVersionBelow(version=args.reroll_version_below)
    if args.state is not None:
        return StateSelector(state=WheelState[args.state])
    if args.project is not None:
        return ProjectSelector(project=args.project)
    return SkippedOnly()  # --skipped-only: the mutually exclusive group guarantees this


def _unquarantine_selector_from_args(args: argparse.Namespace) -> Selector:
    if args.project is not None:
        return ProjectSelector(project=args.project)
    if args.stage is not None:
        return StateSelector(state=WheelState[args.stage])
    return StateSelector(state=WheelState.QUARANTINED)  # --all


def _socket_is_live(socket_path: Path, *, timeout: float = DEFAULT_LIVE_SOCKET_TIMEOUT) -> bool:
    """Whether something is listening at ``socket_path``.

    Only asks whether *a* connection can be established, not whether the
    protocol on the other end actually answers -- ``init``/``run`` only
    need to know "is this socket claimed", not talk to whatever holds it.
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        sock.connect(str(socket_path))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _empty_stage_inputs() -> dict[str, StageInput]:
    """`fetch`/`convert` `StageInput`s with a live-daemon-shaped but zeroed queue.

    Lets `health.snapshot()` populate the database-derived parts of
    `queues` (`depth`, `depth_by_lane`, `oldest_pending_age_seconds`) for a
    one-shot CLI process, while the live-only parts (`in_flight`,
    `throughput_ema`, outcome counts) stay at their zero defaults, since
    nothing persists them.
    """
    zero_loop = StageLoopStats(
        last_run_at=None, last_success_at=None, consecutive_failures=0, paused=False
    )
    zero_queue = StageMetrics(
        queue_depth=0,
        in_flight=0,
        oldest_pending_age=None,
        throughput_ema=0.0,
        outcome_counts={},
        retry_count=0,
        quarantine_count=0,
    )
    return {
        "fetch": StageInput(loop=zero_loop, queue=zero_queue),
        "convert": StageInput(loop=zero_loop, queue=zero_queue),
    }


def _format_bytes(n: int) -> str:
    """Human-readable byte count, e.g. ``0 B``, ``1023 B``, ``1.0 KB``, ``2.5 TB``."""
    if n < 1024:
        return f"{n} B"
    value = float(n)
    unit = _BYTE_UNITS[0]
    for candidate in _BYTE_UNITS:
        value /= 1024
        unit = candidate
        if value < 1024:
            break
    return f"{value:.1f} {unit}"


def _format_duration(seconds: float) -> str:
    """Human-readable duration for an archive segment's age, e.g. ``45s``,
    ``3m12s``, ``1h14m``.
    """
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    minutes, secs = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _format_status(health: Health, found_alarms: tuple[Any, ...]) -> str:
    lines = ["reroll-sync status", ""]

    if found_alarms:
        lines.append("ALARMS")
        for alarm in found_alarms:
            marker = "!" if alarm.severity == "critical" else "~"
            lines.append(f"  {marker} {alarm.message}")
    else:
        lines.append("ALARMS: none")
    lines.append("")

    poll_age = (
        None
        if health.last_index_poll_at is None
        else health.snapshot_at - health.last_index_poll_at
    )
    poll_desc = "never" if poll_age is None else f"{poll_age:.0f}s ago"
    lines.append(
        f"FRESHNESS       index lag {health.index_lag:,} serial(s)   last poll {poll_desc}"
    )

    queue_parts = []
    for name in sorted(health.queues):
        queue = health.queues[name]
        lanes = ", ".join(
            f"lane{lane}={count:,}" for lane, count in sorted(queue.depth_by_lane.items())
        )
        queue_parts.append(f"{name} {queue.depth:,}" + (f" ({lanes})" if lanes else ""))
    lines.append("QUEUES          " + ("   ".join(queue_parts) if queue_parts else "none"))

    state_parts = [
        f"{name.lower()} {count:,}" for name, count in health.state_counts.items() if count > 0
    ]
    lines.append("STATES          " + ("  ".join(state_parts) if state_parts else "none"))

    rate_parts = [
        f"{name} {snap.available:,.0f}" for name, snap in sorted(health.limiter_children.items())
    ]
    if rate_parts:
        lines.append("RATE            " + "   ".join(rate_parts))
    else:
        # `limiter`/`breakers` are never supplied from this CLI path (see
        # `_cmd_status`'s docstring): say so honestly rather than printing a
        # bare `none` that reads like a real (zero) measurement.
        lines.append(
            "RATE            not available (run against a live daemon's control "
            "socket for live rate-limiter state)"
        )

    age_suffix = (
        ""
        if health.open_segment_age_seconds is None
        else f", age {_format_duration(health.open_segment_age_seconds)}"
    )
    lines.append(
        f"ARCHIVE         {health.segments_sealed} sealed ({_format_bytes(health.archive_bytes)})"
        f"  open {_format_bytes(health.open_segment_bytes)}{age_suffix}"
    )
    lines.append(
        f"SQLITE          db {_format_bytes(health.db_bytes)}  "
        f"wal {_format_bytes(health.wal_bytes)}  "
        f"longest read {health.longest_read_txn_ms:.0f}ms"
    )

    return "\n".join(lines)
