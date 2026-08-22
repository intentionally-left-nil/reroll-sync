"""Tests for the CLI: structure/dispatch, read-only commands, `init`,
mutating commands (over a real control socket), `reprocess`, and output
formatting.
"""

from __future__ import annotations

import ast
import json
import shutil
import socket
import sqlite3
import tempfile
from pathlib import Path

import pytest

from reroll_sync import cli
from reroll_sync.cli import COMMANDS, CommandClass, build_parser, main
from reroll_sync.control import ControlHandlers, ControlServer
from reroll_sync.db import connect_reader, init_db
from reroll_sync.dispatcher import ProjectSelector, RerollVersionBelow, SkippedOnly, StateSelector
from reroll_sync.schema import WheelState

# ---------------------------------------------------------------------------
# Shared fixtures/helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "reroll_sync.db")
    init_db(path)
    return path


def _insert_wheel(
    db_path: str,
    *,
    filename: str,
    project: str = "proj",
    state: WheelState = WheelState.NEED_CONVERT,
    lane: int = 0,
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO wheels (filename, project, state, lane, url, serial, change_seq, "
            "updated_at) VALUES (?, ?, ?, ?, ?, 1, 1, '2024-01-01T00:00:00+00:00')",
            (filename, project, int(state), lane, f"https://example.test/{filename}"),
        )
        conn.commit()
        assert cursor.lastrowid is not None
        return cursor.lastrowid
    finally:
        conn.close()


class _FakeHandlers:
    """Backs a real `ControlServer`, mirroring test_control.py's style."""

    def __init__(self) -> None:
        self.paused: list[str] = []
        self.resumed: list[str] = []
        self.drained = False
        self.shutdown_called = False
        self.reprocess_calls: list[object] = []
        self.unquarantine_calls: list[object] = []
        self.status_result: dict = {"stages": {}}
        self.reprocess_result = 0
        self.unquarantine_result = 0
        self.raise_on_pause: Exception | None = None

    def status(self):
        return self.status_result

    def pause(self, stage: str) -> None:
        if self.raise_on_pause is not None:
            raise self.raise_on_pause
        self.paused.append(stage)

    def resume(self, stage: str) -> None:
        self.resumed.append(stage)

    def drain(self) -> None:
        self.drained = True

    def reprocess(self, selector) -> int:
        self.reprocess_calls.append(selector)
        return self.reprocess_result

    def unquarantine(self, selector) -> int:
        self.unquarantine_calls.append(selector)
        return self.unquarantine_result

    def shutdown(self) -> None:
        self.shutdown_called = True


def _handlers(fake: _FakeHandlers) -> ControlHandlers:
    return ControlHandlers(
        status=fake.status,
        pause=fake.pause,
        resume=fake.resume,
        drain=fake.drain,
        reprocess=fake.reprocess,
        unquarantine=fake.unquarantine,
        shutdown=fake.shutdown,
    )


@pytest.fixture
def fake() -> _FakeHandlers:
    return _FakeHandlers()


@pytest.fixture
def socket_dir():
    # macOS caps AF_UNIX paths well below pytest's default (deeply nested)
    # tmp_path, so control sockets get their own short-lived directory
    # directly under /tmp instead (mirrors test_control.py).
    path = Path(tempfile.mkdtemp(prefix="rs-cli-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def server(socket_dir, fake):
    socket_path = socket_dir / "control.sock"
    srv = ControlServer(socket_path, _handlers(fake))
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture
def socket_path(socket_dir) -> str:
    return str(socket_dir / "control.sock")


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------


def test_every_registry_command_is_reachable_via_help(capsys):
    for name in COMMANDS:
        with pytest.raises(SystemExit) as exc_info:
            main([name, "--help"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert name in captured.out


def test_every_parser_subcommand_has_a_registry_entry():
    import argparse

    parser = build_parser()
    subparsers_action = next(
        action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
    )
    assert set(subparsers_action.choices) == set(COMMANDS)


def test_every_registry_entry_declares_a_command_class():
    for entry in COMMANDS.values():
        assert isinstance(entry.command_class, CommandClass)


def test_every_mutating_command_registry_entry_is_declarative_not_a_callable():
    """Spec 12 requires this to be structural, not ad-hoc `if` branches: every
    `MUTATING` registry entry must hold a `MutatingCommand` describing WHAT to
    send (command name + how to build `args`), not an arbitrary handler
    function a mutating command could use to skip the control socket.
    """
    from reroll_sync.cli import MutatingCommand

    mutating_entries = [
        entry for entry in COMMANDS.values() if entry.command_class is CommandClass.MUTATING
    ]
    assert mutating_entries  # at least one to actually exercise this
    for entry in mutating_entries:
        assert isinstance(entry.handler, MutatingCommand)
        assert not callable(entry.handler)


def _functions_calling(tree, target_name: str) -> set[str]:
    callers = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for inner in ast.walk(node):
                if (
                    isinstance(inner, ast.Call)
                    and isinstance(inner.func, ast.Name)
                    and inner.func.id == target_name
                ):
                    callers.add(node.name)
    return callers


def test_dispatch_mutating_is_the_only_path_to_send_control_command():
    """The generic `_dispatch_mutating` wrapper -- not each individual
    mutating command -- is structurally the only path that reaches
    `send_control_command`: `_control` is the sole direct caller of
    `send_control_command`, and `_dispatch_mutating` is the sole caller of
    `_control`.
    """
    tree = ast.parse(Path(cli.__file__).read_text())
    assert _functions_calling(tree, "send_control_command") == {"_control"}
    assert _functions_calling(tree, "_control") == {"_dispatch_mutating"}


def test_unknown_command_errors_not_a_fall_through(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["definitely-not-a-real-command"])
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "invalid choice" in captured.err


def test_no_command_at_all_errors(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main([])
    assert exc_info.value.code == 1


def _normalized_help_output(capsys, argv: list[str]) -> str:
    """`argv` (ending in `--help`) rendered, with argparse's line-wrapping
    collapsed to single spaces so a multi-word phrase can be searched for
    regardless of exactly where the formatter wrapped it.
    """
    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    return " ".join(captured.out.split())


def test_help_states_daemon_requirement_for_a_mutating_command(capsys):
    """`reroll-sync pause --help` (the help a user actually runs for a
    subcommand) must state the daemon requirement itself, not merely be
    listed with it in the top-level `--help` summary.
    """
    out = _normalized_help_output(capsys, ["pause", "--help"])
    assert "Requires a running daemon" in out


def test_help_states_daemon_requirement_for_a_read_only_command(capsys):
    """Same as above, for a read-only subcommand's own `--help`."""
    out = _normalized_help_output(capsys, ["status", "--help"])
    assert "whether or not the daemon is running" in out


@pytest.mark.parametrize(
    ("command", "expected_phrase"),
    [
        ("init", "Does not require a running daemon"),
        ("run", "does not require another daemon"),
        ("status", "whether or not the daemon is running"),
        ("errors", "whether or not the daemon is running"),
        ("fsck", "whether or not the daemon is running"),
        ("verify-archive", "whether or not the daemon is running"),
        ("queue", "whether or not the daemon is running"),
        ("pause", "Requires a running daemon"),
        ("resume", "Requires a running daemon"),
        ("drain", "Requires a running daemon"),
        ("reprocess", "Requires a running daemon"),
        ("unquarantine", "Requires a running daemon"),
        ("shutdown", "Requires a running daemon"),
    ],
)
def test_every_subcommands_own_help_states_its_daemon_requirement(command, expected_phrase, capsys):
    """`add_parser(name, help=X)`'s `help=` only populates the top-level
    listing, not the subparser's own `description` -- so
    `reroll-sync <command> --help` must be checked directly for each command.
    """
    out = _normalized_help_output(capsys, [command, "--help"])
    assert expected_phrase in out


def test_removed_commands_no_longer_exist():
    assert "sync-index" not in COMMANDS
    assert "sync-metadata" not in COMMANDS
    assert "parse-metadata" not in COMMANDS
    assert "sync-reroll" not in COMMANDS


def test_removed_commands_error_at_the_cli(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["sync-index"])
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Read-only commands
# ---------------------------------------------------------------------------


def test_status_against_nonexistent_database_exits_1_and_creates_nothing(tmp_path, capsys):
    db_path = tmp_path / "missing.db"
    exit_code = main(["status", "--db", str(db_path)])
    assert exit_code == 1
    assert not db_path.exists()
    captured = capsys.readouterr()
    assert str(db_path) in captured.err


def test_errors_against_nonexistent_database_exits_1_and_creates_nothing(tmp_path, capsys):
    db_path = tmp_path / "missing.db"
    exit_code = main(["errors", "--db", str(db_path)])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert not db_path.exists()
    assert str(db_path) in captured.err


def test_fsck_against_nonexistent_database_exits_1_and_creates_nothing(tmp_path, capsys):
    db_path = tmp_path / "missing.db"
    exit_code = main(["fsck", "--db", str(db_path)])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert not db_path.exists()
    assert str(db_path) in captured.err


def test_verify_archive_against_nonexistent_database_exits_1_and_creates_nothing(tmp_path, capsys):
    db_path = tmp_path / "missing.db"
    exit_code = main(["verify-archive", "--db", str(db_path)])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert not db_path.exists()
    assert str(db_path) in captured.err


def test_queue_against_nonexistent_database_exits_1_and_creates_nothing(tmp_path, capsys):
    db_path = tmp_path / "missing.db"
    exit_code = main(["queue", "--db", str(db_path)])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert not db_path.exists()
    assert str(db_path) in captured.err


@pytest.mark.parametrize("command", ["status", "errors", "fsck", "verify-archive", "queue"])
def test_read_only_command_never_opens_a_writable_connection(command, db_path, monkeypatch):
    """Injects a `connect_reader` that raises if ever called with a
    writable-looking path, and confirms `sqlite3.connect`/`connect_writer`
    are never reached from the read-only dispatch path.
    """

    def _fail_on_writable_open(path, *args, **kwargs):
        raise AssertionError(f"attempted a writable open of {path!r}")

    monkeypatch.setattr("reroll_sync.db.connect_writer", _fail_on_writable_open)
    monkeypatch.setattr("reroll_sync.cli.init_db", _fail_on_writable_open)

    argv = [command, "--db", db_path]
    if command == "verify-archive":
        argv += ["--segments-dir", str(Path(db_path).parent / "segments")]
    exit_code = main(argv)
    assert exit_code in (0, 1)  # never raises AssertionError


@pytest.mark.parametrize("command", ["status", "errors", "fsck", "verify-archive", "queue"])
def test_read_only_command_never_calls_init_db(command, db_path, monkeypatch):
    calls = []
    monkeypatch.setattr("reroll_sync.cli.init_db", lambda *a, **k: calls.append((a, k)))
    argv = [command, "--db", db_path]
    if command == "verify-archive":
        argv += ["--segments-dir", str(Path(db_path).parent / "segments")]
    main(argv)
    assert calls == []


@pytest.mark.parametrize("command", ["status", "errors", "fsck", "verify-archive", "queue"])
def test_read_only_command_works_while_a_simulated_daemon_holds_a_writer(command, tmp_path):
    db_path = str(tmp_path / "reroll_sync.db")
    init_db(db_path)
    writer_conn = sqlite3.connect(db_path, check_same_thread=False)
    writer_conn.execute("PRAGMA journal_mode = WAL")
    try:
        argv = [command, "--db", db_path]
        if command == "verify-archive":
            argv += ["--segments-dir", str(tmp_path / "segments")]
        exit_code = main(argv)
        assert exit_code in (0, 1, 2)
    finally:
        writer_conn.close()


def test_status_human_output(db_path, capsys):
    _insert_wheel(db_path, filename="a-1.0-py3-none-any.whl", state=WheelState.QUARANTINED)
    exit_code = main(["status", "--db", db_path])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "FRESHNESS" in captured.out
    assert "quarantined 1" in captured.out


def test_status_reports_real_archive_segment_counts_and_disk_free_bytes(db_path, tmp_path, capsys):
    """`_cmd_status` must wire a read-only `ArchiveStore` into `snapshot()`
    the same way `_cmd_verify_archive` does. `disk_free_bytes` is the ARCHIVE
    field this actually changes for a one-shot CLI process: `segments_sealed`/
    `archive_bytes` already came straight from the database either way, and
    `open_segment_bytes`/`open_segment_age_seconds` reflect only *this*
    `ArchiveStore` instance's own in-process writer (`recover=False` never
    inspects the directory for one, by design -- see `ArchiveStore`'s
    docstring), so they stay `0`/absent regardless of what's on disk.
    """
    from reroll_sync.archive.store import ArchiveStore
    from reroll_sync.db import connect_writer

    segments_dir = tmp_path / "segments"
    writer_conn = connect_writer(db_path)
    store = ArchiveStore(segments_dir, writer_conn)
    store.add(b"sealed-blob")
    store.seal_writer(store.current_writer())
    writer_conn.close()

    exit_code = main(["status", "--db", db_path, "--segments-dir", str(segments_dir), "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    parsed = json.loads(captured.out)
    assert parsed["segments_sealed"] == 1
    assert parsed["disk_free_bytes"] is not None
    assert parsed["disk_free_bytes"] > 0


def test_status_disk_free_bytes_is_none_without_a_segments_dir(db_path, tmp_path, capsys):
    """No `--segments-dir` (or one that doesn't exist) must degrade to the
    same `None` `disk_free_bytes` as before -- never create a directory as
    a side effect of a read-only command.
    """
    missing = tmp_path / "nope"
    exit_code = main(["status", "--db", db_path, "--segments-dir", str(missing), "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    parsed = json.loads(captured.out)
    assert parsed["disk_free_bytes"] is None
    assert not missing.exists()


def test_status_json_round_trips_and_has_every_health_field(db_path, capsys):
    import dataclasses

    from reroll_sync.health import Health

    exit_code = main(["status", "--db", db_path, "--json"])
    captured = capsys.readouterr()
    assert exit_code == 0
    parsed = json.loads(captured.out)
    for field in dataclasses.fields(Health):
        assert field.name in parsed


def test_status_exit_code_3_on_critical_alarm(db_path, monkeypatch, capsys):
    from reroll_sync import health as health_module

    monkeypatch.setattr(
        "reroll_sync.cli.alarms",
        lambda health, **k: (
            health_module.Alarm(severity="critical", condition="test", message="boom"),
        ),
    )
    exit_code = main(["status", "--db", db_path])
    captured = capsys.readouterr()
    assert exit_code == 3
    assert "boom" in captured.out


def test_status_exit_code_0_with_no_alarms(db_path):
    exit_code = main(["status", "--db", db_path])
    assert exit_code == 0


def test_errors_reports_matching_rows(db_path, capsys):
    wheel_id = _insert_wheel(db_path, filename="errwheel-1.0-py3-none-any.whl")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO errors (wheel_id, error_category, details, reroll_version, created_at) "
        "VALUES (?, 'network', 'boom', '1.0', '2024-06-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.commit()
    conn.close()

    exit_code = main(["errors", "--db", db_path])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "network" in captured.out
    assert "boom" in captured.out


def test_errors_filters_by_category(db_path, capsys):
    wheel_id = _insert_wheel(db_path, filename="errwheel2-1.0-py3-none-any.whl")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO errors (wheel_id, error_category, details, reroll_version, created_at) "
        "VALUES (?, 'network', 'a', '1.0', '2024-06-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.execute(
        "INSERT INTO errors (wheel_id, error_category, details, reroll_version, created_at) "
        "VALUES (?, 'parse', 'b', '1.0', '2024-06-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.commit()
    conn.close()

    exit_code = main(["errors", "--db", db_path, "--category", "parse"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "parse" in captured.out
    assert "network" not in captured.out


def test_errors_filters_by_since(db_path, capsys):
    wheel_id = _insert_wheel(db_path, filename="errwheel3-1.0-py3-none-any.whl")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO errors (wheel_id, error_category, details, reroll_version, created_at) "
        "VALUES (?, 'old', 'x', '1.0', '2020-01-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.execute(
        "INSERT INTO errors (wheel_id, error_category, details, reroll_version, created_at) "
        "VALUES (?, 'new', 'y', '1.0', '2030-01-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.commit()
    conn.close()

    exit_code = main(["errors", "--db", db_path, "--since", "2025-01-01"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "new" in captured.out
    assert "old" not in captured.out


def test_errors_invalid_since_exits_1(db_path, capsys):
    exit_code = main(["errors", "--db", db_path, "--since", "not-a-date"])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "not-a-date" in captured.err


def test_errors_reports_nothing_matching(db_path, capsys):
    exit_code = main(["errors", "--db", db_path])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "no matching errors" in captured.out


def test_fsck_clean_database_exits_0(db_path, capsys):
    exit_code = main(["fsck", "--db", db_path])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "clean" in captured.out


def test_fsck_finds_a_violation_exits_2(db_path, capsys):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO wheels (filename, project, state, lane, url, serial, change_seq, "
        "updated_at, deleted_at) VALUES (?, ?, ?, 0, ?, 1, 1, '2024-01-01T00:00:00+00:00', "
        "'2024-01-01T00:00:00+00:00')",
        (
            "bad-1.0-py3-none-any.whl",
            "proj",
            int(WheelState.NEED_CONVERT),
            "https://example.test/x",
        ),
    )
    conn.commit()
    conn.close()

    exit_code = main(["fsck", "--db", db_path])
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "FAIL" in captured.out


def test_fsck_chunk_option_is_honored(db_path):
    exit_code = main(["fsck", "--db", db_path, "--chunk", "5"])
    assert exit_code == 0


def test_verify_archive_missing_segments_dir_exits_1(db_path, tmp_path):
    exit_code = main(["verify-archive", "--db", db_path, "--segments-dir", str(tmp_path / "nope")])
    assert exit_code == 1
    assert not (tmp_path / "nope").exists()


def test_verify_archive_clean_exits_0(db_path, tmp_path, capsys):
    from reroll_sync.archive.store import ArchiveStore
    from reroll_sync.db import connect_writer

    segments_dir = tmp_path / "segments"
    writer_conn = connect_writer(db_path)
    store = ArchiveStore(segments_dir, writer_conn)
    store.add(b"hello")
    store.seal_writer(store.current_writer())
    writer_conn.close()

    exit_code = main(["verify-archive", "--db", db_path, "--segments-dir", str(segments_dir)])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "clean" in captured.out


def test_verify_archive_finds_a_problem_exits_2(db_path, tmp_path, capsys):
    from reroll_sync.archive.store import ArchiveStore
    from reroll_sync.db import connect_writer

    segments_dir = tmp_path / "segments"
    writer_conn = connect_writer(db_path)
    store = ArchiveStore(segments_dir, writer_conn)
    store.add(b"hello")
    store.seal_writer(store.current_writer())
    writer_conn.execute("DELETE FROM blobs")
    writer_conn.commit()
    writer_conn.close()

    exit_code = main(["verify-archive", "--db", db_path, "--segments-dir", str(segments_dir)])
    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out.strip()


def test_verify_archive_segment_option_restricts_scope(db_path, tmp_path):
    from reroll_sync.archive.store import ArchiveStore
    from reroll_sync.db import connect_writer

    segments_dir = tmp_path / "segments"
    writer_conn = connect_writer(db_path)
    store = ArchiveStore(segments_dir, writer_conn)
    store.add(b"hello")
    store.seal_writer(store.current_writer())
    segment_id = store.sealed_segment_ids()[0]
    writer_conn.close()

    exit_code = main(
        [
            "verify-archive",
            "--db",
            db_path,
            "--segments-dir",
            str(segments_dir),
            "--segment",
            str(segment_id),
        ]
    )
    assert exit_code == 0


def test_verify_archive_does_not_corrupt_a_live_daemons_open_segment(db_path, tmp_path):
    """The whole point of `recover=False`: a second, CLI-owned `ArchiveStore`
    must not truncate a segment a live daemon still owns.
    """
    from reroll_sync.archive.store import ArchiveStore
    from reroll_sync.db import connect_writer

    segments_dir = tmp_path / "segments"
    writer_conn = connect_writer(db_path)
    live_store = ArchiveStore(segments_dir, writer_conn)
    location = live_store.add(b"still being written")

    main(["verify-archive", "--db", db_path, "--segments-dir", str(segments_dir)])

    live_store.seal_writer(live_store.current_writer())
    assert live_store.get(location.sha256) == b"still being written"
    writer_conn.close()


def test_queue_shows_all_stages_by_default(db_path, capsys):
    _insert_wheel(db_path, filename="q1-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA)
    exit_code = main(["queue", "--db", db_path])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "fetch" in captured.out
    assert "convert" in captured.out


def test_queue_stage_option_restricts_output(db_path, capsys):
    _insert_wheel(db_path, filename="q2-1.0-py3-none-any.whl", state=WheelState.NEED_METADATA)
    exit_code = main(["queue", "--db", db_path, "--stage", "fetch"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "fetch" in captured.out
    assert "convert" not in captured.out


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


def test_init_creates_database_and_exits_0(tmp_path, capsys):
    db_path = tmp_path / "reroll_sync.db"
    exit_code = main(["init", "--db", str(db_path), "--socket", str(tmp_path / "none.sock")])
    assert exit_code == 0
    assert db_path.exists()
    captured = capsys.readouterr()
    assert str(db_path) in captured.out


def test_init_is_idempotent(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    socket_arg = ["--socket", str(tmp_path / "none.sock")]
    assert main(["init", "--db", str(db_path), *socket_arg]) == 0
    assert main(["init", "--db", str(db_path), *socket_arg]) == 0


def test_init_reports_schema_mismatch(tmp_path, capsys):
    from reroll_sync.schema import SCHEMA_VERSION

    db_path = tmp_path / "bad.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.execute("CREATE TABLE pypi_index (name TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    exit_code = main(["init", "--db", str(db_path), "--socket", str(tmp_path / "none.sock")])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Schema mismatch" in captured.err
    assert "pypi_index" in captured.err


def test_init_refuses_when_a_live_socket_is_present(
    server, socket_dir, socket_path, tmp_path, capsys
):
    db_path = tmp_path / "reroll_sync.db"
    exit_code = main(["init", "--db", str(db_path), "--socket", socket_path])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert not db_path.exists()
    assert socket_path in captured.err


def test_init_proceeds_when_the_socket_file_is_stale(socket_dir, tmp_path):
    """A socket file left behind by a crashed daemon (bound, listened on,
    then closed -- nothing is accepting on it anymore) must not be
    mistaken for a live daemon: `_socket_is_live`'s real connect-with-
    timeout must distinguish this from an actually-live socket, and `init`
    must proceed normally rather than wrongly refusing.
    """
    stale_socket_path = socket_dir / "control.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(stale_socket_path))
    listener.listen(1)
    listener.close()  # closed: the bound socket file remains on disk, stale

    db_path = tmp_path / "reroll_sync.db"
    exit_code = main(["init", "--db", str(db_path), "--socket", str(stale_socket_path)])

    assert exit_code == 0
    assert db_path.exists()


def test_init_never_calls_connect_writer_outside_init_db(monkeypatch, tmp_path):
    # Confirms init's writable path is exclusively through db.init_db, not
    # some separate ad-hoc sqlite3.connect call.
    import reroll_sync.cli as cli_module

    assert not hasattr(cli_module, "connect_writer")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


class _FakeDaemon:
    instances: list[_FakeDaemon] = []

    def __init__(self, config) -> None:
        self.config = config
        self.install_signal_handlers_called = False
        self.run_forever_called = False
        _FakeDaemon.instances.append(self)

    def install_signal_handlers(self) -> None:
        self.install_signal_handlers_called = True

    def run_forever(self) -> None:
        self.run_forever_called = True


@pytest.fixture
def fake_daemon(monkeypatch):
    _FakeDaemon.instances = []
    monkeypatch.setattr("reroll_sync.cli.Daemon", _FakeDaemon)
    return _FakeDaemon


def test_run_builds_config_and_runs_the_daemon(monkeypatch, tmp_path, fake_daemon):
    monkeypatch.setenv("REROLL_SYNC_USER_AGENT", "test-agent (contact@example.invalid)")
    monkeypatch.setenv("REROLL_SYNC_DB_PATH", str(tmp_path / "reroll_sync.db"))
    monkeypatch.setenv("REROLL_SYNC_SOCKET_PATH", str(tmp_path / "reroll_sync.sock"))

    exit_code = main(["run", "--config-from-env"])

    assert exit_code == 0
    assert len(fake_daemon.instances) == 1
    assert fake_daemon.instances[0].install_signal_handlers_called
    assert fake_daemon.instances[0].run_forever_called


def test_run_missing_user_agent_exits_1(monkeypatch, capsys):
    monkeypatch.delenv("REROLL_SYNC_USER_AGENT", raising=False)
    exit_code = main(["run"])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "USER_AGENT" in captured.err


def test_run_refuses_when_a_live_socket_is_present(
    server, socket_dir, socket_path, monkeypatch, fake_daemon
):
    monkeypatch.setenv("REROLL_SYNC_USER_AGENT", "test-agent (contact@example.invalid)")
    monkeypatch.setenv("REROLL_SYNC_SOCKET_PATH", socket_path)

    exit_code = main(["run"])

    assert exit_code == 1
    assert fake_daemon.instances == []


# ---------------------------------------------------------------------------
# Mutating commands
# ---------------------------------------------------------------------------


def test_pause_sends_the_right_command_and_args(server, socket_path, fake):
    exit_code = main(["pause", "fetch", "--socket", socket_path])
    assert exit_code == 0
    assert fake.paused == ["fetch"]


def test_resume_sends_the_right_command_and_args(server, socket_path, fake):
    exit_code = main(["resume", "convert", "--socket", socket_path])
    assert exit_code == 0
    assert fake.resumed == ["convert"]


def test_drain_sends_the_right_command(server, socket_path, fake):
    exit_code = main(["drain", "--socket", socket_path])
    assert exit_code == 0
    assert fake.drained is True


def test_shutdown_sends_the_right_command(server, socket_path, fake):
    exit_code = main(["shutdown", "--socket", socket_path])
    assert exit_code == 0
    assert fake.shutdown_called is True


def test_unquarantine_all_maps_to_state_selector_quarantined(server, socket_path, fake):
    exit_code = main(["unquarantine", "--all", "--socket", socket_path])
    assert exit_code == 0
    assert fake.unquarantine_calls == [StateSelector(state=WheelState.QUARANTINED)]


def test_unquarantine_project_maps_to_project_selector(server, socket_path, fake):
    exit_code = main(["unquarantine", "--project", "numpy", "--socket", socket_path])
    assert exit_code == 0
    assert fake.unquarantine_calls == [ProjectSelector(project="numpy")]


def test_unquarantine_stage_maps_to_state_selector(server, socket_path, fake):
    exit_code = main(["unquarantine", "--stage", "QUARANTINED", "--socket", socket_path])
    assert exit_code == 0
    assert fake.unquarantine_calls == [StateSelector(state=WheelState.QUARANTINED)]


def test_unquarantine_requires_a_selector(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["unquarantine"])
    assert exc_info.value.code == 1


def test_unquarantine_mutually_exclusive_selectors_rejected(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["unquarantine", "--all", "--project", "numpy"])
    assert exc_info.value.code == 1


@pytest.mark.parametrize(
    ("command", "cli_args"),
    [
        ("pause", ["pause", "fetch"]),
        ("resume", ["resume", "fetch"]),
        ("drain", ["drain"]),
        ("shutdown", ["shutdown"]),
        ("unquarantine", ["unquarantine", "--all"]),
        (
            "reprocess",
            ["reprocess", "--project", "numpy"],
        ),
    ],
)
def test_mutating_command_with_no_daemon_exits_1_naming_socket(command, cli_args, tmp_path, capsys):
    missing_socket = tmp_path / "control.sock"
    exit_code = main([*cli_args, "--socket", str(missing_socket)])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert str(missing_socket) in captured.err


@pytest.mark.parametrize(
    "cli_args",
    [
        ["pause", "fetch"],
        ["resume", "fetch"],
        ["drain"],
        ["shutdown"],
        ["unquarantine", "--all"],
        ["reprocess", "--project", "numpy"],  # a real selector, not --dry-run: hits the socket
    ],
)
def test_mutating_command_with_socket_not_accepting_exits_1_bounded(
    cli_args, socket_dir, capsys, monkeypatch
):
    monkeypatch.setattr("reroll_sync.cli.CLI_CONNECT_TIMEOUT", 0.2)
    monkeypatch.setattr("reroll_sync.cli.CLI_READ_TIMEOUT", 0.2)
    socket_path_obj = socket_dir / "control.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path_obj))
    listener.listen(1)
    try:
        exit_code = main([*cli_args, "--socket", str(socket_path_obj)])
        captured = capsys.readouterr()
        assert exit_code == 1
        assert captured.err
    finally:
        listener.close()


def test_mutating_command_daemon_error_reply_is_surfaced_verbatim(
    server, socket_path, fake, capsys
):
    fake.raise_on_pause = RuntimeError("no such stage 'bogus'")
    exit_code = main(["pause", "bogus", "--socket", socket_path])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "no such stage 'bogus'" in captured.err


def test_cli_module_never_writes_directly_outside_init_and_run():
    """Grep guard: no mutating/read-only command handler touches sqlite3 or
    a writable connection directly; `_cmd_init`/`_cmd_verify_archive`
    (read-only, but constructs an `ArchiveStore` bound to the reader) and
    `_cmd_run` are the only places sqlite3-adjacent writable machinery is
    named at all.
    """
    text = Path(cli.__file__).read_text()
    assert "connect_writer" not in text
    assert "sqlite3.connect(" not in text


# ---------------------------------------------------------------------------
# reprocess
# ---------------------------------------------------------------------------


def test_reprocess_dry_run_writes_nothing_and_reports_counts(db_path, capsys):
    wheel_id = _insert_wheel(
        db_path, filename="dryrun-1.0-py3-none-any.whl", state=WheelState.SKIPPED
    )
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO skips (wheel_id, stage, reason, permanent, reroll_version, created_at) "
        "VALUES (?, 'convert', 'r', 0, '1.0.0', '2024-01-01T00:00:00+00:00')",
        (wheel_id,),
    )
    conn.commit()
    conn.close()

    exit_code = main(["reprocess", "--reroll-version-below", "2.0.0", "--dry-run", "--db", db_path])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "1 wheel(s)" in captured.out
    assert "1 skips row(s)" in captured.out
    reader = connect_reader(db_path)
    try:
        row = reader.execute("SELECT state FROM wheels WHERE id = ?", (wheel_id,)).fetchone()
        assert row[0] == int(WheelState.SKIPPED)  # unchanged
        (skips_count,) = reader.execute("SELECT COUNT(*) FROM skips").fetchone()
        assert skips_count == 1  # unchanged
    finally:
        reader.close()


def test_reprocess_dry_run_does_not_require_a_daemon(db_path, tmp_path):
    exit_code = main(
        [
            "reprocess",
            "--project",
            "numpy",
            "--dry-run",
            "--db",
            db_path,
            "--socket",
            str(tmp_path / "no-daemon-here.sock"),
        ]
    )
    assert exit_code == 0


def test_reprocess_without_dry_run_submits_over_the_socket(server, socket_path, fake):
    fake.reprocess_result = 42
    exit_code = main(["reprocess", "--project", "numpy", "--socket", socket_path])
    assert exit_code == 0
    assert fake.reprocess_calls == [ProjectSelector(project="numpy")]


def test_reprocess_reroll_version_below_selector_wire_format(server, socket_path, fake):
    main(["reprocess", "--reroll-version-below", "1.2.3", "--socket", socket_path])
    assert fake.reprocess_calls == [RerollVersionBelow(version="1.2.3")]


def test_reprocess_state_selector_wire_format(server, socket_path, fake):
    main(["reprocess", "--state", "SKIPPED", "--socket", socket_path])
    assert fake.reprocess_calls == [StateSelector(state=WheelState.SKIPPED)]


def test_reprocess_skipped_only_selector_wire_format(server, socket_path, fake):
    main(["reprocess", "--skipped-only", "--socket", socket_path])
    assert fake.reprocess_calls == [SkippedOnly()]


def test_reprocess_reports_affected_count(server, socket_path, fake, capsys):
    fake.reprocess_result = 7
    exit_code = main(["reprocess", "--skipped-only", "--socket", socket_path])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "7 wheel(s)" in captured.out


def test_reprocess_bare_command_is_rejected(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["reprocess"])
    assert exc_info.value.code == 1


def test_reprocess_mutually_exclusive_selectors_rejected(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["reprocess", "--project", "numpy", "--skipped-only"])
    assert exc_info.value.code == 1


def test_reprocess_no_daemon_without_dry_run_exits_1_naming_socket(db_path, tmp_path, capsys):
    missing_socket = tmp_path / "control.sock"
    exit_code = main(["reprocess", "--project", "numpy", "--socket", str(missing_socket)])
    captured = capsys.readouterr()
    assert exit_code == 1
    assert str(missing_socket) in captured.err


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def test_format_bytes_zero():
    assert cli._format_bytes(0) == "0 B"


def test_format_bytes_just_below_a_kilobyte():
    assert cli._format_bytes(1023) == "1023 B"


def test_format_bytes_exactly_a_kilobyte():
    assert cli._format_bytes(1024) == "1.0 KB"


def test_format_bytes_multi_terabyte():
    value = int(2.5 * 1024**4)
    assert cli._format_bytes(value) == "2.5 TB"


def test_format_bytes_beyond_the_largest_unit_still_formats(monkeypatch):
    monkeypatch.setattr(cli, "_BYTE_UNITS", ("KB",))
    assert cli._format_bytes(1024 * 2000) == "2000.0 KB"


def test_format_duration_under_a_minute():
    assert cli._format_duration(45) == "45s"


def test_format_duration_under_an_hour():
    assert cli._format_duration(3 * 60 + 12) == "3m12s"


def test_format_duration_an_hour_or_more():
    assert cli._format_duration(3600 + 14 * 60) == "1h14m"


def test_format_status_shows_real_rate_when_limiter_children_present(db_path):
    """`_format_status` itself must render real rate-limiter figures when
    given them -- the "not available" wording in
    `test_status_rate_section_states_not_available_without_a_live_daemon`
    is specific to the CLI's `status` path never supplying a `limiter`,
    not a limitation of the formatter.
    """
    import dataclasses

    from reroll_sync import health as health_module
    from reroll_sync.ratelimit import ChildLimiterSnapshot

    reader = connect_reader(db_path)
    try:
        base = health_module.snapshot(reader, stages=cli._empty_stage_inputs())
    finally:
        reader.close()
    snap = ChildLimiterSnapshot(available=12.0, acquired=3, denied=0, penalty_deadline=0.0)
    health = dataclasses.replace(base, limiter_children={"pypi.org": snap})

    out = cli._format_status(health, ())

    assert "RATE            pypi.org 12" in out
    assert "not available" not in out


def test_format_status_shows_archive_age_when_present(db_path):
    import dataclasses

    from reroll_sync import health as health_module

    reader = connect_reader(db_path)
    try:
        base = health_module.snapshot(reader, stages=cli._empty_stage_inputs())
    finally:
        reader.close()
    health = dataclasses.replace(base, open_segment_age_seconds=3600.0 + 14 * 60)

    out = cli._format_status(health, ())

    assert "age 1h14m" in out


def test_human_status_includes_every_nonzero_state_count(db_path, capsys):
    _insert_wheel(db_path, filename="ns1-1.0-py3-none-any.whl", state=WheelState.READY)
    _insert_wheel(db_path, filename="ns2-1.0-py3-none-any.whl", state=WheelState.SKIPPED)
    _insert_wheel(db_path, filename="ns3-1.0-py3-none-any.whl", state=WheelState.QUARANTINED)

    main(["status", "--db", db_path])
    captured = capsys.readouterr()

    assert "ready 1" in captured.out
    assert "skipped 1" in captured.out
    assert "quarantined 1" in captured.out
    assert "need_metadata" not in captured.out  # zero count: omitted


def test_human_and_json_status_derive_from_the_same_snapshot(db_path, monkeypatch, capsys):
    from reroll_sync import health as health_module

    reader = connect_reader(db_path)
    try:
        injected = health_module.snapshot(reader, stages=cli._empty_stage_inputs())
    finally:
        reader.close()
    monkeypatch.setattr("reroll_sync.cli.snapshot", lambda *a, **k: injected)

    main(["status", "--db", db_path])
    human_out = capsys.readouterr().out
    main(["status", "--db", db_path, "--json"])
    json_out = capsys.readouterr().out

    assert str(injected.index_lag) in human_out
    parsed = json.loads(json_out)
    assert parsed["index_lag"] == injected.index_lag
    assert parsed["db_bytes"] == injected.db_bytes


def test_status_rate_section_states_not_available_without_a_live_daemon(db_path, capsys):
    """`status` never has a live `limiter`/`breakers` to hand `snapshot()`
    (those exist only in a running daemon's process memory), so RATE must
    say so honestly instead of printing a bare `none` that reads like a
    real (zero) measurement.
    """
    exit_code = main(["status", "--db", db_path])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "RATE" in captured.out
    assert "not available" in captured.out


def test_status_human_output_shows_multiple_simultaneous_alarms(db_path, monkeypatch, capsys):
    """Both of two simultaneously-firing alarms must appear in the
    human-readable output, not just whichever determines the exit code.
    """
    from reroll_sync import health as health_module

    injected_alarms = (
        health_module.Alarm(severity="critical", condition="wal_bytes", message="wal too big"),
        health_module.Alarm(
            severity="warning", condition="quarantined_count", message="wheels quarantined"
        ),
    )
    monkeypatch.setattr("reroll_sync.cli.alarms", lambda health, **k: injected_alarms)

    exit_code = main(["status", "--db", db_path])
    captured = capsys.readouterr()

    assert exit_code == 3
    assert "wal too big" in captured.out
    assert "wheels quarantined" in captured.out
