"""Command-line interface for reroll-sync.

Usage:
    reroll-sync init [db_path]
    reroll-sync sync-index [db_path] [--timeout SECONDS] [--limit N]
    reroll-sync sync-metadata [db_path] [--timeout SECONDS] [--limit N]
    reroll-sync parse-metadata [db_path] [--timeout SECONDS] [--limit N]
    reroll-sync sync-reroll [db_path] [--timeout SECONDS] [--limit N]
    reroll-sync stats [db_path]

``sync-metadata`` and ``parse-metadata`` read/write Cloudflare R2 and require
the environment variables ``R2_ACCOUNT_ID``, ``R2_ACCESS_KEY_ID``,
``R2_SECRET_ACCESS_KEY``, and ``R2_BUCKET`` to be set.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from typing import NoReturn

from .db import SchemaMismatchError, connect, init_db
from .r2_client import R2Config, R2ConfigError, r2_config_from_env

DEFAULT_DB_PATH = "reroll_sync.db"


def _parse_metadata(
    conn: sqlite3.Connection,
    r2_config: R2Config,
    *,
    timeout: float | None = None,
    limit: int | None = None,
) -> NoReturn:
    raise NotImplementedError("parse-metadata is not yet implemented")


def _sync_reroll(
    conn: sqlite3.Connection,
    *,
    timeout: float | None = None,
    limit: int | None = None,
) -> NoReturn:
    raise NotImplementedError("sync-reroll is not yet implemented")


def _compute_stats(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (projects_indexed, wheels_synced) row counts.

    A minimal, self-contained replacement for the now-deleted ``stats.py``
    (its two counts survive as fields of ``health.snapshot()``); kept here
    only so the ``stats`` command's import chain doesn't break, pending
    this module's full rewrite in spec 12.
    """
    (projects_indexed,) = conn.execute("SELECT COUNT(*) FROM pypi_index").fetchone()
    (wheels_synced,) = conn.execute("SELECT COUNT(*) FROM wheels").fetchone()
    return projects_indexed, wheels_synced


def _sync_index(
    conn: sqlite3.Connection,
    *,
    timeout: float | None = None,
    limit: int | None = None,
) -> NoReturn:
    raise NotImplementedError(
        "sync-index is not yet implemented against the spec 08 ingestion module; "
        "see reroll_sync.ingest"
    )


def _sync_metadata(
    conn: sqlite3.Connection,
    r2_config: R2Config,
    *,
    timeout: float | None = None,
    limit: int | None = None,
) -> NoReturn:
    raise NotImplementedError(
        "sync-metadata is not yet implemented against the spec 09 fetch stage; "
        "see reroll_sync.fetch"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reroll-sync",
        description="Keep reroll in sync with a pypi index url.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser(
        "init",
        help="Create the sqlite database and tables if missing, "
        "or verify the schema of an existing database.",
    )
    init_parser.add_argument(
        "db_path",
        nargs="?",
        default=DEFAULT_DB_PATH,
        help=f"Path to the sqlite database file (default: {DEFAULT_DB_PATH})",
    )

    sync_parser = subparsers.add_parser(
        "sync-index",
        help="Sync the pypi_index and wheels tables against the PyPI simple index.",
    )
    sync_parser.add_argument(
        "db_path",
        nargs="?",
        default=DEFAULT_DB_PATH,
        help=f"Path to the sqlite database file (default: {DEFAULT_DB_PATH})",
    )
    sync_parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Max seconds to spend syncing, and the per-request network timeout "
        "(default: no limit).",
    )
    sync_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of outdated projects to process in this run (default: no limit).",
    )

    stats_parser = subparsers.add_parser(
        "stats",
        help="Show summary counts of the current state of the database.",
    )
    stats_parser.add_argument(
        "db_path",
        nargs="?",
        default=DEFAULT_DB_PATH,
        help=f"Path to the sqlite database file (default: {DEFAULT_DB_PATH})",
    )

    metadata_parser = subparsers.add_parser(
        "sync-metadata",
        help="Download each wheel's .metadata sidecar and upload it to R2, keyed by rowid.",
    )
    metadata_parser.add_argument(
        "db_path",
        nargs="?",
        default=DEFAULT_DB_PATH,
        help=f"Path to the sqlite database file (default: {DEFAULT_DB_PATH})",
    )
    metadata_parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Max seconds to spend syncing, and the per-request network timeout "
        "(default: no limit).",
    )
    metadata_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of pending wheels to process in this run (default: no limit).",
    )

    parse_parser = subparsers.add_parser(
        "parse-metadata",
        help="Parse each wheel's downloaded METADATA (from R2) with reroll.",
    )
    parse_parser.add_argument(
        "db_path",
        nargs="?",
        default=DEFAULT_DB_PATH,
        help=f"Path to the sqlite database file (default: {DEFAULT_DB_PATH})",
    )
    parse_parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Max seconds to spend parsing in this run (default: no limit).",
    )
    parse_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of pending wheels to process in this run (default: no limit).",
    )

    reroll_parser = subparsers.add_parser(
        "sync-reroll",
        help="Convert each wheel's parsed METADATA into repodata record(s) with reroll.",
    )
    reroll_parser.add_argument(
        "db_path",
        nargs="?",
        default=DEFAULT_DB_PATH,
        help=f"Path to the sqlite database file (default: {DEFAULT_DB_PATH})",
    )
    reroll_parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Max seconds to spend converting in this run (default: no limit).",
    )
    reroll_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of pending wheels to process in this run (default: no limit).",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        init_db(args.db_path)
    except SchemaMismatchError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.command == "init":
        print(f"Initialized database at '{args.db_path}'")
        return 0

    if args.command == "stats":
        conn = connect(args.db_path)
        try:
            projects_indexed, wheels_synced = _compute_stats(conn)
        finally:
            conn.close()
        print(f"{projects_indexed} project(s) indexed, {wheels_synced} wheel(s) synced")
        return 0

    if args.command == "sync-metadata":
        try:
            r2_config = r2_config_from_env()
        except R2ConfigError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        conn = connect(args.db_path)
        try:
            metadata_stats = _sync_metadata(conn, r2_config, timeout=args.timeout, limit=args.limit)
        finally:
            conn.close()
        print(
            f"Uploaded {metadata_stats.wheels_uploaded}/{metadata_stats.wheels_considered} "
            f"wheel metadata file(s) to R2 "
            f"({metadata_stats.wheels_skipped_no_metadata} skipped, "
            f"{metadata_stats.wheels_failed_hash_mismatch} hash mismatch(es))"
            + (" (stopped early: timeout reached)" if metadata_stats.stopped_early else "")
        )
        return 0

    if args.command == "parse-metadata":
        try:
            r2_config = r2_config_from_env()
        except R2ConfigError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        conn = connect(args.db_path)
        try:
            parse_stats = _parse_metadata(conn, r2_config, timeout=args.timeout, limit=args.limit)
        finally:
            conn.close()
        print(
            f"Parsed {parse_stats.wheels_parsed}/{parse_stats.wheels_considered} "
            f"wheel metadata file(s) ({parse_stats.wheels_failed} failed)"
            + (" (stopped early: timeout reached)" if parse_stats.stopped_early else "")
        )
        return 0

    if args.command == "sync-reroll":
        conn = connect(args.db_path)
        try:
            reroll_stats = _sync_reroll(conn, timeout=args.timeout, limit=args.limit)
        finally:
            conn.close()
        print(
            f"Converted {reroll_stats.wheels_converted}/{reroll_stats.wheels_considered} "
            f"wheel(s) to repodata ({reroll_stats.wheels_failed} failed)"
            + (" (stopped early: timeout reached)" if reroll_stats.stopped_early else "")
        )
        return 0

    conn = connect(args.db_path)
    try:
        _sync_index(conn, timeout=args.timeout, limit=args.limit)
    finally:
        conn.close()
    return 0
