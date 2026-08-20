"""Command-line interface for reroll-sync.

Usage:
    reroll-sync init [db_path]
    reroll-sync sync-index [db_path] [--timeout SECONDS] [--limit N]
"""

from __future__ import annotations

import argparse
import sys

from .db import SchemaMismatchError, connect, init_db
from .sync import sync_index

DEFAULT_DB_PATH = "reroll_sync.db"


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

    conn = connect(args.db_path)
    try:
        stats = sync_index(conn, timeout=args.timeout, limit=args.limit)
    finally:
        conn.close()
    print(
        f"Synced {stats.projects_updated}/{stats.projects_outdated} outdated "
        f"project(s), inserted {stats.wheels_inserted} wheel(s)"
        + (" (stopped early: timeout reached)" if stats.stopped_early else "")
    )
    return 0
