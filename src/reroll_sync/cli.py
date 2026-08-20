"""Command-line interface for reroll-sync.

Usage:
    reroll-sync init [db_path]
"""

from __future__ import annotations

import argparse
import sys

from .db import SchemaMismatchError, init_db

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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        init_db(args.db_path)
    except SchemaMismatchError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"Initialized database at '{args.db_path}'")
    return 0
