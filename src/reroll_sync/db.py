"""SQLite database setup and schema validation for reroll-sync.

Provides two operations used by the ``init`` CLI command:

* create the database file and all tables/indexes if they do not exist yet
* if a table already exists, verify its actual schema (columns, types,
  nullability, primary key, foreign keys, indexes) matches the expected
  schema exactly. Existing tables are never altered -- a mismatch raises
  :class:`SchemaMismatchError` instead.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .schema import SCHEMA, Table


class SchemaMismatchError(Exception):
    """Raised when an existing table's schema does not match the expected one."""

    def __init__(self, table_name: str, problems: list[str]):
        self.table_name = table_name
        self.problems = problems
        message = f"Schema mismatch for table '{table_name}':\n" + "\n".join(
            f"  - {problem}" for problem in problems
        )
        super().__init__(message)


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open a connection to the sqlite database, creating the file if needed."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _existing_columns(conn: sqlite3.Connection, table_name: str) -> dict[str, dict]:
    """Return {column_name: {type, notnull, pk}} for the given table."""
    columns: dict[str, dict] = {}
    for row in conn.execute(f"PRAGMA table_info({table_name})"):
        # cid, name, type, notnull, dflt_value, pk
        _cid, name, col_type, notnull, _dflt_value, pk = row
        columns[name] = {
            "type": col_type,
            "notnull": bool(notnull),
            "pk": pk > 0,
        }
    return columns


def _existing_foreign_keys(conn: sqlite3.Connection, table_name: str) -> set[tuple]:
    """Return set of (from_column, to_table, to_column) for the given table."""
    fks = set()
    for row in conn.execute(f"PRAGMA foreign_key_list({table_name})"):
        # id, seq, table, from, to, on_update, on_delete, match
        _id, _seq, ref_table, from_col, to_col, *_rest = row
        fks.add((from_col, ref_table, to_col))
    return fks


def _existing_indexes(conn: sqlite3.Connection, table_name: str) -> dict[str, dict]:
    """Return {index_name: {unique, columns}} for named (non-autoindex) indexes."""
    indexes: dict[str, dict] = {}
    for row in conn.execute(f"PRAGMA index_list({table_name})"):
        # seq, name, unique, origin, partial
        _seq, name, unique, origin, _partial = row
        if origin == "pk":
            # implicit index backing the primary key; not one we declared
            continue
        cols = [
            info_row[2]  # name column of index_info row (seqno, cid, name)
            for info_row in conn.execute(f"PRAGMA index_info({name})")
        ]
        indexes[name] = {"unique": bool(unique), "columns": cols}
    return indexes


def _validate_table(conn: sqlite3.Connection, table: Table) -> list[str]:
    """Return a list of human-readable problems, empty if schema matches."""
    problems: list[str] = []

    existing_cols = _existing_columns(conn, table.name)
    expected_names = {col.name for col in table.columns}
    existing_names = set(existing_cols)

    missing = expected_names - existing_names
    extra = existing_names - expected_names
    for name in sorted(missing):
        problems.append(f"missing column '{name}'")
    for name in sorted(extra):
        problems.append(f"unexpected column '{name}'")

    for col in table.columns:
        if col.name not in existing_cols:
            continue
        actual = existing_cols[col.name]
        if actual["type"].upper() != col.type.upper():
            problems.append(
                f"column '{col.name}' type mismatch: expected {col.type}, found {actual['type']}"
            )
        if actual["notnull"] != col.not_null:
            problems.append(
                f"column '{col.name}' NOT NULL mismatch: "
                f"expected {col.not_null}, found {actual['notnull']}"
            )
        if actual["pk"] != col.primary_key:
            problems.append(
                f"column '{col.name}' PRIMARY KEY mismatch: "
                f"expected {col.primary_key}, found {actual['pk']}"
            )

    expected_fks = {
        (col.name, col.references.ref_table, col.references.ref_column)
        for col in table.columns
        if col.references is not None
    }
    actual_fks = _existing_foreign_keys(conn, table.name)
    for fk in sorted(expected_fks - actual_fks):
        problems.append(f"missing foreign key {fk[0]} -> {fk[1]}.{fk[2]}")
    for fk in sorted(actual_fks - expected_fks):
        problems.append(f"unexpected foreign key {fk[0]} -> {fk[1]}.{fk[2]}")

    existing_indexes = _existing_indexes(conn, table.name)
    expected_index_names = {idx.name for idx in table.indexes}
    existing_index_names = set(existing_indexes)

    for name in sorted(expected_index_names - existing_index_names):
        problems.append(f"missing index '{name}'")
    for name in sorted(existing_index_names - expected_index_names):
        problems.append(f"unexpected index '{name}'")

    for idx in table.indexes:
        if idx.name not in existing_indexes:
            continue
        actual_idx = existing_indexes[idx.name]
        if tuple(actual_idx["columns"]) != tuple(idx.columns):
            problems.append(
                f"index '{idx.name}' columns mismatch: "
                f"expected {list(idx.columns)}, found {actual_idx['columns']}"
            )
        if actual_idx["unique"] != idx.unique:
            problems.append(
                f"index '{idx.name}' UNIQUE mismatch: "
                f"expected {idx.unique}, found {actual_idx['unique']}"
            )

    return problems


def init_db(db_path: str | Path) -> None:
    """Create the database/tables if missing, or validate them if present.

    For each table in the schema:

    * if it doesn't exist, create it (and its indexes)
    * if it does exist, validate its schema matches exactly; raise
      :class:`SchemaMismatchError` if not. Existing tables are never altered.
    """
    conn = connect(db_path)
    try:
        for table in SCHEMA:
            if _table_exists(conn, table.name):
                problems = _validate_table(conn, table)
                if problems:
                    raise SchemaMismatchError(table.name, problems)
            else:
                conn.execute(table.create_table_sql())
                for index_sql in table.create_index_sql():
                    conn.execute(index_sql)
        conn.commit()
    finally:
        conn.close()
