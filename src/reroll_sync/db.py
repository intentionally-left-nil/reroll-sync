"""Connection setup and schema validation for the reroll-sync sqlite database.

``connect_writer`` is the single runtime writer, plus offline bulk tools.
``connect_reader`` is read-only, for CLI introspection and any read path
outside the writer thread. ``init_db`` creates a fresh database (setting
``auto_vacuum`` before the first table, per ``docs/db.md``) or, for an
existing one, validates every table against ``schema.SCHEMA`` without ever
altering it; mismatches raise :class:`SchemaMismatchError`.

There is no migration framework. Until there is real production data, the
only supported path for a schema change is to drop and recreate the
database.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from .schema import SCHEMA, SCHEMA_VERSION, Table

_EXPECTED_AUTO_VACUUM = 2  # INCREMENTAL


class SchemaMismatchError(Exception):
    """Raised when an existing table's schema does not match the expected one."""

    def __init__(self, table_name: str, problems: list[str]):
        self.table_name = table_name
        self.problems = problems
        message = f"Schema mismatch for table '{table_name}':\n" + "\n".join(
            f"  - {problem}" for problem in problems
        )
        super().__init__(message)


class SchemaVersionError(Exception):
    """Raised when an existing database's ``user_version`` is not the expected one."""

    def __init__(self, found: int, expected: int = SCHEMA_VERSION):
        self.found = found
        self.expected = expected
        super().__init__(f"database reports user_version = {found}, expected {expected}")


class AutoVacuumError(Exception):
    """Raised when an existing database was not created with ``auto_vacuum = INCREMENTAL``.

    ``auto_vacuum`` cannot be changed without a full ``VACUUM`` (double the
    disk, a long exclusive lock), so a mismatched database must be dropped
    and recreated rather than fixed in place.
    """

    def __init__(self, found: int):
        self.found = found
        super().__init__(
            f"database reports auto_vacuum = {found}, expected {_EXPECTED_AUTO_VACUUM} "
            "(INCREMENTAL)"
        )


def connect_writer(db_path: str | Path) -> sqlite3.Connection:
    """Open the single runtime writer connection, creating the file if needed.

    Uses ``check_same_thread=False``: the returned connection is constructed
    on the caller's thread but is meant to be handed to a :class:`Writer`
    (spec 06), which consumes it exclusively from its own background thread.
    """
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA cache_size = -262144")
    conn.execute("PRAGMA mmap_size = 4294967296")
    conn.execute("PRAGMA wal_autocheckpoint = 1000")
    return conn


def connect_reader(db_path: str | Path) -> sqlite3.Connection:
    """Open a read-only connection. Raises if ``db_path`` does not exist."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA cache_size = -262144")
    conn.execute("PRAGMA mmap_size = 4294967296")
    conn.execute("PRAGMA query_only = ON")
    return conn


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Back-compat alias for :func:`connect_writer`.

    Kept only so the pre-rewrite ``cli.py`` (and the modules it imports)
    still import successfully; a later spec that rewrites the CLI onto
    ``connect_writer``/``connect_reader`` should remove this.
    """
    return connect_writer(db_path)


def init_db(db_path: str | Path) -> None:
    """Create the database and its tables if missing, or validate them if present.

    For a brand-new file: set ``auto_vacuum`` and ``user_version`` before
    creating any table, then create every table in :data:`schema.SCHEMA`.

    For an existing file: verify ``auto_vacuum`` and ``user_version`` match
    what this database version requires, then for each table either create
    it (if missing) or validate it against the expected shape, raising
    :class:`SchemaMismatchError` on the first mismatch. Existing tables are
    never altered.
    """
    is_new = not Path(db_path).exists()
    conn = sqlite3.connect(str(db_path))
    try:
        if is_new:
            conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        else:
            _check_auto_vacuum(conn)
            _check_user_version(conn)

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


def _check_auto_vacuum(conn: sqlite3.Connection) -> None:
    (value,) = conn.execute("PRAGMA auto_vacuum").fetchone()
    if value != _EXPECTED_AUTO_VACUUM:
        raise AutoVacuumError(value)


def _check_user_version(conn: sqlite3.Connection) -> None:
    (value,) = conn.execute("PRAGMA user_version").fetchone()
    if value != SCHEMA_VERSION:
        raise SchemaVersionError(value)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _existing_columns(conn: sqlite3.Connection, table_name: str) -> dict[str, dict]:
    """Return {column_name: {type, notnull, pk}} for the given table.

    ``pk`` is the 1-based position of the column within the table's primary
    key (0 if the column is not part of it), matching ``PRAGMA table_info``.
    """
    columns: dict[str, dict] = {}
    for row in conn.execute(f"PRAGMA table_info({table_name})"):
        # cid, name, type, notnull, dflt_value, pk
        _cid, name, col_type, notnull, _dflt_value, pk = row
        columns[name] = {
            "type": col_type,
            "notnull": bool(notnull),
            "pk": pk,
        }
    return columns


def _existing_primary_key(existing_cols: dict[str, dict]) -> tuple[str, ...]:
    pk_cols = [(info["pk"], name) for name, info in existing_cols.items() if info["pk"] > 0]
    return tuple(name for _rank, name in sorted(pk_cols))


def _existing_unique_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """Return column names with a single-column UNIQUE constraint (not the PK)."""
    unique_columns: set[str] = set()
    for row in conn.execute(f"PRAGMA index_list({table_name})"):
        # seq, name, unique, origin, partial
        _seq, name, _unique, origin, _partial = row
        if origin != "u":
            continue
        cols = [info_row[2] for info_row in conn.execute(f"PRAGMA index_info({name})")]
        if len(cols) == 1:
            unique_columns.add(cols[0])
    return unique_columns


def _existing_foreign_keys(conn: sqlite3.Connection, table_name: str) -> set[tuple]:
    """Return set of (from_column, to_table, to_column) for the given table."""
    fks = set()
    for row in conn.execute(f"PRAGMA foreign_key_list({table_name})"):
        # id, seq, table, from, to, on_update, on_delete, match
        _id, _seq, ref_table, from_col, to_col, *_rest = row
        fks.add((from_col, ref_table, to_col))
    return fks


def _existing_indexes(conn: sqlite3.Connection, table_name: str) -> dict[str, dict]:
    """Return {index_name: {columns, where}} for named (CREATE INDEX) indexes.

    Indexes with origin ``pk`` (backing a primary key) or ``u`` (backing a
    column-level UNIQUE constraint) are implicit, not ones we declared via
    ``CREATE INDEX``, and are excluded.
    """
    indexes: dict[str, dict] = {}
    for row in conn.execute(f"PRAGMA index_list({table_name})"):
        # seq, name, unique, origin, partial
        _seq, name, _unique, origin, _partial = row
        if origin != "c":
            continue
        cols = [info_row[2] for info_row in conn.execute(f"PRAGMA index_info({name})")]
        (index_sql,) = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?", (name,)
        ).fetchone()
        indexes[name] = {"columns": cols, "where": _extract_where_clause(index_sql)}
    return indexes


def _extract_where_clause(index_sql: str) -> str | None:
    match = re.search(r"\bWHERE\b(.*)$", index_sql, re.IGNORECASE | re.DOTALL)
    if match is None:
        return None
    return _normalize_where(match.group(1))


def _normalize_where(where: str | None) -> str | None:
    if where is None:
        return None
    return " ".join(where.split())


def _validate_table(conn: sqlite3.Connection, table: Table) -> list[str]:
    """Return a list of human-readable problems, empty if schema matches."""
    problems: list[str] = []

    existing_cols = _existing_columns(conn, table.name)
    expected_names = {col.name for col in table.columns}
    existing_names = set(existing_cols)

    for name in sorted(expected_names - existing_names):
        problems.append(f"missing column '{name}'")
    for name in sorted(existing_names - expected_names):
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

    actual_pk = _existing_primary_key(existing_cols)
    if actual_pk != table.primary_key:
        problems.append(
            f"primary key mismatch: expected {list(table.primary_key)}, found {list(actual_pk)}"
        )

    expected_unique = {col.name for col in table.columns if col.unique}
    actual_unique = _existing_unique_columns(conn, table.name)
    for col in table.columns:
        if col.name not in existing_cols:
            continue
        expected = col.name in expected_unique
        actual = col.name in actual_unique
        if expected != actual:
            problems.append(
                f"column '{col.name}' UNIQUE mismatch: expected {expected}, found {actual}"
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
        expected_where = _normalize_where(idx.where)
        if actual_idx["where"] != expected_where:
            problems.append(
                f"index '{idx.name}' WHERE clause mismatch: "
                f"expected {expected_where!r}, found {actual_idx['where']!r}"
            )

    return problems
