import sqlite3

import pytest

from reroll_sync.db import SchemaMismatchError, init_db
from reroll_sync.schema import SCHEMA


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row[0] for row in rows}


def test_init_creates_new_database_with_all_tables(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    assert not db_path.exists()

    init_db(db_path)

    assert db_path.exists()
    conn = sqlite3.connect(str(db_path))
    try:
        assert _table_names(conn) == {t.name for t in SCHEMA}
    finally:
        conn.close()


def test_init_creates_expected_columns_for_each_table(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        for table in SCHEMA:
            cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table.name})")}
            expected = {c.name for c in table.columns}
            assert cols == expected, f"table {table.name}"
    finally:
        conn.close()


def test_init_creates_expected_indexes(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        for table in SCHEMA:
            index_rows = conn.execute(f"PRAGMA index_list({table.name})").fetchall()
            names = {row[1] for row in index_rows}
            for idx in table.indexes:
                assert idx.name in names
    finally:
        conn.close()


def test_init_is_idempotent_on_matching_existing_database(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    # Second call against the same (already correct) database should not
    # raise and should not alter anything.
    init_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        assert _table_names(conn) == {t.name for t in SCHEMA}
    finally:
        conn.close()


def test_init_uses_default_path_argument(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    init_db("reroll_sync.db")
    assert (tmp_path / "reroll_sync.db").exists()


def test_init_fails_on_missing_column(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT PRIMARY KEY NOT NULL, serial INTEGER NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("updated_at" in p for p in exc_info.value.problems)


def test_init_fails_on_missing_primary_key_not_null(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT PRIMARY KEY, serial INTEGER NOT NULL, "
        "updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("name" in p and "NOT NULL" in p for p in exc_info.value.problems)


def test_init_fails_on_missing_name_conversions_column(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE wheels ("
        "filename TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, conda_name TEXT, "
        "pypi_simple TEXT, skip_reason TEXT, metadata_downloaded_at TEXT, "
        "wheel_metadata TEXT, metadata_reroll_version TEXT, repodata TEXT, "
        "repodata_reroll_version TEXT, updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("missing column 'name_conversions'" in p for p in exc_info.value.problems)


def test_init_fails_on_missing_conda_name_column(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE wheels ("
        "filename TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, pypi_simple TEXT, "
        "skip_reason TEXT, metadata_downloaded_at TEXT, wheel_metadata TEXT, "
        "metadata_reroll_version TEXT, repodata TEXT, name_conversions TEXT, "
        "repodata_reroll_version TEXT, updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("missing column 'conda_name'" in p for p in exc_info.value.problems)


def test_init_fails_on_unexpected_column(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE pypi_index ("
        "name TEXT PRIMARY KEY NOT NULL, serial INTEGER NOT NULL, "
        "updated_at TEXT NOT NULL, extra_column TEXT)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert any("extra_column" in p for p in exc_info.value.problems)


def test_init_fails_on_wrong_column_type(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE pypi_index ("
        "name TEXT PRIMARY KEY NOT NULL, serial TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert any("serial" in p and "type mismatch" in p for p in exc_info.value.problems)


def test_init_fails_on_wrong_not_null(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT PRIMARY KEY NOT NULL, serial INTEGER, "
        "updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert any("serial" in p and "NOT NULL" in p for p in exc_info.value.problems)


def test_init_fails_on_missing_index(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE wheels ("
        "filename TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, conda_name TEXT, "
        "pypi_simple TEXT, "
        "skip_reason TEXT, metadata_downloaded_at TEXT, wheel_metadata TEXT, "
        "metadata_reroll_version TEXT, repodata TEXT, name_conversions TEXT, "
        "repodata_reroll_version TEXT, updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("ix_wheels_project" in p for p in exc_info.value.problems)


def test_init_fails_on_missing_foreign_key(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE wheels (
            filename TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, conda_name TEXT,
            pypi_simple TEXT,
            skip_reason TEXT, metadata_downloaded_at TEXT, wheel_metadata TEXT,
            metadata_reroll_version TEXT, repodata TEXT, name_conversions TEXT,
            repodata_reroll_version TEXT, updated_at TEXT NOT NULL
        );
        CREATE INDEX ix_wheels_project ON wheels (project);
        CREATE INDEX ix_wheels_conda_name ON wheels (conda_name);
        CREATE TABLE errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wheel_filename TEXT NOT NULL,
            error_category TEXT NOT NULL,
            error_subcategory TEXT,
            details TEXT,
            reroll_version TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX ix_errors_wheel_filename ON errors (wheel_filename);
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "errors"
    assert any("foreign key" in p for p in exc_info.value.problems)


def test_init_fails_on_missing_reroll_version_not_null(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE wheels (
            filename TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, conda_name TEXT,
            pypi_simple TEXT,
            skip_reason TEXT, metadata_downloaded_at TEXT, wheel_metadata TEXT,
            metadata_reroll_version TEXT, repodata TEXT, name_conversions TEXT,
            repodata_reroll_version TEXT, updated_at TEXT NOT NULL
        );
        CREATE INDEX ix_wheels_project ON wheels (project);
        CREATE INDEX ix_wheels_conda_name ON wheels (conda_name);
        CREATE TABLE errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wheel_filename TEXT NOT NULL REFERENCES wheels(filename),
            error_category TEXT NOT NULL,
            error_subcategory TEXT,
            details TEXT,
            reroll_version TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX ix_errors_wheel_filename ON errors (wheel_filename);
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "errors"
    assert any("reroll_version" in p and "NOT NULL" in p for p in exc_info.value.problems)


def test_init_fails_on_missing_wheels_filename_not_null(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE wheels (
            filename TEXT PRIMARY KEY, project TEXT NOT NULL, conda_name TEXT,
            pypi_simple TEXT,
            skip_reason TEXT, metadata_downloaded_at TEXT, wheel_metadata TEXT,
            metadata_reroll_version TEXT, repodata TEXT, name_conversions TEXT,
            repodata_reroll_version TEXT, updated_at TEXT NOT NULL
        );
        CREATE INDEX ix_wheels_project ON wheels (project);
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("filename" in p and "NOT NULL" in p for p in exc_info.value.problems)


def test_init_fails_on_unexpected_foreign_key(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE pypi_index ("
        "name TEXT PRIMARY KEY NOT NULL REFERENCES wheels(filename), "
        "serial INTEGER NOT NULL, updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("unexpected foreign key" in p for p in exc_info.value.problems)


def test_init_fails_on_unexpected_index(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE wheels (
            filename TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, conda_name TEXT,
            pypi_simple TEXT,
            skip_reason TEXT, metadata_downloaded_at TEXT, wheel_metadata TEXT,
            metadata_reroll_version TEXT, repodata TEXT, name_conversions TEXT,
            repodata_reroll_version TEXT, updated_at TEXT NOT NULL
        );
        CREATE INDEX ix_wheels_project ON wheels (project);
        CREATE INDEX ix_wheels_extra ON wheels (skip_reason);
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("unexpected index 'ix_wheels_extra'" in p for p in exc_info.value.problems)


def test_init_fails_on_index_column_mismatch(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE wheels (
            filename TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, conda_name TEXT,
            pypi_simple TEXT,
            skip_reason TEXT, metadata_downloaded_at TEXT, wheel_metadata TEXT,
            metadata_reroll_version TEXT, repodata TEXT, name_conversions TEXT,
            repodata_reroll_version TEXT, updated_at TEXT NOT NULL
        );
        CREATE INDEX ix_wheels_project ON wheels (skip_reason);
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("columns mismatch" in p for p in exc_info.value.problems)


def test_init_fails_on_index_unique_mismatch(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE wheels (
            filename TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, conda_name TEXT,
            pypi_simple TEXT,
            skip_reason TEXT, metadata_downloaded_at TEXT, wheel_metadata TEXT,
            metadata_reroll_version TEXT, repodata TEXT, name_conversions TEXT,
            repodata_reroll_version TEXT, updated_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX ix_wheels_project ON wheels (project);
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("UNIQUE mismatch" in p for p in exc_info.value.problems)


def test_init_fails_on_primary_key_mismatch(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT NOT NULL, serial INTEGER NOT NULL, "
        "updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("PRIMARY KEY mismatch" in p for p in exc_info.value.problems)


def test_init_does_not_alter_existing_valid_table(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 1, "2024-01-01T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    init_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT name, serial, updated_at FROM pypi_index WHERE name = ?",
            ("numpy",),
        ).fetchone()
    finally:
        conn.close()
    assert row == ("numpy", 1, "2024-01-01T00:00:00Z")
