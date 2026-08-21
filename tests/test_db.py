import sqlite3
import threading

import pytest

from reroll_sync.db import (
    AutoVacuumError,
    SchemaMismatchError,
    SchemaVersionError,
    connect,
    connect_reader,
    connect_writer,
    init_db,
)
from reroll_sync.schema import SCHEMA, WHEEL_REPODATA, WHEELS, WheelState


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row[0] for row in rows}


def _init(tmp_path) -> str:
    db_path = str(tmp_path / "reroll_sync.db")
    init_db(db_path)
    return db_path


def _create_raw_db(path, *, auto_vacuum: int, user_version: int) -> None:
    """Build a database with specific pragma values, bypassing init_db.

    auto_vacuum only takes effect once at least one page beyond the header
    has been written, so a throwaway table forces that write.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(f"PRAGMA auto_vacuum = {auto_vacuum}")
        conn.execute(f"PRAGMA user_version = {user_version}")
        conn.execute("CREATE TABLE _bootstrap (x INTEGER)")
        conn.commit()
    finally:
        conn.close()


def _raw_conn(path) -> sqlite3.Connection:
    """Open a connection to a not-yet-existing db with init_db's pragmas
    already set, for tests that hand-build a mismatched table and want to
    exercise only that mismatch, not the auto_vacuum/user_version gate."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
    conn.execute("PRAGMA user_version = 1")
    return conn


def _insert_wheel(
    conn: sqlite3.Connection,
    *,
    filename: str,
    state: WheelState = WheelState.NEED_METADATA,
    lane: int = 0,
    conda_name: str | None = None,
    blob_sha256: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO wheels "
        "(filename, project, conda_name, state, lane, url, serial, change_seq, "
        "updated_at, blob_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            filename,
            "foo",
            conda_name,
            int(state),
            lane,
            f"https://files.pythonhosted.org/packages/{filename}",
            1,
            1,
            "2024-01-01T00:00:00Z",
            blob_sha256,
        ),
    )
    conn.commit()


# --- pragmas and creation ---------------------------------------------


def test_fresh_database_reports_user_version_one(tmp_path):
    db_path = _init(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        (value,) = conn.execute("PRAGMA user_version").fetchone()
    finally:
        conn.close()
    assert value == 1


def test_fresh_database_reports_auto_vacuum_incremental(tmp_path):
    db_path = _init(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        (value,) = conn.execute("PRAGMA auto_vacuum").fetchone()
    finally:
        conn.close()
    assert value == 2


def test_init_db_raises_on_auto_vacuum_none(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    _create_raw_db(db_path, auto_vacuum=0, user_version=1)

    with pytest.raises(AutoVacuumError):
        init_db(db_path)


def test_init_db_raises_schema_version_error_on_unexpected_version(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    _create_raw_db(db_path, auto_vacuum=2, user_version=99)

    with pytest.raises(SchemaVersionError):
        init_db(db_path)


def test_connect_writer_reports_wal_journal_mode(tmp_path):
    db_path = _init(tmp_path)
    conn = connect_writer(db_path)
    try:
        (mode,) = conn.execute("PRAGMA journal_mode").fetchone()
    finally:
        conn.close()
    assert mode == "wal"


def test_connect_writer_connection_is_usable_from_a_different_thread(tmp_path):
    # Writer (spec 06) constructs the connection on one thread and then
    # runs its background daemon thread against it -- connect_writer must
    # not bind the connection to its creating thread.
    db_path = _init(tmp_path)
    conn = connect_writer(db_path)
    result: dict[str, object] = {}

    def _use_from_other_thread() -> None:
        try:
            (value,) = conn.execute("SELECT 1").fetchone()
            result["value"] = value
        except BaseException as exc:  # noqa: BLE001
            result["error"] = exc

    thread = threading.Thread(target=_use_from_other_thread)
    thread.start()
    thread.join(timeout=5)
    try:
        assert "error" not in result, result.get("error")
        assert result["value"] == 1
    finally:
        conn.close()


def test_connect_reader_on_nonexistent_path_raises_without_creating_file(tmp_path):
    db_path = tmp_path / "does_not_exist.db"

    with pytest.raises(sqlite3.OperationalError):
        connect_reader(db_path)

    assert not db_path.exists()


def test_connect_reader_rejects_a_write(tmp_path):
    db_path = _init(tmp_path)
    conn = connect_reader(db_path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute(
                "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
                ("numpy", 1, "2024-01-01T00:00:00Z"),
            )
    finally:
        conn.close()


def test_init_db_is_idempotent(tmp_path):
    db_path = _init(tmp_path)
    init_db(db_path)  # second call must not raise or alter anything

    conn = sqlite3.connect(db_path)
    try:
        (user_version,) = conn.execute("PRAGMA user_version").fetchone()
        (auto_vacuum,) = conn.execute("PRAGMA auto_vacuum").fetchone()
        assert _table_names(conn) == {t.name for t in SCHEMA}
    finally:
        conn.close()
    assert user_version == 1
    assert auto_vacuum == 2


def test_connect_is_a_writer_alias_kept_for_the_pre_rewrite_cli(tmp_path):
    db_path = _init(tmp_path)
    conn = connect(db_path)
    try:
        (mode,) = conn.execute("PRAGMA journal_mode").fetchone()
    finally:
        conn.close()
    assert mode == "wal"


# --- validation: column-level mismatches --------------------------------


def test_init_fails_on_missing_column(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT PRIMARY KEY NOT NULL, serial INTEGER NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("updated_at" in p for p in exc_info.value.problems)


def test_init_fails_on_unexpected_column(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT PRIMARY KEY NOT NULL, serial INTEGER NOT NULL, "
        "updated_at TEXT NOT NULL, extra_column TEXT)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("extra_column" in p for p in exc_info.value.problems)


def test_init_fails_on_wrong_column_type(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT PRIMARY KEY NOT NULL, serial TEXT NOT NULL, "
        "updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("serial" in p and "type mismatch" in p for p in exc_info.value.problems)


def test_init_fails_on_wrong_not_null(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT PRIMARY KEY NOT NULL, serial INTEGER, "
        "updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("serial" in p and "NOT NULL" in p for p in exc_info.value.problems)


def test_init_fails_on_wrong_primary_key(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(
        "CREATE TABLE pypi_index (name TEXT NOT NULL, serial INTEGER NOT NULL, "
        "updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("primary key" in p.lower() for p in exc_info.value.problems)


def test_init_fails_on_wrong_index_uniqueness(tmp_path):
    # wheels.filename must be UNIQUE; build the table with everything else
    # correct except that constraint, using the real DDL for the rest so
    # only the uniqueness defect is under test.
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    columns_sql = WHEELS.create_table_sql().replace(
        "filename TEXT NOT NULL UNIQUE", "filename TEXT NOT NULL"
    )
    conn.execute(columns_sql)
    for index_sql in WHEELS.create_index_sql():
        conn.execute(index_sql)
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("filename" in p and "UNIQUE" in p for p in exc_info.value.problems)


def test_init_fails_on_uniqueness_from_a_composite_constraint_not_a_single_column(tmp_path):
    # A UNIQUE(filename, project) table constraint is not the same thing as
    # filename being unique on its own; the validator must not conflate them.
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    columns_sql = (
        WHEELS.create_table_sql()
        .replace("filename TEXT NOT NULL UNIQUE,", "filename TEXT NOT NULL,")
        .replace("PRIMARY KEY (id)", "PRIMARY KEY (id),\n    UNIQUE (filename, project)")
    )
    conn.execute(columns_sql)
    for index_sql in WHEELS.create_index_sql():
        conn.execute(index_sql)
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any(
        "filename" in p and "UNIQUE" in p and "expected True, found False" in p
        for p in exc_info.value.problems
    )


# --- validation: foreign keys --------------------------------------------


def test_init_fails_on_missing_foreign_key(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(
        "CREATE TABLE wheel_repodata ("
        "wheel_id INTEGER NOT NULL, repodata_zst BLOB NOT NULL, name_conv_zst BLOB, "
        "requires_prerelease INTEGER NOT NULL DEFAULT 0, reroll_version TEXT NOT NULL, "
        "PRIMARY KEY (wheel_id))"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheel_repodata"
    assert any("foreign key" in p and "wheels" in p for p in exc_info.value.problems)


def test_init_fails_on_unexpected_foreign_key(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(
        "CREATE TABLE pypi_index ("
        "name TEXT NOT NULL REFERENCES wheels(id), serial INTEGER NOT NULL, "
        "updated_at TEXT NOT NULL, PRIMARY KEY (name))"
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "pypi_index"
    assert any("unexpected foreign key" in p for p in exc_info.value.problems)


# --- validation: indexes -------------------------------------------------


def test_init_fails_on_missing_index(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(WHEELS.create_table_sql())
    for index_sql in WHEELS.create_index_sql():
        if "ix_wheels_queue" in index_sql:
            continue
        conn.execute(index_sql)
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("ix_wheels_queue" in p for p in exc_info.value.problems)


def test_init_fails_on_unexpected_index(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(WHEELS.create_table_sql())
    for index_sql in WHEELS.create_index_sql():
        conn.execute(index_sql)
    conn.execute("CREATE INDEX ix_wheels_bogus ON wheels (blob_sha256)")
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("unexpected index 'ix_wheels_bogus'" in p for p in exc_info.value.problems)


def test_init_fails_on_index_column_mismatch(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(WHEELS.create_table_sql())
    for index_sql in WHEELS.create_index_sql():
        if "ix_wheels_project" in index_sql:
            conn.execute("CREATE INDEX ix_wheels_project ON wheels (project)")
        else:
            conn.execute(index_sql)
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any(
        "ix_wheels_project" in p and "columns mismatch" in p for p in exc_info.value.problems
    )


def test_init_fails_on_partial_index_where_clause_mismatch(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    conn = _raw_conn(db_path)
    conn.execute(WHEELS.create_table_sql())
    for index_sql in WHEELS.create_index_sql():
        if "ix_wheels_conda_name" in index_sql:
            conn.execute(
                "CREATE INDEX ix_wheels_conda_name ON wheels (conda_name) WHERE conda_name IS NULL"
            )
        else:
            conn.execute(index_sql)
    conn.commit()
    conn.close()

    with pytest.raises(SchemaMismatchError) as exc_info:
        init_db(db_path)

    assert exc_info.value.table_name == "wheels"
    assert any("ix_wheels_conda_name" in p and "WHERE" in p for p in exc_info.value.problems)


def test_init_does_not_alter_existing_valid_table(tmp_path):
    db_path = _init(tmp_path)

    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 1, "2024-01-01T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    init_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name, serial, updated_at FROM pypi_index WHERE name = ?",
            ("numpy",),
        ).fetchone()
    finally:
        conn.close()
    assert row == ("numpy", 1, "2024-01-01T00:00:00Z")


# --- schema shape (runtime) ----------------------------------------------


def test_wheels_id_is_pk_one_in_table_info(tmp_path):
    db_path = _init(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("PRAGMA table_info(wheels)").fetchall()
    finally:
        conn.close()
    id_row = next(r for r in rows if r[1] == "id")
    assert id_row[5] == 1  # pk column of table_info


def test_no_autoincrement_anywhere(tmp_path):
    db_path = _init(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence'"
        ).fetchone()
    finally:
        conn.close()
    assert row is None


def test_phase_2_tables_do_not_exist(tmp_path):
    db_path = _init(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        names = _table_names(conn)
    finally:
        conn.close()
    assert "dirty_packages" not in names
    assert "shard_index" not in names
    assert "objects" not in names


def test_every_wheel_state_round_trips_through_insert_and_select(tmp_path):
    db_path = _init(tmp_path)
    conn = connect_writer(db_path)
    try:
        for state in WheelState:
            filename = f"pkg-{state.name.lower()}-1.0-py3-none-any.whl"
            _insert_wheel(conn, filename=filename, state=state)
            (stored,) = conn.execute(
                "SELECT state FROM wheels WHERE filename = ?", (filename,)
            ).fetchone()
            assert stored == state
            assert WheelState(stored) is state
    finally:
        conn.close()


# --- behaviour under real constraints ------------------------------------


def test_duplicate_filename_violates_unique_constraint(tmp_path):
    db_path = _init(tmp_path)
    conn = connect_writer(db_path)
    try:
        _insert_wheel(conn, filename="foo-1.0-py3-none-any.whl")
        with pytest.raises(sqlite3.IntegrityError):
            _insert_wheel(conn, filename="foo-1.0-py3-none-any.whl")
    finally:
        conn.close()


def test_wheel_repodata_foreign_key_is_enforced_on_writer_connection(tmp_path):
    db_path = _init(tmp_path)
    conn = connect_writer(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO wheel_repodata (wheel_id, repodata_zst, reroll_version) "
                "VALUES (?, ?, ?)",
                (999, b"\x00", "0.4.0"),
            )
    finally:
        conn.close()
    assert WHEEL_REPODATA.primary_key == ("wheel_id",)


def test_two_wheels_may_share_one_blob_sha256(tmp_path):
    db_path = _init(tmp_path)
    conn = connect_writer(db_path)
    try:
        _insert_wheel(conn, filename="a-1.0-py3-none-any.whl", blob_sha256="deadbeef")
        _insert_wheel(conn, filename="b-1.0-py3-none-any.whl", blob_sha256="deadbeef")
        rows = conn.execute(
            "SELECT filename FROM wheels WHERE blob_sha256 = ?", ("deadbeef",)
        ).fetchall()
    finally:
        conn.close()
    assert {r[0] for r in rows} == {"a-1.0-py3-none-any.whl", "b-1.0-py3-none-any.whl"}


def test_conda_name_lookup_uses_partial_index(tmp_path):
    db_path = _init(tmp_path)
    conn = connect_writer(db_path)
    try:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM wheels WHERE conda_name = ?",
            ("numpy",),
        ).fetchall()
    finally:
        conn.close()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_conda_name" in detail for detail in details)
    assert not any("SCAN" in detail for detail in details)


def test_fetch_stage_queue_query_plan_uses_ix_wheels_queue(tmp_path):
    db_path = _init(tmp_path)
    conn = connect_writer(db_path)
    try:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT id FROM wheels WHERE state = ? AND lane = ? "
            "ORDER BY project, id LIMIT ?",
            (int(WheelState.NEED_METADATA), 0, 10),
        ).fetchall()
    finally:
        conn.close()
    details = [row[-1] for row in plan]
    assert any("ix_wheels_queue" in detail for detail in details)
    assert not any("SCAN" in detail for detail in details)
