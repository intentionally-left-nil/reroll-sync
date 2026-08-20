import re

from frozendict import frozendict

from reroll_sync.schema import (
    ALLOWED_TRANSITIONS,
    SCHEMA,
    SCHEMA_VERSION,
    Column,
    ForeignKey,
    Index,
    Table,
    WheelState,
)

FORBIDDEN_IDENTIFIERS = (
    "pypi_simple",
    "wheel_metadata",
    "metadata_reroll_version",
    "repodata",
    "name_conversions",
    "skip_reason",
    "metadata_downloaded_at",
)

EXPECTED_TABLE_NAMES = {
    "pypi_index",
    "wheels",
    "segments",
    "blobs",
    "wheel_repodata",
    "skips",
    "work",
    "errors",
    "unlinked_blobs",
}

# The mermaid diagram in docs/pipeline.md, minus the `[*] --> NEED_METADATA`
# pseudo-edge (that is state creation, not a transition between two states).
EXPECTED_TRANSITION_EDGES = {
    (WheelState.NEED_METADATA, WheelState.NEED_CONVERT),
    (WheelState.NEED_METADATA, WheelState.NO_METADATA),
    (WheelState.NEED_METADATA, WheelState.QUARANTINED),
    (WheelState.NO_METADATA, WheelState.NEED_METADATA),
    (WheelState.NEED_CONVERT, WheelState.READY),
    (WheelState.NEED_CONVERT, WheelState.SKIPPED),
    (WheelState.NEED_CONVERT, WheelState.QUARANTINED),
    (WheelState.SKIPPED, WheelState.NEED_CONVERT),
    (WheelState.READY, WheelState.NEED_CONVERT),
    (WheelState.READY, WheelState.DELETED),
    (WheelState.QUARANTINED, WheelState.NEED_METADATA),
}


def _table(name: str) -> Table:
    by_name = {t.name: t for t in SCHEMA}
    return by_name[name]


# --- dataclass rendering -----------------------------------------------


def test_column_to_sql_renders_type_and_not_null():
    col = Column("project", "TEXT", not_null=True)
    assert col.to_sql() == "project TEXT NOT NULL"


def test_column_to_sql_renders_nullable_bare_column():
    col = Column("conda_name", "TEXT")
    assert col.to_sql() == "conda_name TEXT"


def test_column_to_sql_renders_unique():
    col = Column("filename", "TEXT", not_null=True, unique=True)
    assert col.to_sql() == "filename TEXT NOT NULL UNIQUE"


def test_column_to_sql_renders_default():
    col = Column("lane", "INTEGER", not_null=True, default="0")
    assert col.to_sql() == "lane INTEGER NOT NULL DEFAULT 0"


def test_column_to_sql_renders_foreign_key():
    col = Column("wheel_id", "INTEGER", not_null=True, references=ForeignKey("wheels", "id"))
    assert col.to_sql() == "wheel_id INTEGER NOT NULL REFERENCES wheels(id)"


def test_index_to_sql_renders_plain_index():
    idx = Index("ix_wheels_project", ("project", "id"))
    assert (
        idx.to_sql("wheels")
        == "CREATE INDEX IF NOT EXISTS ix_wheels_project ON wheels (project, id)"
    )


def test_index_to_sql_renders_partial_where():
    idx = Index("ix_wheels_conda_name", ("conda_name",), where="conda_name IS NOT NULL")
    assert idx.to_sql("wheels") == (
        "CREATE INDEX IF NOT EXISTS ix_wheels_conda_name ON wheels (conda_name) "
        "WHERE conda_name IS NOT NULL"
    )


def test_table_create_table_sql_includes_primary_key_constraint():
    table = Table(
        name="pypi_index",
        columns=(
            Column("name", "TEXT", not_null=True),
            Column("serial", "INTEGER", not_null=True),
        ),
        primary_key=("name",),
    )
    sql = table.create_table_sql()
    assert "name TEXT NOT NULL" in sql
    assert "PRIMARY KEY (name)" in sql


def test_table_create_table_sql_supports_composite_primary_key():
    table = _table("skips")
    sql = table.create_table_sql()
    assert "PRIMARY KEY (wheel_id, stage)" in sql


def test_table_create_index_sql_returns_one_statement_per_index():
    table = _table("wheels")
    statements = table.create_index_sql()
    assert len(statements) == len(table.indexes)
    assert all(stmt.startswith("CREATE INDEX IF NOT EXISTS") for stmt in statements)


# --- schema shape / boundary --------------------------------------------


def test_schema_has_exactly_nine_tables():
    assert {t.name for t in SCHEMA} == EXPECTED_TABLE_NAMES
    assert len(SCHEMA) == 9


def test_schema_does_not_define_phase_2_tables():
    assert "dirty_packages" not in EXPECTED_TABLE_NAMES
    assert "shard_index" not in EXPECTED_TABLE_NAMES
    assert "objects" not in EXPECTED_TABLE_NAMES


def test_schema_version_is_one():
    assert SCHEMA_VERSION == 1


def test_wheels_table_has_explicit_integer_primary_key_named_id():
    wheels = _table("wheels")
    assert wheels.primary_key == ("id",)
    id_col = next(c for c in wheels.columns if c.name == "id")
    assert id_col.type == "INTEGER"


def test_no_column_declares_autoincrement():
    # AUTOINCREMENT was deliberately dropped: nothing here needs
    # monotonic-forever ids, and Column has no such field to misuse.
    assert not hasattr(Column("x", "INTEGER"), "autoincrement")


def test_no_forbidden_identifier_appears_in_any_table_or_column_name():
    for table in SCHEMA:
        assert table.name not in FORBIDDEN_IDENTIFIERS
        for col in table.columns:
            assert col.name not in FORBIDDEN_IDENTIFIERS


def test_no_forbidden_identifier_appears_in_generated_ddl():
    # Word-boundary match: "repodata" must not appear as its own identifier,
    # but "wheel_repodata"/"repodata_zst" (legitimate Phase 1 names that
    # happen to contain that substring) are fine.
    for table in SCHEMA:
        sql = table.create_table_sql()
        for forbidden in FORBIDDEN_IDENTIFIERS:
            assert not re.search(rf"\b{forbidden}\b", sql), (
                f"{forbidden!r} found in DDL for {table.name}"
            )


def test_requires_prerelease_lives_on_wheel_repodata_not_wheels():
    wheels_columns = {c.name for c in _table("wheels").columns}
    wheel_repodata_columns = {c.name for c in _table("wheel_repodata").columns}
    assert "requires_prerelease" not in wheels_columns
    assert "requires_prerelease" in wheel_repodata_columns


def test_ix_wheels_queue_orders_project_before_id():
    wheels = _table("wheels")
    queue_index = next(idx for idx in wheels.indexes if idx.name == "ix_wheels_queue")
    assert queue_index.columns == ("state", "lane", "project", "id")


def test_wheels_blob_sha256_has_no_foreign_key():
    wheels = _table("wheels")
    blob_col = next(c for c in wheels.columns if c.name == "blob_sha256")
    assert blob_col.references is None


# --- WheelState -----------------------------------------------------------


def test_wheel_state_values_match_spec():
    assert WheelState.NEED_METADATA == 0
    assert WheelState.NO_METADATA == 1
    assert WheelState.NEED_CONVERT == 2
    assert WheelState.READY == 3
    assert WheelState.SKIPPED == 4
    assert WheelState.QUARANTINED == 5
    assert WheelState.DELETED == 6


def test_wheel_state_is_int_enum_and_stored_as_integer():
    assert isinstance(WheelState.READY, int)


# --- ALLOWED_TRANSITIONS ---------------------------------------------------


def _flatten_transitions() -> set[tuple[WheelState, WheelState]]:
    edges = set()
    for source, destinations in ALLOWED_TRANSITIONS.items():
        for dest in destinations:
            edges.add((source, dest))
    return edges


def test_allowed_transitions_matches_pipeline_doc_exactly():
    assert _flatten_transitions() == EXPECTED_TRANSITION_EDGES


def test_allowed_transitions_has_no_self_edges():
    for source, dest in _flatten_transitions():
        assert source != dest


def test_allowed_transitions_covers_every_state_as_a_key():
    assert set(ALLOWED_TRANSITIONS) == set(WheelState)


def test_allowed_transitions_is_immutable():
    # Immutability is enforced by construction (frozendict of frozensets),
    # not by catching a mutation attempt at the type-checked API surface.
    assert isinstance(ALLOWED_TRANSITIONS, frozendict)
    assert all(isinstance(dest, frozenset) for dest in ALLOWED_TRANSITIONS.values())


# --- DDL text sanity (spot checks against the literal spec DDL) -----------


def test_wheels_ddl_matches_expected_column_order_and_text():
    wheels = _table("wheels")
    sql = wheels.create_table_sql()
    # id has no NOT NULL: an INTEGER column that is the sole member of the
    # table's PRIMARY KEY becomes a rowid alias regardless, and SQLite does
    # not report it as NOT NULL unless the keyword is present.
    assert re.search(r"\bid INTEGER\b(?!\s+NOT)", sql)
    assert "filename TEXT NOT NULL UNIQUE" in sql
    assert "lane INTEGER NOT NULL DEFAULT 0" in sql
    assert "yanked INTEGER NOT NULL DEFAULT 0" in sql


def test_wheel_repodata_ddl_has_explicit_not_null_on_primary_key():
    wheel_repodata = _table("wheel_repodata")
    sql = wheel_repodata.create_table_sql()
    assert "wheel_id INTEGER NOT NULL REFERENCES wheels(id)" in sql


def test_unlinked_blobs_has_no_secondary_indexes():
    assert _table("unlinked_blobs").indexes == ()
