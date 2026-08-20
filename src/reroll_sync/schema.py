"""Declarative schema definition for the reroll-sync sqlite database.

The tables here are the canonical source of truth described in
``docs/db.md``. Both the ``CREATE TABLE`` statements used by ``init`` and the
runtime introspection used to validate an already-existing database are
derived from these definitions, so they can never drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ForeignKey:
    """A ``REFERENCES`` constraint on a column."""

    ref_table: str
    ref_column: str


@dataclass(frozen=True)
class Column:
    """A single column definition."""

    name: str
    type: str  # one of sqlite's storage classes: TEXT, INTEGER, REAL, BLOB
    primary_key: bool = False
    not_null: bool = False
    autoincrement: bool = False
    references: ForeignKey | None = None

    def to_sql(self) -> str:
        parts = [self.name, self.type]
        if self.primary_key:
            parts.append("PRIMARY KEY")
            if self.autoincrement:
                parts.append("AUTOINCREMENT")
        if self.not_null:
            parts.append("NOT NULL")
        if self.references is not None:
            parts.append(f"REFERENCES {self.references.ref_table}({self.references.ref_column})")
        return " ".join(parts)


@dataclass(frozen=True)
class Index:
    """A named index on one or more columns of a table."""

    name: str
    columns: tuple[str, ...]
    unique: bool = False

    def to_sql(self, table_name: str) -> str:
        kind = "UNIQUE INDEX" if self.unique else "INDEX"
        cols = ", ".join(self.columns)
        return f"CREATE {kind} IF NOT EXISTS {self.name} ON {table_name} ({cols})"


@dataclass(frozen=True)
class Table:
    """A table definition: its columns and any secondary indexes."""

    name: str
    columns: tuple[Column, ...]
    indexes: tuple[Index, ...] = field(default_factory=tuple)

    def create_table_sql(self) -> str:
        col_sql = [col.to_sql() for col in self.columns]
        body = ",\n    ".join(col_sql)
        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n    {body}\n)"

    def create_index_sql(self) -> list[str]:
        return [idx.to_sql(self.name) for idx in self.indexes]


# ---------------------------------------------------------------------------
# pypi_index
# ---------------------------------------------------------------------------
PYPI_INDEX = Table(
    name="pypi_index",
    columns=(
        Column("name", "TEXT", primary_key=True),
        Column("serial", "INTEGER", not_null=True),
        Column("updated_at", "TEXT", not_null=True),
    ),
)

# ---------------------------------------------------------------------------
# wheels
# ---------------------------------------------------------------------------
WHEELS = Table(
    name="wheels",
    columns=(
        Column("filename", "TEXT", primary_key=True),
        Column("project", "TEXT", not_null=True),
        Column("pypi_simple", "TEXT"),
        Column("skip_reason", "TEXT"),
        Column("metadata_downloaded_at", "TEXT"),
        Column("wheel_metadata", "TEXT"),
        Column("metadata_reroll_version", "TEXT"),
        Column("repodata", "TEXT"),
        Column("repodata_reroll_version", "TEXT"),
        Column("updated_at", "TEXT", not_null=True),
    ),
    indexes=(Index("ix_wheels_project", ("project",)),),
)

# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------
ERRORS = Table(
    name="errors",
    columns=(
        Column("id", "INTEGER", primary_key=True, autoincrement=True),
        Column(
            "wheel_filename",
            "TEXT",
            not_null=True,
            references=ForeignKey(WHEELS.name, "filename"),
        ),
        Column("error_category", "TEXT", not_null=True),
        Column("error_subcategory", "TEXT"),
        Column("details", "TEXT"),
        Column("reroll_version", "TEXT"),
        Column("created_at", "TEXT", not_null=True),
    ),
    indexes=(Index("ix_errors_wheel_filename", ("wheel_filename",)),),
)

SCHEMA: tuple[Table, ...] = (PYPI_INDEX, WHEELS, ERRORS)
