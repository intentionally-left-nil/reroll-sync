"""Declarative schema for the Phase 1 reroll-sync sqlite database.

The ``Table``/``Column``/``Index`` definitions below are the single source
of truth for both the DDL ``init_db`` executes and the introspection that
validates an already-existing database, so the two can never drift apart.
See ``docs/db.md`` for the schema's rationale.

Invariants a later change must not violate:

* ``requires_prerelease`` lives on ``wheel_repodata``, not ``wheels``: it is
  an output of conversion and must be written and deleted atomically with
  the repodata it describes.
* ``ix_wheels_queue`` orders by ``(state, lane, project, id)``, not
  ``(state, lane, id)``: the ``project`` column keeps backfill processing
  grouped by project, which publishing needs to build each shard once.
* Raw PyPI simple-index JSON is not retained; only the normalized columns
  below are kept.
* Parsed wheel metadata is not stored at all: it is re-derivable in ~5 ms
  from the archived ``METADATA`` bytes.
* ``skips.reroll_version`` is ``NULL`` if and only if ``permanent = 1``
  (enforced by ``fsck``, not by a database constraint).
* ``blobs.sha256`` is the sha256 of the raw ``METADATA`` bytes; blobs are
  content-addressed and shared, so many wheels may reference the same one.
* No table uses ``AUTOINCREMENT``: nothing here needs monotonic-forever
  ids, and ``Column``/``Table`` have no field to declare it.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

from frozendict import frozendict


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
    not_null: bool = False
    unique: bool = False
    default: str | None = None
    references: ForeignKey | None = None

    def to_sql(self) -> str:
        parts = [self.name, self.type]
        if self.not_null:
            parts.append("NOT NULL")
        if self.unique:
            parts.append("UNIQUE")
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        if self.references is not None:
            parts.append(f"REFERENCES {self.references.ref_table}({self.references.ref_column})")
        return " ".join(parts)


@dataclass(frozen=True)
class Index:
    """A named index on one or more columns of a table, optionally partial."""

    name: str
    columns: tuple[str, ...]
    where: str | None = None

    def to_sql(self, table_name: str) -> str:
        cols = ", ".join(self.columns)
        sql = f"CREATE INDEX IF NOT EXISTS {self.name} ON {table_name} ({cols})"
        if self.where is not None:
            sql += f" WHERE {self.where}"
        return sql


@dataclass(frozen=True)
class Table:
    """A table definition: its columns, primary key, and secondary indexes."""

    name: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    indexes: tuple[Index, ...] = field(default_factory=tuple)

    def create_table_sql(self) -> str:
        col_sql = [col.to_sql() for col in self.columns]
        pk_sql = f"PRIMARY KEY ({', '.join(self.primary_key)})"
        body = ",\n    ".join([*col_sql, pk_sql])
        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n    {body}\n)"

    def create_index_sql(self) -> list[str]:
        return [idx.to_sql(self.name) for idx in self.indexes]


# ---------------------------------------------------------------------------
# pypi_index
# ---------------------------------------------------------------------------
PYPI_INDEX = Table(
    name="pypi_index",
    columns=(
        Column("name", "TEXT", not_null=True),
        Column("serial", "INTEGER", not_null=True),
        Column("updated_at", "TEXT", not_null=True),
    ),
    primary_key=("name",),
)

# ---------------------------------------------------------------------------
# wheels
# ---------------------------------------------------------------------------
WHEELS = Table(
    name="wheels",
    columns=(
        Column("id", "INTEGER"),
        Column("filename", "TEXT", not_null=True, unique=True),
        Column("project", "TEXT", not_null=True),
        Column("conda_name", "TEXT"),
        Column("state", "INTEGER", not_null=True),
        Column("lane", "INTEGER", not_null=True, default="0"),
        Column("url", "TEXT", not_null=True),
        Column("wheel_sha256", "TEXT"),
        Column("metadata_sha256", "TEXT"),
        Column("size", "INTEGER"),
        Column("upload_time", "TEXT"),
        Column("requires_python", "TEXT"),
        Column("yanked", "INTEGER", not_null=True, default="0"),
        Column("yanked_reason", "TEXT"),
        Column("blob_sha256", "TEXT"),
        Column("serial", "INTEGER", not_null=True),
        Column("change_seq", "INTEGER", not_null=True),
        Column("deleted_at", "TEXT"),
        Column("updated_at", "TEXT", not_null=True),
    ),
    primary_key=("id",),
    indexes=(
        Index("ix_wheels_queue", ("state", "lane", "project", "id")),
        Index("ix_wheels_conda_name", ("conda_name",), where="conda_name IS NOT NULL"),
        Index("ix_wheels_project", ("project", "id")),
        Index("ix_wheels_change_seq", ("change_seq",)),
    ),
)

# ---------------------------------------------------------------------------
# segments
# ---------------------------------------------------------------------------
SEGMENTS = Table(
    name="segments",
    columns=(
        Column("id", "INTEGER"),
        Column("sealed_at", "TEXT"),
        Column("bytes", "INTEGER"),
        Column("records", "INTEGER"),
        Column("footer_sha", "TEXT"),
    ),
    primary_key=("id",),
)

# ---------------------------------------------------------------------------
# blobs
# ---------------------------------------------------------------------------
BLOBS = Table(
    name="blobs",
    columns=(
        Column("sha256", "TEXT", not_null=True),
        Column("segment_id", "INTEGER", not_null=True, references=ForeignKey(SEGMENTS.name, "id")),
        Column("block_no", "INTEGER", not_null=True),
        Column("offset", "INTEGER", not_null=True),
        Column("length", "INTEGER", not_null=True),
    ),
    primary_key=("sha256",),
    indexes=(Index("ix_blobs_segment", ("segment_id", "block_no")),),
)

# ---------------------------------------------------------------------------
# wheel_repodata
# ---------------------------------------------------------------------------
WHEEL_REPODATA = Table(
    name="wheel_repodata",
    columns=(
        Column("wheel_id", "INTEGER", not_null=True, references=ForeignKey(WHEELS.name, "id")),
        Column("repodata_zst", "BLOB", not_null=True),
        Column("name_conv_zst", "BLOB"),
        Column("requires_prerelease", "INTEGER", not_null=True, default="0"),
        Column("reroll_version", "TEXT", not_null=True),
    ),
    primary_key=("wheel_id",),
    indexes=(
        Index("ix_wheel_repodata_version", ("reroll_version",)),
        Index(
            "ix_wheel_repodata_prerelease",
            ("requires_prerelease",),
            where="requires_prerelease = 1",
        ),
    ),
)

# ---------------------------------------------------------------------------
# skips
# ---------------------------------------------------------------------------
SKIPS = Table(
    name="skips",
    columns=(
        Column("wheel_id", "INTEGER", not_null=True, references=ForeignKey(WHEELS.name, "id")),
        Column("stage", "TEXT", not_null=True),
        Column("reason", "TEXT", not_null=True),
        Column("permanent", "INTEGER", not_null=True),
        Column("reroll_version", "TEXT"),
        Column("created_at", "TEXT", not_null=True),
    ),
    primary_key=("wheel_id", "stage"),
    indexes=(Index("ix_skips_retryable", ("reroll_version",), where="permanent = 0"),),
)

# ---------------------------------------------------------------------------
# work
# ---------------------------------------------------------------------------
WORK = Table(
    name="work",
    columns=(
        Column("wheel_id", "INTEGER", not_null=True, references=ForeignKey(WHEELS.name, "id")),
        Column("stage", "TEXT", not_null=True),
        Column("attempts", "INTEGER", not_null=True),
        Column("next_attempt_at", "TEXT", not_null=True),
        Column("last_error", "TEXT"),
        Column("quarantined_at", "TEXT"),
    ),
    primary_key=("wheel_id", "stage"),
    indexes=(Index("ix_work_ready", ("stage", "next_attempt_at"), where="quarantined_at IS NULL"),),
)

# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------
ERRORS = Table(
    name="errors",
    columns=(
        Column("id", "INTEGER"),
        Column("wheel_id", "INTEGER", references=ForeignKey(WHEELS.name, "id")),
        Column("error_category", "TEXT", not_null=True),
        Column("error_subcat", "TEXT"),
        Column("details", "TEXT"),
        Column("reroll_version", "TEXT", not_null=True),
        Column("created_at", "TEXT", not_null=True),
    ),
    primary_key=("id",),
    indexes=(Index("ix_errors_cat", ("error_category", "created_at")),),
)

# ---------------------------------------------------------------------------
# unlinked_blobs -- bulk-import bridge (spec 13); a blob whose filename has
# no wheels row yet. Spec 08's ingestion links it on insert and deletes the
# row. Not documented in docs/db.md as of this schema version.
# ---------------------------------------------------------------------------
UNLINKED_BLOBS = Table(
    name="unlinked_blobs",
    columns=(
        Column("filename", "TEXT", not_null=True),
        Column("sha256", "TEXT", not_null=True),
        Column("noted_at", "TEXT", not_null=True),
    ),
    primary_key=("filename",),
)

SCHEMA: tuple[Table, ...] = (
    PYPI_INDEX,
    WHEELS,
    SEGMENTS,
    BLOBS,
    WHEEL_REPODATA,
    SKIPS,
    WORK,
    ERRORS,
    UNLINKED_BLOBS,
)

SCHEMA_VERSION = 1


class WheelState(enum.IntEnum):
    """A ``wheels.state`` value. See ``docs/pipeline.md`` for the state diagram."""

    NEED_METADATA = 0
    NO_METADATA = 1
    NEED_CONVERT = 2
    READY = 3
    SKIPPED = 4
    QUARANTINED = 5
    DELETED = 6


ALLOWED_TRANSITIONS: frozendict[WheelState, frozenset[WheelState]] = frozendict(
    {
        WheelState.NEED_METADATA: frozenset(
            {WheelState.NEED_CONVERT, WheelState.NO_METADATA, WheelState.QUARANTINED}
        ),
        WheelState.NO_METADATA: frozenset({WheelState.NEED_METADATA}),
        WheelState.NEED_CONVERT: frozenset(
            {WheelState.READY, WheelState.SKIPPED, WheelState.QUARANTINED}
        ),
        WheelState.READY: frozenset({WheelState.NEED_CONVERT, WheelState.DELETED}),
        WheelState.SKIPPED: frozenset({WheelState.NEED_CONVERT}),
        WheelState.QUARANTINED: frozenset({WheelState.NEED_METADATA}),
        WheelState.DELETED: frozenset(),
    }
)
