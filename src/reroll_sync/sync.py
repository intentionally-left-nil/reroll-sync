"""The pypi_index/wheels update algorithm described in ``docs/index_ingestion.md``.

Each run fetches the PyPI simple index, figures out which projects are
missing or stale locally, and re-syncs those projects' wheel filenames.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from .pypi_client import (
    IndexProject,
    ProjectFile,
    ProjectResponse,
    SimpleIndexResponse,
    fetch_project,
    fetch_simple_index,
)

FetchIndex = Callable[[float | None], SimpleIndexResponse]
FetchProject = Callable[[str, float | None], ProjectResponse]


@dataclass(frozen=True)
class SyncStats:
    """Summary of one ``sync_index`` run."""

    projects_outdated: int
    projects_updated: int
    wheels_inserted: int
    stopped_early: bool


def sync_index(
    conn: sqlite3.Connection,
    *,
    timeout: float | None = None,
    limit: int | None = None,
    fetch_index: FetchIndex = fetch_simple_index,
    fetch_project_files: FetchProject = fetch_project,
) -> SyncStats:
    """Run one pass of the update algorithm against ``conn``.

    ``timeout`` bounds both the total elapsed time of the run and each
    individual HTTP request; once exceeded, processing stops early and the
    remaining outdated projects are left for the next run. ``limit`` caps how
    many outdated projects are processed in this run.
    """
    start = time.monotonic()
    index = fetch_index(timeout)
    known_serials = _known_serials(conn)
    outdated = _outdated_projects(index.projects, known_serials)
    if limit is not None:
        outdated = outdated[:limit]

    projects_updated = 0
    wheels_inserted = 0
    stopped_early = False

    for project in outdated:
        if timeout is not None and time.monotonic() - start > timeout:
            stopped_early = True
            break
        try:
            response = fetch_project_files(project.name, timeout)
        except OSError:
            continue
        updated_at = datetime.now(UTC).isoformat()
        wheels_inserted += _insert_wheels(conn, project.name, response.files, updated_at)
        _upsert_pypi_index(conn, project.name, response.last_serial, updated_at)
        conn.commit()
        projects_updated += 1

    return SyncStats(
        projects_outdated=len(outdated),
        projects_updated=projects_updated,
        wheels_inserted=wheels_inserted,
        stopped_early=stopped_early,
    )


def _known_serials(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT name, serial FROM pypi_index").fetchall()
    return dict(rows)


def _outdated_projects(
    projects: tuple[IndexProject, ...], known_serials: dict[str, int]
) -> list[IndexProject]:
    return [
        project
        for project in projects
        if project.name not in known_serials or project.serial > known_serials[project.name]
    ]


def _insert_wheels(
    conn: sqlite3.Connection,
    project_name: str,
    files: tuple[ProjectFile, ...],
    updated_at: str,
) -> int:
    inserted = 0
    for file in files:
        if not file.filename.endswith(".whl"):
            continue
        cursor = conn.execute(
            "INSERT INTO wheels (filename, project, pypi_simple, updated_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(filename) DO NOTHING",
            (file.filename, project_name, json.dumps(file.raw), updated_at),
        )
        inserted += cursor.rowcount
    return inserted


def _upsert_pypi_index(conn: sqlite3.Connection, name: str, serial: int, updated_at: str) -> None:
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(name) DO UPDATE SET "
        "serial = excluded.serial, updated_at = excluded.updated_at",
        (name, serial, updated_at),
    )
