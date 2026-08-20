"""Aggregate counts summarizing the current state of the database."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class Stats:
    """Summary counts of a reroll-sync database."""

    projects_indexed: int
    wheels_synced: int


def compute_stats(conn: sqlite3.Connection) -> Stats:
    """Compute summary counts from the current state of ``conn``."""
    (projects_indexed,) = conn.execute("SELECT COUNT(*) FROM pypi_index").fetchone()
    (wheels_synced,) = conn.execute("SELECT COUNT(*) FROM wheels").fetchone()
    return Stats(projects_indexed=projects_indexed, wheels_synced=wheels_synced)
