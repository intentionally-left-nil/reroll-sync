import sqlite3

from reroll_sync.db import init_db
from reroll_sync.stats import compute_stats


def test_compute_stats_on_empty_database(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        stats = compute_stats(conn)
    finally:
        conn.close()

    assert stats.projects_indexed == 0
    assert stats.wheels_synced == 0


def test_compute_stats_counts_projects_and_wheels(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
            ("numpy", 1, "2024-01-01T00:00:00+00:00"),
        )
        conn.execute(
            "INSERT INTO wheels (filename, project, pypi_simple, updated_at) VALUES (?, ?, ?, ?)",
            (
                "numpy-1.0-py3-none-any.whl",
                "numpy",
                '{"filename": "numpy-1.0-py3-none-any.whl"}',
                "2024-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            "INSERT INTO wheels (filename, project, pypi_simple, updated_at) VALUES (?, ?, ?, ?)",
            (
                "numpy-2.0-py3-none-any.whl",
                "numpy",
                '{"filename": "numpy-2.0-py3-none-any.whl"}',
                "2024-01-01T00:00:00+00:00",
            ),
        )
        conn.commit()

        stats = compute_stats(conn)
    finally:
        conn.close()

    assert stats.projects_indexed == 1
    assert stats.wheels_synced == 2
