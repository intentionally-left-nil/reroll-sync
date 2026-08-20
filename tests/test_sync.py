import sqlite3

import pytest

from reroll_sync.db import init_db
from reroll_sync.pypi_client import (
    IndexProject,
    ProjectFile,
    ProjectResponse,
    SimpleIndexResponse,
)
from reroll_sync.sync import sync_index


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    connection = sqlite3.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()


def _index(*projects: tuple[str, int]) -> SimpleIndexResponse:
    return SimpleIndexResponse(
        last_serial=999,
        projects=tuple(IndexProject(name=name, serial=serial) for name, serial in projects),
    )


def _project(last_serial: int, *filenames: str) -> ProjectResponse:
    return ProjectResponse(
        last_serial=last_serial,
        files=tuple(
            ProjectFile(filename=filename, raw={"filename": filename}) for filename in filenames
        ),
    )


def _wheels_rows(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT filename, project, pypi_simple, conda_name, skip_reason, "
        "metadata_downloaded_at, wheel_metadata, metadata_reroll_version, repodata, "
        "name_conversions, repodata_reroll_version, updated_at FROM wheels ORDER BY filename"
    ).fetchall()


def _pypi_index_rows(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute("SELECT name, serial, updated_at FROM pypi_index ORDER BY name").fetchall()


def test_new_project_is_synced_and_wheels_inserted(conn):
    index = _index(("numpy", 42))
    project = _project(42, "numpy-1.0-py3-none-any.whl", "numpy-1.0.tar.gz")

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=lambda name, timeout: project,
    )

    assert stats.projects_outdated == 1
    assert stats.projects_updated == 1
    assert stats.wheels_inserted == 1
    assert stats.stopped_early is False

    wheels = _wheels_rows(conn)
    assert len(wheels) == 1
    filename, project_name, pypi_simple, *rest_and_updated_at = wheels[0]
    *rest, updated_at = rest_and_updated_at
    assert filename == "numpy-1.0-py3-none-any.whl"
    assert project_name == "numpy"
    assert pypi_simple == '{"filename": "numpy-1.0-py3-none-any.whl"}'
    assert rest == [None] * 8
    assert updated_at

    assert _pypi_index_rows(conn) == [("numpy", 42, updated_at)]


def test_project_missing_from_db_is_outdated(conn):
    index = _index(("numpy", 1))
    project = _project(1)

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=lambda name, timeout: project,
    )

    assert stats.projects_outdated == 1
    assert stats.projects_updated == 1


def test_project_with_older_index_serial_is_not_outdated(conn):
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 50, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    index = _index(("numpy", 10))

    def fetch_project_files(name, timeout):
        raise AssertionError("should not be called for an up-to-date project")

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=fetch_project_files,
    )

    assert stats.projects_outdated == 0
    assert stats.projects_updated == 0


def test_project_with_newer_index_serial_is_outdated(conn):
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 10, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    index = _index(("numpy", 50))
    project = _project(50, "numpy-2.0-py3-none-any.whl")

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=lambda name, timeout: project,
    )

    assert stats.projects_outdated == 1
    assert stats.projects_updated == 1
    assert _pypi_index_rows(conn)[0][:2] == ("numpy", 50)


def test_project_with_equal_index_serial_is_not_outdated(conn):
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 10, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    index = _index(("numpy", 10))

    def fetch_project_files(name, timeout):
        raise AssertionError("should not be called for an up-to-date project")

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=fetch_project_files,
    )

    assert stats.projects_outdated == 0


def test_only_whl_files_are_inserted_into_wheels(conn):
    index = _index(("beautifulsoup4", 1))
    project = _project(
        1,
        "beautifulsoup4-4.0.1.tar.gz",
        "beautifulsoup4-4.0.1-py3-none-any.whl",
        "beautifulsoup4-4.0.1.zip",
    )

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=lambda name, timeout: project,
    )

    assert stats.wheels_inserted == 1
    wheels = _wheels_rows(conn)
    assert [row[0] for row in wheels] == ["beautifulsoup4-4.0.1-py3-none-any.whl"]


def test_conflicting_wheel_row_is_not_overwritten(conn):
    conn.execute(
        "INSERT INTO wheels (filename, project, conda_name, pypi_simple, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            "numpy-1.0-py3-none-any.whl",
            "numpy",
            "numpy-conda",
            '{"filename": "numpy-1.0-py3-none-any.whl"}',
            "2024-01-01T00:00:00+00:00",
        ),
    )
    conn.commit()
    index = _index(("numpy", 1))
    project = _project(1, "numpy-1.0-py3-none-any.whl")

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=lambda name, timeout: project,
    )

    assert stats.wheels_inserted == 0
    row = conn.execute(
        "SELECT conda_name, updated_at FROM wheels WHERE filename = ?",
        ("numpy-1.0-py3-none-any.whl",),
    ).fetchone()
    assert row == ("numpy-conda", "2024-01-01T00:00:00+00:00")


def test_existing_pypi_index_row_is_updated_on_conflict(conn):
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 1, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    index = _index(("numpy", 2))
    project = _project(2)

    sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=lambda name, timeout: project,
    )

    row = conn.execute(
        "SELECT name, serial, updated_at FROM pypi_index WHERE name = ?", ("numpy",)
    ).fetchone()
    assert row[1] == 2
    assert row[2] != "2024-01-01T00:00:00+00:00"


def test_limit_caps_number_of_projects_processed(conn):
    index = _index(("a", 1), ("b", 1), ("c", 1))
    processed: list[str] = []

    def fetch_project_files(name, timeout):
        processed.append(name)
        return _project(1)

    stats = sync_index(
        conn,
        limit=2,
        fetch_index=lambda timeout: index,
        fetch_project_files=fetch_project_files,
    )

    assert stats.projects_outdated == 2
    assert stats.projects_updated == 2
    assert processed == ["a", "b"]


def test_no_limit_processes_all_outdated_projects(conn):
    index = _index(("a", 1), ("b", 1))
    processed: list[str] = []

    def fetch_project_files(name, timeout):
        processed.append(name)
        return _project(1)

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=fetch_project_files,
    )

    assert stats.projects_outdated == 2
    assert processed == ["a", "b"]


def test_timeout_stops_processing_early(conn, monkeypatch):
    index = _index(("a", 1), ("b", 1))
    processed: list[str] = []
    times = iter([0.0, 100.0])
    monkeypatch.setattr("reroll_sync.sync.time.monotonic", lambda: next(times))

    def fetch_project_files(name, timeout):
        processed.append(name)
        return _project(1)

    stats = sync_index(
        conn,
        timeout=5,
        fetch_index=lambda timeout: index,
        fetch_project_files=fetch_project_files,
    )

    assert processed == []
    assert stats.projects_updated == 0
    assert stats.stopped_early is True


def test_partial_failure_skips_project_and_continues(conn):
    index = _index(("broken", 1), ("ok", 1))

    def fetch_project_files(name, timeout):
        if name == "broken":
            raise OSError("network error")
        return _project(1, "ok-1.0-py3-none-any.whl")

    stats = sync_index(
        conn,
        fetch_index=lambda timeout: index,
        fetch_project_files=fetch_project_files,
    )

    assert stats.projects_outdated == 2
    assert stats.projects_updated == 1
    assert stats.wheels_inserted == 1
    names = {row[0] for row in _pypi_index_rows(conn)}
    assert names == {"ok"}
