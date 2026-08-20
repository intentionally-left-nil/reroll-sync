import sqlite3

from reroll_sync.cli import main


def test_cli_init_creates_database_with_default_path(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    exit_code = main(["init"])

    assert exit_code == 0
    db_path = tmp_path / "reroll_sync.db"
    assert db_path.exists()
    captured = capsys.readouterr()
    assert "reroll_sync.db" in captured.out


def test_cli_init_creates_database_at_custom_path(tmp_path):
    db_path = tmp_path / "custom.db"

    exit_code = main(["init", str(db_path)])

    assert exit_code == 0
    assert db_path.exists()


def test_cli_init_reports_schema_mismatch(tmp_path, capsys):
    db_path = tmp_path / "bad.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE pypi_index (name TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    exit_code = main(["init", str(db_path)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Schema mismatch" in captured.err
    assert "pypi_index" in captured.err


def test_cli_sync_index_reports_schema_mismatch(tmp_path, capsys):
    db_path = tmp_path / "bad.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE pypi_index (name TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    exit_code = main(["sync-index", str(db_path)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Schema mismatch" in captured.err


def test_cli_stats_reports_schema_mismatch(tmp_path, capsys):
    db_path = tmp_path / "bad.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE pypi_index (name TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    exit_code = main(["stats", str(db_path)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Schema mismatch" in captured.err


def test_cli_stats_reports_zero_counts_on_empty_database(tmp_path, capsys):
    db_path = tmp_path / "reroll_sync.db"

    exit_code = main(["stats", str(db_path)])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "0 project(s) indexed" in captured.out
    assert "0 wheel(s) synced" in captured.out


def test_cli_stats_reports_counts_with_data(tmp_path, capsys):
    db_path = tmp_path / "reroll_sync.db"
    main(["init", str(db_path)])
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?)",
        ("numpy", 1, "2024-01-01T00:00:00+00:00"),
    )
    conn.execute(
        "INSERT INTO wheels (filename, project, updated_at) VALUES (?, ?, ?)",
        ("numpy-1.0-py3-none-any.whl", "numpy", "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    exit_code = main(["stats", str(db_path)])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "1 project(s) indexed" in captured.out
    assert "1 wheel(s) synced" in captured.out


def test_cli_sync_index_against_real_pypi_populates_one_project(tmp_path, capsys):
    """Integration test: hits the real PyPI simple index over the network."""
    db_path = tmp_path / "reroll_sync.db"

    exit_code = main(["sync-index", str(db_path), "--limit", "1", "--timeout", "60"])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Synced" in captured.out

    conn = sqlite3.connect(str(db_path))
    try:
        index_rows = conn.execute("SELECT name, serial, updated_at FROM pypi_index").fetchall()
        assert len(index_rows) == 1
        name, serial, updated_at = index_rows[0]
        assert isinstance(name, str)
        assert name
        assert isinstance(serial, int)
        assert updated_at

        wheel_rows = conn.execute(
            "SELECT filename, project, pypi_simple, conda_name, skip_reason, "
            "metadata_downloaded_at, wheel_metadata, metadata_reroll_version, repodata, "
            "name_conversions, repodata_reroll_version, updated_at FROM wheels"
        ).fetchall()
        assert len(wheel_rows) >= 1
        for row in wheel_rows:
            filename, project, pypi_simple, *nullable_fields, wheel_updated_at = row
            assert filename.endswith(".whl")
            assert project == name
            assert pypi_simple
            assert wheel_updated_at
            assert nullable_fields == [None] * 8
    finally:
        conn.close()
