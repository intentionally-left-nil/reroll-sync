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
