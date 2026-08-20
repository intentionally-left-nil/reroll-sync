import json
import sqlite3

import pytest
from reroll import NameResolution, WheelRecord, Winner
from reroll.errors import NetworkFetchError, UnresolvedCondaNameError
from reroll.name_mapping import CandidateSource, passthrough_mapper

from reroll_sync.db import init_db
from reroll_sync.reroll_convert import sync_reroll
from reroll_sync.version import REROLL_VERSION

_VALID_METADATA = json.dumps({"name": "example", "version": "1.0"})


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    connection = sqlite3.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()


def _insert_wheel(conn, filename, *, wheel_metadata=_VALID_METADATA):
    conn.execute(
        "INSERT INTO wheels (filename, project, pypi_simple, wheel_metadata, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (filename, "example", "{}", wheel_metadata, "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()


def _wheel_row(conn, filename):
    return conn.execute(
        "SELECT skip_reason, repodata, name_conversions, repodata_reroll_version FROM wheels "
        "WHERE filename = ?",
        (filename,),
    ).fetchone()


def _errors(conn):
    return conn.execute(
        "SELECT wheel_filename, error_category, error_subcategory, details, reroll_version "
        "FROM errors"
    ).fetchall()


def _winner(name):
    return Winner(conda_name=name, probability=0.0, source=CandidateSource.PASSTHROUGH, mapper="m")


def _record(name="example", resolutions=()):
    return WheelRecord(
        name=name,
        version="1.0",
        build="py_0",
        build_number=0,
        subdir="noarch",
        fn=f"{name}-1.0-py3-none-any.whl",
        noarch="python",
        depends=(),
        extra_depends={},
        name_resolutions=resolutions,
    )


def test_wheel_with_parsed_metadata_is_converted_and_stored_using_real_conversion(conn):
    _insert_wheel(conn, "example-1.0-py3-none-any.whl")

    stats = sync_reroll(conn, mappers=(passthrough_mapper,))

    assert stats.wheels_considered == 1
    assert stats.wheels_converted == 1
    assert stats.wheels_failed == 0
    assert stats.stopped_early is False

    skip_reason, repodata, name_conversions, reroll_version = _wheel_row(
        conn, "example-1.0-py3-none-any.whl"
    )
    assert skip_reason is None
    assert reroll_version == REROLL_VERSION
    records = json.loads(repodata)
    assert len(records) == 1
    assert records[0]["name"] == "example"
    assert records[0]["subdir"] == "noarch"
    assert "name_resolutions" not in records[0]
    conversions = json.loads(name_conversions)
    assert len(conversions) == 1
    assert conversions[0]["pypi_name"] == "example"
    assert conversions[0]["winner"]["conda_name"] == "example"


def test_mappers_are_preloaded_once_for_the_whole_run(conn, monkeypatch):
    _insert_wheel(conn, "example-1.0-py3-none-any.whl")
    _insert_wheel(
        conn,
        "other-1.0-py3-none-any.whl",
        wheel_metadata=json.dumps({"name": "other", "version": "1.0"}),
    )
    calls = []

    def fake_default_mappers():
        calls.append(())
        return (passthrough_mapper,)

    monkeypatch.setattr("reroll_sync.reroll_convert.default_mappers", fake_default_mappers)

    stats = sync_reroll(conn)

    assert stats.wheels_converted == 2
    assert len(calls) == 1


def test_wheels_without_parsed_metadata_are_not_considered(conn):
    _insert_wheel(conn, "example-1.0-py3-none-any.whl", wheel_metadata=None)

    stats = sync_reroll(conn, get_wheel_records=lambda *a, **k: pytest.fail("should not convert"))

    assert stats.wheels_considered == 0


def test_wheels_already_converted_are_not_reconsidered(conn):
    conn.execute(
        "INSERT INTO wheels "
        "(filename, project, pypi_simple, wheel_metadata, repodata, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            "example-1.0-py3-none-any.whl",
            "example",
            "{}",
            _VALID_METADATA,
            "[]",
            "2024-01-01T00:00:00+00:00",
        ),
    )
    conn.commit()

    stats = sync_reroll(conn, get_wheel_records=lambda *a, **k: pytest.fail("should not convert"))

    assert stats.wheels_considered == 0


def test_wheels_with_skip_reason_are_not_reconsidered(conn):
    conn.execute(
        "INSERT INTO wheels "
        "(filename, project, pypi_simple, wheel_metadata, skip_reason, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            "example-1.0-py3-none-any.whl",
            "example",
            "{}",
            _VALID_METADATA,
            "reroll_conversion_failed",
            "2024-01-01T00:00:00+00:00",
        ),
    )
    conn.commit()

    stats = sync_reroll(conn, get_wheel_records=lambda *a, **k: pytest.fail("should not convert"))

    assert stats.wheels_considered == 0


def test_reroll_error_is_recorded_as_error_and_permanently_skipped(conn):
    _insert_wheel(conn, "example-1.0-py3-none-any.whl")

    def get_wheel_records(metadata, filename, **kwargs):
        raise UnresolvedCondaNameError("example")

    stats = sync_reroll(conn, mappers=(passthrough_mapper,), get_wheel_records=get_wheel_records)

    assert stats.wheels_converted == 0
    assert stats.wheels_failed == 1

    skip_reason, repodata, name_conversions, reroll_version = _wheel_row(
        conn, "example-1.0-py3-none-any.whl"
    )
    assert skip_reason == "reroll_conversion_failed"
    assert repodata is None
    assert name_conversions is None
    assert reroll_version is None

    errors = _errors(conn)
    assert len(errors) == 1
    filename, category, subcategory, details, error_reroll_version = errors[0]
    assert filename == "example-1.0-py3-none-any.whl"
    assert category == "reroll_conversion_failed"
    assert subcategory == "UnresolvedCondaNameError"
    assert "example" in details
    assert error_reroll_version == REROLL_VERSION


def test_reroll_runtime_error_is_left_for_retry(conn):
    _insert_wheel(conn, "example-1.0-py3-none-any.whl")

    def get_wheel_records(metadata, filename, **kwargs):
        raise NetworkFetchError("could not resolve a name mapper")

    stats = sync_reroll(conn, mappers=(passthrough_mapper,), get_wheel_records=get_wheel_records)

    assert stats.wheels_converted == 0
    assert stats.wheels_failed == 0
    skip_reason, repodata, _name_conversions, _reroll_version = _wheel_row(
        conn, "example-1.0-py3-none-any.whl"
    )
    assert skip_reason is None
    assert repodata is None
    assert _errors(conn) == []


def test_limit_caps_number_of_wheels_processed(conn):
    for i in range(3):
        _insert_wheel(
            conn,
            f"pkg{i}-1.0-py3-none-any.whl",
            wheel_metadata=json.dumps({"name": f"pkg{i}", "version": "1.0"}),
        )
    processed: list = []

    def get_wheel_records(metadata, filename, **kwargs):
        processed.append(filename)
        return (_record(name=metadata.name),)

    stats = sync_reroll(
        conn, mappers=(passthrough_mapper,), limit=2, get_wheel_records=get_wheel_records
    )

    assert stats.wheels_considered == 2
    assert len(processed) == 2


def test_timeout_stops_processing_early(conn, monkeypatch):
    for i in range(2):
        _insert_wheel(
            conn,
            f"pkg{i}-1.0-py3-none-any.whl",
            wheel_metadata=json.dumps({"name": f"pkg{i}", "version": "1.0"}),
        )
    times = iter([0.0, 100.0])
    monkeypatch.setattr("reroll_sync.reroll_convert.time.monotonic", lambda: next(times))
    processed: list = []

    def get_wheel_records(metadata, filename, **kwargs):
        processed.append(filename)
        return (_record(),)

    stats = sync_reroll(
        conn, mappers=(passthrough_mapper,), timeout=5, get_wheel_records=get_wheel_records
    )

    assert processed == []
    assert stats.wheels_converted == 0
    assert stats.stopped_early is True


def test_allow_pre_defaults_to_false_and_is_passed_through(conn):
    _insert_wheel(conn, "example-1.0-py3-none-any.whl")
    seen_allow_pre = []

    def get_wheel_records(metadata, filename, *, allow_pre, **kwargs):
        seen_allow_pre.append(allow_pre)
        return (_record(),)

    sync_reroll(conn, mappers=(passthrough_mapper,), get_wheel_records=get_wheel_records)

    assert seen_allow_pre == [False]


def test_name_conversions_are_deduped_across_records_for_the_same_wheel(conn):
    _insert_wheel(conn, "example-1.0-py3-none-any.whl")
    resolution_a = NameResolution(pypi_name="numpy", winner=_winner("numpy"))
    resolution_b = NameResolution(pypi_name="numpy", winner=_winner("numpy"))
    records = (
        _record(resolutions=(resolution_a,)),
        _record(resolutions=(resolution_b,)),
    )

    sync_reroll(
        conn,
        mappers=(passthrough_mapper,),
        get_wheel_records=lambda *a, **k: records,
    )

    _skip_reason, _repodata, name_conversions, _reroll_version = _wheel_row(
        conn, "example-1.0-py3-none-any.whl"
    )
    conversions = json.loads(name_conversions)
    assert len(conversions) == 1
    assert conversions[0]["pypi_name"] == "numpy"
