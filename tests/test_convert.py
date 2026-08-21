import pickle

import pytest
import reroll
import reroll.stages
from reroll import NameResolution, WheelRecord, Winner
from reroll.errors import (
    ConfigLoadError,
    DatabaseError,
    InvalidFilenameError,
    NetworkFetchError,
    RerollInvalidWheelError,
    RerollScopeError,
    RerollUnconvertableError,
    UnconvertableRequirementError,
    UnexpectedError,
    UnsupportedPrereleaseError,
)
from reroll.name_mapping import CandidateSource, passthrough_mapper, static_mapper

from reroll_sync.convert import (
    PRERELEASE_RETRY_ERRORS,
    ConvertOk,
    ConvertRetry,
    ConvertSkip,
    convert,
    convert_in_worker,
    worker_init,
)
from reroll_sync.version import REROLL_VERSION

_MAPPERS = (passthrough_mapper,)


def _metadata_text(name="example", version="1.0", requires_dist=(), requires_python=None):
    lines = [f"Name: {name}", f"Version: {version}"]
    if requires_python is not None:
        lines.append(f"Requires-Python: {requires_python}")
    for requirement in requires_dist:
        lines.append(f"Requires-Dist: {requirement}")
    return "\n".join(lines) + "\n"


def _metadata_bytes(**kwargs):
    return _metadata_text(**kwargs).encode("utf-8")


def _winner(name):
    return Winner(conda_name=name, probability=0.0, source=CandidateSource.PASSTHROUGH, mapper="m")


def _record(name="example", subdir="noarch", resolutions=()):
    return WheelRecord(
        name=name,
        version="1.0",
        build="py_0",
        build_number=0,
        subdir=subdir,
        fn=f"{name}-1.0-py3-none-any.whl",
        noarch="python" if subdir == "noarch" else None,
        depends=(),
        extra_depends={},
        name_resolutions=resolutions,
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_valid_metadata_produces_convert_ok_with_nonempty_records():
    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )

    assert isinstance(outcome, ConvertOk)
    assert len(outcome.records) > 0


def test_requires_prerelease_is_false_when_first_attempt_succeeds():
    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )

    assert isinstance(outcome, ConvertOk)
    assert outcome.requires_prerelease is False


def test_conda_name_matches_the_expected_mapped_name():
    """Uses a real ``static_mapper`` override (not the passthrough chain
    every other test uses) so this proves an actual pypi-name ->
    different-conda-name mapping flows through into ``ConvertOk.conda_name``,
    not just identity.
    """
    mappers = (static_mapper({"example": "example-conda"}),)

    outcome = convert(
        _metadata_bytes(name="example"),
        "example-1.0-py3-none-any.whl",
        mappers=mappers,
        reroll_version=REROLL_VERSION,
    )

    assert isinstance(outcome, ConvertOk)
    assert outcome.conda_name == "example-conda"


def test_resolutions_are_deduped_and_sorted_across_records():
    resolution_a = NameResolution(pypi_name="numpy", winner=_winner("numpy"))
    resolution_b = NameResolution(pypi_name="numpy", winner=_winner("numpy"))
    resolution_c = NameResolution(pypi_name="attrs", winner=_winner("attrs"))
    records = (
        _record(resolutions=(resolution_a, resolution_c)),
        _record(resolutions=(resolution_b,)),
    )

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=lambda *a, **k: records,
    )

    assert isinstance(outcome, ConvertOk)
    assert [r.pypi_name for r in outcome.resolutions] == ["attrs", "numpy"]


def test_wheel_producing_multiple_records_returns_all_of_them():
    records = (_record(subdir="linux-64"), _record(subdir="linux-aarch64"))

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=lambda *a, **k: records,
    )

    assert isinstance(outcome, ConvertOk)
    assert len(outcome.records) == 2
    assert {r.subdir for r in outcome.records} == {"linux-64", "linux-aarch64"}


# ---------------------------------------------------------------------------
# Pre-release retry
# ---------------------------------------------------------------------------


def test_unsupported_prerelease_error_then_retry_success_sets_requires_prerelease():
    calls = []

    def get_wheel_records(metadata, filename, *, allow_pre, **kwargs):
        calls.append(allow_pre)
        if not allow_pre:
            raise UnsupportedPrereleaseError("rejected pre-release version")
        return (_record(),)

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=get_wheel_records,
    )

    assert isinstance(outcome, ConvertOk)
    assert outcome.requires_prerelease is True
    assert calls == [False, True]


def test_unconvertable_requirement_error_then_retry_success_sets_requires_prerelease():
    calls = []

    def get_wheel_records(metadata, filename, *, allow_pre, **kwargs):
        calls.append(allow_pre)
        if not allow_pre:
            raise UnconvertableRequirementError("pre-release version without allow_pre")
        return (_record(),)

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=get_wheel_records,
    )

    assert isinstance(outcome, ConvertOk)
    assert outcome.requires_prerelease is True
    assert calls == [False, True]


def test_unsupported_prerelease_error_then_retry_also_fails_records_first_error():
    def get_wheel_records(metadata, filename, *, allow_pre, **kwargs):
        if not allow_pre:
            raise UnsupportedPrereleaseError("rejected pre-release version")
        raise InvalidFilenameError("still bad")

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=get_wheel_records,
    )

    assert isinstance(outcome, ConvertSkip)
    assert outcome.subcategory == "UnsupportedPrereleaseError"
    assert "InvalidFilenameError" in outcome.details


def test_error_not_in_retry_set_causes_exactly_one_call():
    calls = []

    def get_wheel_records(metadata, filename, *, allow_pre, **kwargs):
        calls.append(allow_pre)
        raise InvalidFilenameError("bad filename")

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=get_wheel_records,
    )

    assert isinstance(outcome, ConvertSkip)
    assert calls == [False]


def test_first_attempt_called_with_allow_pre_false_and_retry_with_true():
    seen = []

    def get_wheel_records(metadata, filename, *, allow_pre, **kwargs):
        seen.append(allow_pre)
        if len(seen) == 1:
            raise UnsupportedPrereleaseError("nope")
        return (_record(),)

    convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=get_wheel_records,
    )

    assert seen == [False, True]


def test_reroll_runtime_error_on_first_attempt_does_not_retry():
    calls = []

    def get_wheel_records(metadata, filename, *, allow_pre, **kwargs):
        calls.append(allow_pre)
        raise NetworkFetchError("network is down")

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=get_wheel_records,
    )

    assert isinstance(outcome, ConvertRetry)
    assert calls == [False]


@pytest.mark.parametrize(
    ("filename", "version", "requires_dist", "expected_error"),
    [
        ("example-1.0.0rc1-py3-none-any.whl", "1.0.0rc1", (), UnsupportedPrereleaseError),
        (
            "example-1.0-py3-none-any.whl",
            "1.0",
            ("dep>=2.0.0rc1",),
            UnconvertableRequirementError,
        ),
    ],
)
def test_real_reroll_prerelease_fixture_pins_retry_error_type(
    filename, version, requires_dist, expected_error
):
    """Pins ``PRERELEASE_RETRY_ERRORS`` against real reroll behaviour: a
    pre-release wheel version raises ``UnsupportedPrereleaseError``, and a
    dependency pinning a pre-release raises ``UnconvertableRequirementError``
    -- both from a real ``get_wheel_records`` call, not a fake.
    """
    metadata = reroll.parse_metadata(_metadata_text(version=version, requires_dist=requires_dist))

    with pytest.raises(expected_error) as exc_info:
        reroll.stages.get_wheel_records(metadata, filename, mappers=_MAPPERS, allow_pre=False)
    assert isinstance(exc_info.value, PRERELEASE_RETRY_ERRORS)

    outcome = convert(
        _metadata_bytes(version=version, requires_dist=requires_dist),
        filename,
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )

    assert isinstance(outcome, ConvertOk)
    assert outcome.requires_prerelease is True


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


def test_invalid_utf8_bytes_produce_permanent_skip_with_no_reroll_version():
    outcome = convert(
        b"\xff\xfe not utf-8",
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )

    assert isinstance(outcome, ConvertSkip)
    assert outcome.permanent is True
    assert outcome.reroll_version is None
    assert outcome.subcategory == "UnicodeDecodeError"


@pytest.mark.parametrize(
    ("error_cls", "stage"),
    [
        (RerollScopeError, "parse"),
        (RerollInvalidWheelError, "parse"),
        (RerollUnconvertableError, "parse"),
        (RerollScopeError, "convert"),
        (RerollInvalidWheelError, "convert"),
        (RerollUnconvertableError, "convert"),
    ],
)
def test_each_reroll_error_category_maps_to_skip_with_subcategory(error_cls, stage):
    if stage == "parse":
        outcome = convert(
            _metadata_bytes(),
            "example-1.0-py3-none-any.whl",
            mappers=_MAPPERS,
            reroll_version=REROLL_VERSION,
            parse=lambda text: (_ for _ in ()).throw(error_cls("boom")),
        )
    else:
        outcome = convert(
            _metadata_bytes(),
            "example-1.0-py3-none-any.whl",
            mappers=_MAPPERS,
            reroll_version=REROLL_VERSION,
            get_wheel_records=lambda *a, **k: (_ for _ in ()).throw(error_cls("boom")),
        )

    assert isinstance(outcome, ConvertSkip)
    assert outcome.permanent is False
    assert outcome.reroll_version == REROLL_VERSION
    assert outcome.subcategory == error_cls.__name__


@pytest.mark.parametrize(
    "error_cls",
    [NetworkFetchError, DatabaseError, ConfigLoadError, UnexpectedError],
)
def test_each_reroll_runtime_error_leaf_maps_to_retry(error_cls):
    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=lambda *a, **k: (_ for _ in ()).throw(error_cls("boom")),
    )

    assert isinstance(outcome, ConvertRetry)


def test_runtime_error_during_parse_maps_to_retry():
    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        parse=lambda text: (_ for _ in ()).throw(NetworkFetchError("boom")),
    )

    assert isinstance(outcome, ConvertRetry)


def test_runtime_error_on_retry_attempt_maps_to_retry():
    def get_wheel_records(metadata, filename, *, allow_pre, **kwargs):
        if not allow_pre:
            raise UnsupportedPrereleaseError("nope")
        raise NetworkFetchError("host unstable during retry")

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=get_wheel_records,
    )

    assert isinstance(outcome, ConvertRetry)


def test_parse_failure_and_conversion_failure_have_distinguishable_reasons():
    parse_outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        parse=lambda text: (_ for _ in ()).throw(RerollScopeError("boom")),
    )
    convert_outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=lambda *a, **k: (_ for _ in ()).throw(RerollScopeError("boom")),
    )

    assert isinstance(parse_outcome, ConvertSkip)
    assert isinstance(convert_outcome, ConvertSkip)
    assert parse_outcome.reason != convert_outcome.reason


@pytest.mark.parametrize(
    "error_cls",
    [
        RerollScopeError,
        RerollInvalidWheelError,
        RerollUnconvertableError,
        NetworkFetchError,
        DatabaseError,
        ConfigLoadError,
        UnexpectedError,
    ],
)
def test_convert_raises_nothing_for_any_reroll_error(error_cls):
    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=lambda *a, **k: (_ for _ in ()).throw(error_cls("boom")),
    )

    assert outcome is not None


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_metadata_bytes_is_a_skip_not_a_crash():
    outcome = convert(
        b"",
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )

    assert isinstance(outcome, ConvertSkip)


def test_metadata_with_utf8_bom_parses_successfully():
    metadata_bytes = b"\xef\xbb\xbf" + _metadata_bytes()

    outcome = convert(
        metadata_bytes,
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )

    assert isinstance(outcome, ConvertOk)


def test_metadata_with_crlf_line_endings_parses_successfully():
    metadata_bytes = _metadata_text().replace("\n", "\r\n").encode("utf-8")

    outcome = convert(
        metadata_bytes,
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )

    assert isinstance(outcome, ConvertOk)


def test_records_disagreeing_on_package_name_returns_retry_not_a_silent_pick():
    records = (_record(name="example"), _record(name="other"))

    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=lambda *a, **k: records,
    )

    assert isinstance(outcome, ConvertRetry)


def test_zero_records_without_exception_is_a_retry():
    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=lambda *a, **k: (),
    )

    assert isinstance(outcome, ConvertRetry)


# ---------------------------------------------------------------------------
# Pickling
# ---------------------------------------------------------------------------


def test_convert_ok_round_trips_through_pickle():
    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )
    assert isinstance(outcome, ConvertOk)

    restored = pickle.loads(pickle.dumps(outcome))

    assert restored == outcome


def test_convert_skip_round_trips_through_pickle():
    outcome = convert(
        b"\xff\xfe not utf-8",
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
    )
    assert isinstance(outcome, ConvertSkip)

    restored = pickle.loads(pickle.dumps(outcome))

    assert restored == outcome


def test_convert_retry_round_trips_through_pickle():
    outcome = convert(
        _metadata_bytes(),
        "example-1.0-py3-none-any.whl",
        mappers=_MAPPERS,
        reroll_version=REROLL_VERSION,
        get_wheel_records=lambda *a, **k: (),
    )
    assert isinstance(outcome, ConvertRetry)

    restored = pickle.loads(pickle.dumps(outcome))

    assert restored == outcome


# ---------------------------------------------------------------------------
# Process pool
# ---------------------------------------------------------------------------


def test_worker_init_builds_mappers_exactly_once_for_n_calls(monkeypatch):
    calls = []

    def fake_default_mappers():
        calls.append(())
        return _MAPPERS

    monkeypatch.setattr("reroll_sync.convert.reroll.default_mappers", fake_default_mappers)

    worker_init(REROLL_VERSION)
    for _ in range(3):
        convert_in_worker(_metadata_bytes(), "example-1.0-py3-none-any.whl")

    assert len(calls) == 1


def test_convert_in_worker_uses_module_globals_set_by_worker_init(monkeypatch):
    monkeypatch.setattr("reroll_sync.convert.reroll.default_mappers", lambda: _MAPPERS)
    worker_init(REROLL_VERSION)

    outcome = convert_in_worker(_metadata_bytes(), "example-1.0-py3-none-any.whl")

    assert isinstance(outcome, ConvertOk)


def test_convert_in_worker_before_worker_init_raises_runtime_error(monkeypatch):
    monkeypatch.setattr("reroll_sync.convert._mappers", None)
    monkeypatch.setattr("reroll_sync.convert._reroll_version", None)

    with pytest.raises(RuntimeError):
        convert_in_worker(_metadata_bytes(), "example-1.0-py3-none-any.whl")
