"""Turn a wheel's raw ``METADATA`` bytes into repodata record(s) with reroll.

This is a pure function: no database, no network, no filesystem. Every
wheel-attributable failure is returned as an outcome rather than raised, and
the two-attempt pre-release retry policy lives here -- see
``PRERELEASE_RETRY_ERRORS``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import reroll
import reroll.stages
from reroll import NameMappers, NameResolution, WheelMetadata, WheelRecord
from reroll.errors import (
    RerollError,
    RerollRuntimeError,
    UnconvertableRequirementError,
    UnsupportedPrereleaseError,
)

from .daemon.logging_setup import silence_noisy_reroll_loggers

Parse = Callable[[str], WheelMetadata]
GetWheelRecords = Callable[..., tuple[WheelRecord, ...]]

PRERELEASE_RETRY_ERRORS = (UnsupportedPrereleaseError, UnconvertableRequirementError)
"""Exception types whose ``allow_pre=False`` failure is retried once with
``allow_pre=True``. Matched by ``isinstance``, never by message text --
see ``specs/05-convert-worker.md`` for why each leaf belongs here.
"""

_INVALID_METADATA_ENCODING = "invalid_metadata_encoding"
_INVALID_METADATA_CATEGORY = "invalid_metadata"
_PARSE_RUNTIME_ERROR = "parse_runtime_error"
_CONVERSION_ERROR_CATEGORY = "reroll_conversion_failed"
_CONVERSION_RUNTIME_ERROR = "conversion_runtime_error"
_NO_RECORDS_CATEGORY = "no_records_returned"
_CONDA_NAME_DISAGREEMENT = "conda_name_disagreement"


@dataclass(frozen=True)
class ConvertOk:
    """A wheel successfully converted to one or more repodata records."""

    records: tuple[WheelRecord, ...]
    resolutions: tuple[NameResolution, ...]
    conda_name: str
    requires_prerelease: bool


@dataclass(frozen=True)
class ConvertSkip:
    """A wheel-attributable failure: this wheel will never convert as-is."""

    reason: str
    subcategory: str
    details: str
    permanent: bool
    reroll_version: str | None


@dataclass(frozen=True)
class ConvertRetry:
    """A failure that says nothing about the wheel; retry later."""

    reason: str
    details: str


ConvertOutcome = ConvertOk | ConvertSkip | ConvertRetry

_mappers: NameMappers | None = None
_reroll_version: str | None = None


def convert(
    metadata_bytes: bytes,
    filename: str,
    *,
    mappers: NameMappers,
    reroll_version: str,
    parse: Parse = reroll.parse_metadata,
    get_wheel_records: GetWheelRecords = reroll.stages.get_wheel_records,
) -> ConvertOutcome:
    """Convert one wheel's ``METADATA`` bytes plus its filename into an
    outcome. Never raises for a wheel-attributable failure.

    ``mappers`` is required rather than defaulted: building the default
    chain reloads config and network-backed lookup tables from scratch, so
    callers build it once per worker process (see ``worker_init``) rather
    than once per wheel.

    Conversion is attempted with ``allow_pre=False`` first. If it fails
    with a member of ``PRERELEASE_RETRY_ERRORS``, it is retried once with
    ``allow_pre=True``; a retry success sets ``requires_prerelease=True`` on
    the outcome, and a retry failure records the *first* attempt's error
    (annotating ``details`` with the retry's exception type) since that
    error describes the wheel under the policy the system actually wants.
    """
    try:
        text = metadata_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        return ConvertSkip(
            reason=_INVALID_METADATA_ENCODING,
            subcategory=type(exc).__name__,
            details=str(exc),
            permanent=True,
            reroll_version=None,
        )

    try:
        metadata = parse(text)
    except RerollRuntimeError as exc:
        return ConvertRetry(reason=_PARSE_RUNTIME_ERROR, details=str(exc))
    except RerollError as exc:
        return ConvertSkip(
            reason=_INVALID_METADATA_CATEGORY,
            subcategory=type(exc).__name__,
            details=str(exc),
            permanent=False,
            reroll_version=reroll_version,
        )

    requires_prerelease = False
    try:
        records = get_wheel_records(metadata, filename, mappers=mappers, allow_pre=False)
    except RerollRuntimeError as exc:
        return ConvertRetry(reason=_CONVERSION_RUNTIME_ERROR, details=str(exc))
    except PRERELEASE_RETRY_ERRORS as first_error:
        try:
            records = get_wheel_records(metadata, filename, mappers=mappers, allow_pre=True)
        except RerollRuntimeError as retry_error:
            return ConvertRetry(reason=_CONVERSION_RUNTIME_ERROR, details=str(retry_error))
        except RerollError as retry_error:
            return ConvertSkip(
                reason=_CONVERSION_ERROR_CATEGORY,
                subcategory=type(first_error).__name__,
                details=f"{first_error} "
                f"(retry with allow_pre=True failed with "
                f"{type(retry_error).__name__}: {retry_error})",
                permanent=False,
                reroll_version=reroll_version,
            )
        else:
            requires_prerelease = True
    except RerollError as exc:
        return ConvertSkip(
            reason=_CONVERSION_ERROR_CATEGORY,
            subcategory=type(exc).__name__,
            details=str(exc),
            permanent=False,
            reroll_version=reroll_version,
        )

    return _outcome_from_records(
        records, requires_prerelease=requires_prerelease, reroll_version=reroll_version
    )


def worker_init(reroll_version: str) -> None:
    """``ProcessPoolExecutor`` initializer: builds ``default_mappers()``
    once per process and stashes it (plus ``reroll_version``) in a module
    global for ``convert_in_worker`` to reuse across every wheel the
    process converts. Also silences reroll's noisy per-wheel loggers,
    since a worker process started via ``spawn`` never inherits the main
    process's ``configure_logging`` call.
    """
    global _mappers, _reroll_version
    silence_noisy_reroll_loggers()
    _mappers = reroll.default_mappers()
    _reroll_version = reroll_version


def convert_in_worker(metadata_bytes: bytes, filename: str) -> ConvertOutcome:
    """Thin wrapper around ``convert`` that reads the mappers and reroll
    version ``worker_init`` built for this process.
    """
    if _mappers is None or _reroll_version is None:
        raise RuntimeError("convert_in_worker called before worker_init")
    return convert(metadata_bytes, filename, mappers=_mappers, reroll_version=_reroll_version)


def _outcome_from_records(
    records: tuple[WheelRecord, ...], *, requires_prerelease: bool, reroll_version: str
) -> ConvertOutcome:
    if not records:
        return ConvertRetry(
            reason=_NO_RECORDS_CATEGORY, details="get_wheel_records returned no records"
        )
    conda_names = {record.name for record in records}
    if len(conda_names) != 1:
        return ConvertRetry(
            reason=_CONDA_NAME_DISAGREEMENT,
            details=f"records disagree on conda_name: {sorted(conda_names)!r}",
        )
    (conda_name,) = conda_names
    return ConvertOk(
        records=records,
        resolutions=_deduped_resolutions(records),
        conda_name=conda_name,
        requires_prerelease=requires_prerelease,
    )


def _deduped_resolutions(records: tuple[WheelRecord, ...]) -> tuple[NameResolution, ...]:
    """One ``NameResolution`` per unique PyPI name resolved across all of
    ``records`` -- deduped, since the same name can be resolved by more
    than one of a wheel's records (e.g. one per supported platform).
    """
    seen: dict[str, NameResolution] = {}
    for record in records:
        for resolution in record.resolutions:
            seen.setdefault(resolution.pypi_name, resolution)
    return tuple(seen[name] for name in sorted(seen))
