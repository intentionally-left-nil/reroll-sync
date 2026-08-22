"""Polls the PyPI simple index and reconciles stale projects' ``wheels`` rows.

Replaces ``sync.py``'s insert-only algorithm with diff-and-tombstone: new
files are inserted, changed columns are updated (a yank flip or a metadata
hash appearing never resets ``state`` unless the changed value itself
invalidates an archived blob), and files or projects gone from the index are
tombstoned, never hard-deleted.

Two design decisions this module makes explicit, per spec 08:

* Project-scoped retry/backoff has no persisted home. ``work`` is keyed by
  ``(wheel_id, stage)``, and one project reconciliation touches many wheels
  at once, so it does not fit without bending that table's shape. A new
  ``project_work`` table would be a *tenth* Phase 1 table; spec 01 closes
  the table list at nine (plus the already-flagged ``unlinked_blobs``
  exception) without further sign-off. :class:`ProjectBackoff` therefore
  keeps retry state in memory only, reusing ``dispatcher.compute_backoff``
  for the delay math. A process restart loses this state and retries
  immediately, which is safe because ``poll_index``'s staleness check
  (remote serial vs. stored serial) is idempotent -- it costs an extra
  fetch, not correctness. Flagged to the owner rather than adding the
  table.
* The simple index's ``etag`` and global ``_last-serial`` are likewise not
  persisted: there is no column for an opaque etag string, and adding one
  is the same kind of out-of-scope schema change. ``poll_index`` takes
  ``etag`` as a parameter and returns the newly-seen one on
  :class:`PollResult`; the caller carries it forward across polls (in
  memory today; a future daemon spec may persist it elsewhere). The
  "stored global serial" used for the fast-path-missed check is derived as
  ``MAX(pypi_index.serial)`` instead of stored separately, the same trick
  ``Writer.start()`` already uses to rederive ``change_seq``.
"""

from __future__ import annotations

import logging
import random
import sqlite3
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx

from .dispatcher import DEFAULT_MAX_ATTEMPTS, compute_backoff
from .pypi_client import (
    ProjectFile,
    PyPIClient,
    PyPINotFound,
    PyPIProtocolError,
    PyPIRateLimited,
    PyPITransientError,
)
from .schema import WheelState
from .version import REROLL_VERSION
from .writer import ReadTxnWatchdog, WriteOp, Writer, read_txn

logger = logging.getLogger(__name__)

NO_SIDECAR_SKIP_REASON = "no_sidecar"
"""``skips.reason`` used by the (future) fetch stage for "PyPI publishes no sidecar".

Deleted here when ``has_metadata`` flips ``False`` -> ``True``, per spec 08.
"""

DEFAULT_SERIAL_MAP_CHUNK_SIZE = 5000
DEFAULT_PROJECT_CHUNK_SIZE = 5000
DEFAULT_UNLINKED_BLOB_CHUNK_SIZE = 500
DEFAULT_GONE_CHUNK_SIZE = 5000
DEFAULT_MAX_WORKERS = 32
DEFAULT_READ_BUDGET = 0.25


class Penalizer(Protocol):
    """The subset of :class:`~reroll_sync.ratelimit.HierarchicalLimiter` this module needs."""

    def penalize(self, child_name: str, seconds: float) -> None:
        """Refuse acquisitions for ``child_name`` for ``seconds``.

        See ``HierarchicalLimiter.penalize``.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class PollResult:
    """The outcome of one :func:`poll_index` call."""

    not_modified: bool
    etag: str | None
    remote_global_serial: int | None
    stale_projects: tuple[str, ...]


def poll_index(
    client: PyPIClient,
    conn: sqlite3.Connection,
    *,
    etag: str | None,
    chunk_size: int = DEFAULT_SERIAL_MAP_CHUNK_SIZE,
    read_budget: float = DEFAULT_READ_BUDGET,
    watchdog: ReadTxnWatchdog | None = None,
) -> PollResult:
    """Conditionally fetch the simple index and compute which projects are stale.

    A ``304`` short-circuits with ``not_modified=True`` and an empty
    ``stale_projects``. A ``200`` compares the response's global serial
    against ``MAX(pypi_index.serial)`` (logging if unchanged -- a project
    can still have changed without the global counter moving), then reads
    the local ``name -> serial`` map in ``chunk_size``-row pages, each its
    own bounded :func:`~reroll_sync.writer.read_txn`, and returns every
    project that is absent locally or whose remote serial exceeds the local
    one. A project whose remote serial is *lower* than local is left alone
    and logged, never treated as stale.
    """
    index = client.fetch_simple_index(etag)
    if index.not_modified:
        return PollResult(
            not_modified=True, etag=index.etag, remote_global_serial=None, stale_projects=()
        )

    with read_txn(
        conn, budget=read_budget, label="ingest.poll_index.global_serial", watchdog=watchdog
    ):
        (local_global_serial,) = conn.execute("SELECT MAX(serial) FROM pypi_index").fetchone()
    if local_global_serial is not None and index.last_serial == local_global_serial:
        logger.info(
            "simple index fast path missed: remote _last-serial %d unchanged from local max",
            index.last_serial,
        )

    local_serials = _read_local_serials(
        conn, chunk_size=chunk_size, budget=read_budget, watchdog=watchdog
    )
    stale = _stale_projects(index.projects, local_serials)
    return PollResult(
        not_modified=False,
        etag=index.etag,
        remote_global_serial=index.last_serial,
        stale_projects=stale,
    )


@dataclass(frozen=True)
class _NewWheel:
    """One ``.whl`` file present remotely but absent locally."""

    filename: str
    url: str
    wheel_sha256: str | None
    metadata_sha256: str | None
    size: int | None
    upload_time: str | None
    requires_python: str | None
    yanked: bool
    yanked_reason: str | None
    state: WheelState
    blob_sha256: str | None
    unlinked_blob_filename: str | None


@dataclass(frozen=True)
class _ChangedWheel:
    """One ``.whl`` file present both remotely and locally, with a stored diff."""

    wheel_id: int
    changes: dict[str, Any]
    delete_no_sidecar_skip: bool
    delete_repodata: bool


@dataclass(frozen=True)
class ProjectPlan:
    """Every write needed to make local ``wheels`` rows match one project page."""

    project: str
    remote_serial: int
    new_wheels: tuple[_NewWheel, ...]
    changed_wheels: tuple[_ChangedWheel, ...]
    vanished_wheel_ids: tuple[int, ...]


@dataclass(frozen=True)
class SyncOk:
    """Reconciliation succeeded: ``plan`` describes every write needed."""

    plan: ProjectPlan


@dataclass(frozen=True)
class SyncGone:
    """The project page returned 404/410: PyPI has deleted the project."""

    project: str


@dataclass(frozen=True)
class SyncRetry:
    """A transient failure fetching the project page; retry later, serial unchanged."""

    reason: str
    details: str


@dataclass(frozen=True)
class SyncRateLimited:
    """The fetch was throttled; not an attempt against the project at all."""

    child: str
    seconds: float


ProjectSyncOutcome = SyncOk | SyncGone | SyncRetry | SyncRateLimited
"""The project-reconciliation analogue of ``dispatcher.Outcome``, keyed by project name."""


def sync_project(
    client: PyPIClient,
    conn: sqlite3.Connection,
    name: str,
    *,
    now: Callable[[], float] = time.time,
    project_chunk_size: int = DEFAULT_PROJECT_CHUNK_SIZE,
    unlinked_blob_chunk_size: int = DEFAULT_UNLINKED_BLOB_CHUNK_SIZE,
    read_budget: float = DEFAULT_READ_BUDGET,
    watchdog: ReadTxnWatchdog | None = None,
) -> ProjectSyncOutcome:
    """Fetch project ``name``'s page and diff it against locally stored ``wheels`` rows.

    Pure fetch-and-compute: reads ``conn`` (a reader connection distinct
    from the writer's) but never writes. Filters to ``.whl`` files only.
    Returns a :data:`ProjectSyncOutcome` describing the write (if any) that
    :func:`apply_project_outcome` should submit to the writer.
    """
    try:
        page = client.fetch_project(name)
    except PyPINotFound:
        return SyncGone(project=name)
    except PyPIRateLimited as exc:
        return SyncRateLimited(
            child=_limiter_child_for(client), seconds=exc.retry_after if exc.retry_after else 0.0
        )
    except PyPITransientError as exc:
        return SyncRetry(reason="transient", details=str(exc))
    except PyPIProtocolError as exc:
        logger.error("protocol error fetching project %r: %s", name, exc)
        return SyncRetry(reason="protocol_error", details=str(exc))

    remote_files = {file.filename: file for file in page.files if file.filename.endswith(".whl")}
    local_rows = _read_project_wheels(
        conn, name, chunk_size=project_chunk_size, budget=read_budget, watchdog=watchdog
    )

    new_filenames = remote_files.keys() - local_rows.keys()
    common_filenames = remote_files.keys() & local_rows.keys()
    vanished_filenames = local_rows.keys() - remote_files.keys()

    unlinked = (
        _read_unlinked_blobs(
            conn,
            sorted(new_filenames),
            chunk_size=unlinked_blob_chunk_size,
            budget=read_budget,
            watchdog=watchdog,
        )
        if new_filenames
        else {}
    )
    new_wheels = tuple(
        _plan_new_wheel(remote_files[filename], unlinked.get(filename))
        for filename in sorted(new_filenames)
    )

    changed_wheels = tuple(
        changed
        for filename in sorted(common_filenames)
        if (changed := _diff_common(local_rows[filename], remote_files[filename])) is not None
    )

    vanished_wheel_ids = tuple(
        local_rows[filename].id
        for filename in sorted(vanished_filenames)
        if local_rows[filename].deleted_at is None
    )

    plan = ProjectPlan(
        project=name,
        remote_serial=page.last_serial,
        new_wheels=new_wheels,
        changed_wheels=changed_wheels,
        vanished_wheel_ids=vanished_wheel_ids,
    )
    return SyncOk(plan=plan)


@dataclass(frozen=True)
class ProjectApplyResult:
    """Counts from applying one project's :data:`ProjectSyncOutcome`."""

    inserted: int
    updated: int
    tombstoned: int
    project_gone: bool = False


def apply_project_outcome(
    writer: Writer,
    limiter: Penalizer | None,
    name: str,
    outcome: ProjectSyncOutcome,
    *,
    now: Callable[[], float] = time.time,
    reroll_version: str = REROLL_VERSION,
) -> ProjectApplyResult | None:
    """Turn ``outcome`` into a write submitted to ``writer``, or a limiter penalty.

    Returns ``None`` for :class:`SyncRetry`/:class:`SyncRateLimited`, which
    perform no database write, mirroring ``dispatcher.apply_outcome``'s
    treatment of ``RateLimited``.
    """
    if isinstance(outcome, SyncRateLimited):
        if limiter is not None:
            limiter.penalize(outcome.child, outcome.seconds)
        return None
    if isinstance(outcome, SyncRetry):
        return None

    now_iso = _iso(now())
    if isinstance(outcome, SyncGone):
        op = _build_gone_write_op(name, writer=writer, now_iso=now_iso)
    else:
        op = _build_project_write_op(
            outcome.plan, writer=writer, now_iso=now_iso, reroll_version=reroll_version
        )
    return writer.submit_and_wait(op)


@dataclass(frozen=True)
class IngestSummary:
    """Aggregate counts from one :func:`ingest_stale_projects` call."""

    inserted: int
    updated: int
    tombstoned: int
    projects_gone: int
    retried: tuple[str, ...]
    rate_limited: tuple[str, ...]
    quarantined_projects: int


class ProjectBackoff:
    """In-memory retry/backoff schedule for project-scoped sync failures.

    Not persisted -- process-lifetime only; see the module docstring for
    why. Reuses ``dispatcher.compute_backoff`` for the delay so project
    reconciliation backs off on the same curve as every other stage.
    Guarded by an internal lock, consistent with ``writer.Writer``'s own
    protection of its shared counters -- every mutating/reading method may
    be called from more than one thread.
    """

    def __init__(
        self,
        *,
        now: Callable[[], float] = time.time,
        rng: random.Random | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self._now = now
        self._rng = rng if rng is not None else random.Random()
        self._max_attempts = max_attempts
        self._lock = threading.Lock()
        self._attempts: dict[str, int] = {}
        self._next_attempt_at: dict[str, float] = {}
        self._quarantined: set[str] = set()

    def record_failure(self, project: str) -> float:
        """Record a failed attempt for ``project``, returning its next-eligible time.

        Logs at ERROR, naming ``project`` and its attempt count, the first
        time this call pushes ``project`` past ``max_attempts`` into
        quarantine -- mirroring ``dispatcher.py``'s own quarantine logging.
        """
        with self._lock:
            attempts = self._attempts.get(project, 0) + 1
            self._attempts[project] = attempts
            newly_quarantined = attempts > self._max_attempts and project not in self._quarantined
            if newly_quarantined:
                self._quarantined.add(project)
            next_at = self._now() + compute_backoff(attempts, rng=self._rng)
            self._next_attempt_at[project] = next_at
        if newly_quarantined:
            logger.error("project %r quarantined after %d attempts", project, attempts)
        return next_at

    def record_success(self, project: str) -> None:
        """Clear any retry state for ``project`` after a successful (or terminal) outcome."""
        with self._lock:
            self._attempts.pop(project, None)
            self._next_attempt_at.pop(project, None)
            self._quarantined.discard(project)

    def is_eligible(self, project: str) -> bool:
        """Return whether ``project`` may be attempted now."""
        with self._lock:
            if project in self._quarantined:
                return False
            next_at = self._next_attempt_at.get(project)
            return next_at is None or self._now() >= next_at

    def attempts(self, project: str) -> int:
        """Return the number of failures recorded for ``project`` (0 if never failed)."""
        with self._lock:
            return self._attempts.get(project, 0)

    def quarantined(self) -> frozenset[str]:
        """Return the set of projects that exhausted their retry budget."""
        with self._lock:
            return frozenset(self._quarantined)


def ingest_stale_projects(
    client: PyPIClient,
    reader_conn_factory: Callable[[], sqlite3.Connection],
    writer: Writer,
    limiter: Penalizer | None,
    project_names: Sequence[str],
    *,
    now: Callable[[], float] = time.time,
    max_workers: int = DEFAULT_MAX_WORKERS,
    reroll_version: str = REROLL_VERSION,
    backoff: ProjectBackoff | None = None,
) -> IngestSummary:
    """Reconcile every one of ``project_names``, fetching concurrently, applying serially.

    ``project_names`` is first filtered through ``backoff.is_eligible`` (when
    ``backoff`` is given): a project currently quarantined or inside its
    backoff window is skipped entirely for this call -- neither fetched nor
    counted as an attempt -- rather than refetched on every poll regardless
    of a prior failure. Each worker thread opens its own reader connection
    (via ``reader_conn_factory``) to fetch and diff one eligible project --
    see :func:`sync_project` -- and touches nothing else. Every write is
    submitted to ``writer`` from this function's own thread once a worker's
    result is ready, so ``writer``'s connection is touched by exactly one
    thread, as required. Bounded to ``max_workers`` concurrent fetches
    against the ``pypi.org`` limiter child.
    """
    inserted = updated = tombstoned = projects_gone = quarantined_projects = 0
    retried: list[str] = []
    rate_limited: list[str] = []

    eligible_names = (
        [name for name in project_names if backoff.is_eligible(name)]
        if backoff is not None
        else list(project_names)
    )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_and_sync_project, client, reader_conn_factory, name, now): name
            for name in eligible_names
        }
        for future in as_completed(futures):
            name = futures[future]
            outcome = future.result()
            result = apply_project_outcome(
                writer, limiter, name, outcome, now=now, reroll_version=reroll_version
            )
            if isinstance(outcome, SyncRetry):
                retried.append(name)
                if backoff is not None:
                    was_quarantined = name in backoff.quarantined()
                    backoff.record_failure(name)
                    if not was_quarantined and name in backoff.quarantined():
                        quarantine_op = _build_quarantine_error_write_op(
                            name,
                            backoff.attempts(name),
                            now_iso=_iso(now()),
                            reroll_version=reroll_version,
                        )
                        writer.submit_and_wait(quarantine_op)
                        quarantined_projects += 1
            elif isinstance(outcome, SyncRateLimited):
                rate_limited.append(name)
            else:
                if backoff is not None:
                    backoff.record_success(name)
                assert result is not None
                inserted += result.inserted
                updated += result.updated
                tombstoned += result.tombstoned
                if result.project_gone:
                    projects_gone += 1

    return IngestSummary(
        inserted=inserted,
        updated=updated,
        tombstoned=tombstoned,
        projects_gone=projects_gone,
        retried=tuple(retried),
        rate_limited=tuple(rate_limited),
        quarantined_projects=quarantined_projects,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _LocalWheelRow:
    """One ``wheels`` row's ingestion-relevant columns, keyed by filename by the caller."""

    id: int
    state: WheelState
    url: str
    wheel_sha256: str | None
    metadata_sha256: str | None
    size: int | None
    upload_time: str | None
    requires_python: str | None
    yanked: bool
    yanked_reason: str | None
    blob_sha256: str | None
    deleted_at: str | None


def _read_local_serials(
    conn: sqlite3.Connection,
    *,
    chunk_size: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> dict[str, int]:
    """Read the whole ``pypi_index`` ``name -> serial`` map in bounded, keyset-paginated pages."""
    serials: dict[str, int] = {}
    last_name = ""
    while True:
        with read_txn(conn, budget=budget, label="ingest.poll_index.serial_map", watchdog=watchdog):
            rows = conn.execute(
                "SELECT name, serial FROM pypi_index WHERE name > ? ORDER BY name LIMIT ?",
                (last_name, chunk_size),
            ).fetchall()
        if not rows:
            break
        for row_name, row_serial in rows:
            serials[row_name] = row_serial
        last_name = rows[-1][0]
        if len(rows) < chunk_size:
            break
    return serials


def _stale_projects(
    remote_projects: tuple[Any, ...], local_serials: dict[str, int]
) -> tuple[str, ...]:
    """Return remote projects absent locally or with a remote serial greater than local's.

    A remote serial *lower* than the stored one is left alone and logged --
    serials should never go backwards, so this is worth a warning, not a
    resync.
    """
    stale: list[str] = []
    for project in remote_projects:
        local_serial = local_serials.get(project.name)
        if local_serial is None or project.serial > local_serial:
            stale.append(project.name)
        elif project.serial < local_serial:
            logger.warning(
                "project %r remote serial %d is lower than stored serial %d",
                project.name,
                project.serial,
                local_serial,
            )
    return tuple(stale)


def _read_project_wheels(
    conn: sqlite3.Connection,
    project: str,
    *,
    chunk_size: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> dict[str, _LocalWheelRow]:
    """Read every ``wheels`` row for ``project`` via ``ix_wheels_project``, in bounded pages."""
    rows_by_filename: dict[str, _LocalWheelRow] = {}
    last_id = 0
    columns = (
        "id, filename, state, url, wheel_sha256, metadata_sha256, size, upload_time, "
        "requires_python, yanked, yanked_reason, blob_sha256, deleted_at"
    )
    while True:
        with read_txn(
            conn, budget=budget, label="ingest.sync_project.read_wheels", watchdog=watchdog
        ):
            rows = conn.execute(
                f"SELECT {columns} FROM wheels WHERE project = ? AND id > ? ORDER BY id LIMIT ?",
                (project, last_id, chunk_size),
            ).fetchall()
        if not rows:
            break
        for row in rows:
            (
                wheel_id,
                filename,
                state,
                url,
                wheel_sha256,
                metadata_sha256,
                size,
                upload_time,
                requires_python,
                yanked,
                yanked_reason,
                blob_sha256,
                deleted_at,
            ) = row
            rows_by_filename[filename] = _LocalWheelRow(
                id=wheel_id,
                state=WheelState(state),
                url=url,
                wheel_sha256=wheel_sha256,
                metadata_sha256=metadata_sha256,
                size=size,
                upload_time=upload_time,
                requires_python=requires_python,
                yanked=bool(yanked),
                yanked_reason=yanked_reason,
                blob_sha256=blob_sha256,
                deleted_at=deleted_at,
            )
        last_id = rows[-1][0]
        if len(rows) < chunk_size:
            break
    return rows_by_filename


def _read_unlinked_blobs(
    conn: sqlite3.Connection,
    filenames: list[str],
    *,
    chunk_size: int,
    budget: float,
    watchdog: ReadTxnWatchdog | None,
) -> dict[str, str]:
    """Look up ``filenames`` in ``unlinked_blobs`` (a primary-key hit), chunked."""
    result: dict[str, str] = {}
    for start in range(0, len(filenames), chunk_size):
        chunk = filenames[start : start + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        with read_txn(
            conn, budget=budget, label="ingest.sync_project.unlinked_blobs", watchdog=watchdog
        ):
            rows = conn.execute(
                f"SELECT filename, sha256 FROM unlinked_blobs WHERE filename IN ({placeholders})",
                chunk,
            ).fetchall()
        result.update(rows)
    return result


def _plan_new_wheel(file: ProjectFile, unlinked_sha256: str | None) -> _NewWheel:
    """Apply the initial-state rule, checking ``unlinked_blobs`` first (spec 13's self-heal)."""
    if unlinked_sha256 is not None:
        state = WheelState.NEED_CONVERT
        blob_sha256 = unlinked_sha256
        unlinked_blob_filename = file.filename
    else:
        state = WheelState.NEED_METADATA if file.has_metadata else WheelState.NO_METADATA
        blob_sha256 = None
        unlinked_blob_filename = None
    return _NewWheel(
        filename=file.filename,
        url=file.url,
        wheel_sha256=file.wheel_sha256,
        metadata_sha256=file.metadata_sha256,
        size=file.size,
        upload_time=file.upload_time,
        requires_python=file.requires_python,
        yanked=file.yanked,
        yanked_reason=file.yanked_reason,
        state=state,
        blob_sha256=blob_sha256,
        unlinked_blob_filename=unlinked_blob_filename,
    )


def _diff_common(local: _LocalWheelRow, file: ProjectFile) -> _ChangedWheel | None:
    """Compute the column-level diff for a filename present both locally and remotely.

    Returns ``None`` (zero write) if nothing changed. A tombstoned row
    reappearing takes priority over every other rule and refreshes every
    column, per spec 08.
    """
    if local.deleted_at is not None:
        new_state = WheelState.NEED_METADATA if file.has_metadata else WheelState.NO_METADATA
        changes: dict[str, Any] = {
            "deleted_at": None,
            "state": int(new_state),
            "url": file.url,
            "wheel_sha256": file.wheel_sha256,
            "metadata_sha256": file.metadata_sha256,
            "size": file.size,
            "upload_time": file.upload_time,
            "requires_python": file.requires_python,
            "yanked": int(file.yanked),
            "yanked_reason": file.yanked_reason,
            "blob_sha256": None,
        }
        return _ChangedWheel(
            wheel_id=local.id,
            changes=changes,
            delete_no_sidecar_skip=False,
            delete_repodata=False,
        )

    changes = {}
    new_state = local.state
    delete_no_sidecar_skip = False
    delete_repodata = False

    if local.yanked != file.yanked or local.yanked_reason != file.yanked_reason:
        changes["yanked"] = int(file.yanked)
        changes["yanked_reason"] = file.yanked_reason

    if local.metadata_sha256 != file.metadata_sha256:
        changes["metadata_sha256"] = file.metadata_sha256

    if (
        local.blob_sha256 is not None
        and file.metadata_sha256 is not None
        and file.metadata_sha256 != local.blob_sha256
    ):
        # The authoritative "what's actually archived" value is
        # blob_sha256 (the verified hash of the archived METADATA bytes,
        # per schema.py's docstring on blobs.sha256), not
        # local.metadata_sha256 -- a spec-13 self-healed row can have
        # blob_sha256 set while metadata_sha256 stays None, so comparing
        # against metadata_sha256 alone would miss a hash change on such a
        # row once PyPI starts publishing one.
        logger.warning(
            "wheel id %d: metadata_sha256 %r does not match archived blob_sha256 %r; "
            "clearing blob_sha256 and re-fetching",
            local.id,
            file.metadata_sha256,
            local.blob_sha256,
        )
        new_state = WheelState.NEED_METADATA
        changes["blob_sha256"] = None
        changes["conda_name"] = None
        delete_repodata = True

    if local.state == WheelState.NO_METADATA and file.has_metadata:
        new_state = WheelState.NEED_METADATA
        delete_no_sidecar_skip = True

    if local.url != file.url:
        changes["url"] = file.url
    if local.wheel_sha256 != file.wheel_sha256:
        changes["wheel_sha256"] = file.wheel_sha256
    if local.size != file.size:
        changes["size"] = file.size
    if local.upload_time != file.upload_time:
        changes["upload_time"] = file.upload_time
    if local.requires_python != file.requires_python:
        changes["requires_python"] = file.requires_python

    if new_state != local.state:
        changes["state"] = int(new_state)

    if not changes:
        return None
    return _ChangedWheel(
        wheel_id=local.id,
        changes=changes,
        delete_no_sidecar_skip=delete_no_sidecar_skip,
        delete_repodata=delete_repodata,
    )


def _build_project_write_op(
    plan: ProjectPlan, *, writer: Writer, now_iso: str, reroll_version: str
) -> WriteOp:
    """Build the single ``WriteOp`` that applies ``plan`` and upserts ``pypi_index`` atomically."""

    def _apply(conn: sqlite3.Connection) -> ProjectApplyResult:
        inserted = 0
        for new_wheel in plan.new_wheels:
            seq = writer.next_seq()
            try:
                conn.execute(
                    "INSERT INTO wheels (filename, project, state, lane, url, wheel_sha256, "
                    "metadata_sha256, size, upload_time, requires_python, yanked, "
                    "yanked_reason, blob_sha256, serial, change_seq, updated_at) "
                    "VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        new_wheel.filename,
                        plan.project,
                        int(new_wheel.state),
                        new_wheel.url,
                        new_wheel.wheel_sha256,
                        new_wheel.metadata_sha256,
                        new_wheel.size,
                        new_wheel.upload_time,
                        new_wheel.requires_python,
                        int(new_wheel.yanked),
                        new_wheel.yanked_reason,
                        new_wheel.blob_sha256,
                        plan.remote_serial,
                        seq,
                        now_iso,
                    ),
                )
            except sqlite3.IntegrityError:
                _record_duplicate_filename(
                    conn, new_wheel.filename, plan.project, now_iso, reroll_version
                )
                continue
            if new_wheel.unlinked_blob_filename is not None:
                conn.execute(
                    "DELETE FROM unlinked_blobs WHERE filename = ?",
                    (new_wheel.unlinked_blob_filename,),
                )
            inserted += 1

        updated = 0
        for change in plan.changed_wheels:
            seq = writer.next_seq()
            set_sql = ", ".join(f"{column} = ?" for column in change.changes)
            params = [*change.changes.values(), seq, now_iso, change.wheel_id]
            conn.execute(
                f"UPDATE wheels SET {set_sql}, change_seq = ?, updated_at = ? WHERE id = ?", params
            )
            if change.delete_no_sidecar_skip:
                # Spec 08: only a *permanent* no-sidecar skip is deleted here.
                # No fetch-stage code exists yet to establish a `stage`-scoping
                # convention for this reason string, so that filter is not
                # added until one does.
                conn.execute(
                    "DELETE FROM skips WHERE wheel_id = ? AND reason = ? AND permanent = 1",
                    (change.wheel_id, NO_SIDECAR_SKIP_REASON),
                )
            if change.delete_repodata:
                conn.execute("DELETE FROM wheel_repodata WHERE wheel_id = ?", (change.wheel_id,))
            updated += 1

        tombstoned = 0
        for wheel_id in plan.vanished_wheel_ids:
            seq = writer.next_seq()
            conn.execute(
                "UPDATE wheels SET deleted_at = ?, state = ?, change_seq = ?, updated_at = ? "
                "WHERE id = ?",
                (now_iso, int(WheelState.DELETED), seq, now_iso, wheel_id),
            )
            tombstoned += 1

        conn.execute(
            "INSERT INTO pypi_index (name, serial, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET "
            "serial = excluded.serial, updated_at = excluded.updated_at",
            (plan.project, plan.remote_serial, now_iso),
        )
        return ProjectApplyResult(inserted=inserted, updated=updated, tombstoned=tombstoned)

    return WriteOp(name=f"ingest.sync_project.{plan.project}", apply=_apply)


def _build_gone_write_op(name: str, *, writer: Writer, now_iso: str) -> WriteOp:
    """Build the ``WriteOp`` for a 404'd project: tombstone every wheel, drop its index row."""

    def _apply(conn: sqlite3.Connection) -> ProjectApplyResult:
        tombstoned = 0
        while True:
            rows = conn.execute(
                "SELECT id FROM wheels WHERE project = ? AND deleted_at IS NULL LIMIT ?",
                (name, DEFAULT_GONE_CHUNK_SIZE),
            ).fetchall()
            if not rows:
                break
            for (wheel_id,) in rows:
                seq = writer.next_seq()
                conn.execute(
                    "UPDATE wheels SET deleted_at = ?, state = ?, change_seq = ?, updated_at = ? "
                    "WHERE id = ?",
                    (now_iso, int(WheelState.DELETED), seq, now_iso, wheel_id),
                )
                tombstoned += 1
        conn.execute("DELETE FROM pypi_index WHERE name = ?", (name,))
        return ProjectApplyResult(inserted=0, updated=0, tombstoned=tombstoned, project_gone=True)

    return WriteOp(name=f"ingest.sync_project.{name}.gone", apply=_apply)


def _record_duplicate_filename(
    conn: sqlite3.Connection, filename: str, project: str, now_iso: str, reroll_version: str
) -> None:
    """Record an ``errors`` row for a filename that already belongs to another project."""
    existing = conn.execute("SELECT id FROM wheels WHERE filename = ?", (filename,)).fetchone()
    wheel_id = existing[0] if existing is not None else None
    conn.execute(
        "INSERT INTO errors "
        "(wheel_id, error_category, error_subcat, details, reroll_version, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            wheel_id,
            "duplicate_filename",
            project,
            f"filename {filename!r} already exists under a different project",
            reroll_version,
            now_iso,
        ),
    )


def _build_quarantine_error_write_op(
    project: str, attempts: int, *, now_iso: str, reroll_version: str
) -> WriteOp:
    """Record an ``errors`` row for a project newly quarantined by :class:`ProjectBackoff`.

    Project-scoped, not wheel-scoped -- ``wheel_id`` is ``NULL`` (nullable
    per ``schema.py``) and the project name is carried in ``error_subcat``
    and ``details`` instead, following :func:`_record_duplicate_filename`'s
    pattern for a ``wheel_id``-less ``errors`` insert.
    """

    def _apply(conn: sqlite3.Connection) -> None:
        conn.execute(
            "INSERT INTO errors "
            "(wheel_id, error_category, error_subcat, details, reroll_version, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                None,
                "project_ingest_quarantined",
                project,
                f"project {project!r} quarantined after {attempts} attempts",
                reroll_version,
                now_iso,
            ),
        )

    return WriteOp(name=f"ingest.project_quarantined.{project}", apply=_apply)


def _fetch_and_sync_project(
    client: PyPIClient,
    reader_conn_factory: Callable[[], sqlite3.Connection],
    name: str,
    now: Callable[[], float],
) -> ProjectSyncOutcome:
    """Worker entry point: open a private reader connection, fetch, diff, close, return."""
    conn = reader_conn_factory()
    try:
        return sync_project(client, conn, name, now=now)
    finally:
        conn.close()


def _limiter_child_for(client: PyPIClient) -> str:
    """Return the rate limiter child key for ``client``'s actual configured index host.

    ``PyPIClient`` exposes no public accessor for its configured index URL
    (spec 04's class is out of scope for this module to change), so this
    reads the private ``_index_url`` attribute rather than a hardcoded
    module-level constant -- a client built with a non-default ``index_url``
    must penalize that host, not always ``pypi.org``.
    """
    return httpx.URL(client._index_url).host


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()
