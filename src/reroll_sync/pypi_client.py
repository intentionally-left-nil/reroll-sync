"""HTTP client for the PyPI simple index JSON API.

See https://docs.pypi.org/api/index-api/ for the API this wraps.
"""

from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Any, Protocol

import httpx

SIMPLE_INDEX_URL = "https://pypi.org/simple/"
ACCEPT_HEADER = "application/vnd.pypi.simple.v1+json"

_MAX_METADATA_BYTES = 32 * 1024 * 1024
_PEP503_SEPARATORS = re.compile(r"[-_.]+")
_URL_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")


class RateLimiter(Protocol):
    """The subset of :class:`~reroll_sync.ratelimit.HierarchicalLimiter` a client needs."""

    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        """Acquire ``n`` tokens for ``child_name``; see ``HierarchicalLimiter.acquire``."""
        raise NotImplementedError


class PyPIError(Exception):
    """Base class for every exception :class:`PyPIClient` raises."""


class PyPITransientError(PyPIError):
    """A connect/read timeout, 5xx response, or connection reset.

    The dispatcher should retry this with backoff.
    """


class PyPIRateLimited(PyPIError):
    """A 429 response, a 403 with a rate-limit body, or a local throttle timeout.

    ``retry_after`` is the server's requested delay in seconds, or ``None``
    when none was given.
    """

    def __init__(self, retry_after: float | None) -> None:
        super().__init__(f"rate limited, retry_after={retry_after!r}")
        self.retry_after = retry_after


class PyPINotFound(PyPIError):
    """A 404 or 410 response. Terminal for this URL: the project or file is gone."""


class PyPIProtocolError(PyPIError):
    """The response did not look like the PyPI simple API expects.

    Raised for a non-JSON body, a missing ``meta``/``files`` key, an
    unexpected ``Content-Type``, or an oversized ``fetch_metadata`` response.
    """


class MetadataHashMismatch(PyPIError):
    """A ``fetch_metadata`` response's sha256 didn't match the index's published hash.

    This is a recordable data inconsistency, not a transient failure: PyPI
    served a ``.metadata`` file whose bytes don't match its own index.
    """

    def __init__(self, expected: str, actual: str) -> None:
        super().__init__(f"metadata hash mismatch: expected {expected}, got {actual}")
        self.expected = expected
        self.actual = actual


@dataclass(frozen=True)
class IndexProject:
    """A single project entry from the ``/simple/`` index."""

    name: str
    serial: int


@dataclass(frozen=True)
class SimpleIndex:
    """The parsed response of the ``/simple/`` index endpoint."""

    last_serial: int
    projects: tuple[IndexProject, ...]
    etag: str | None = None
    not_modified: bool = False


@dataclass(frozen=True)
class ProjectFile:
    """A single file entry from a project's ``/simple/{name}/`` page.

    Maps 1:1 onto the ``wheels`` table's normalized columns; no raw JSON is
    retained. ``has_metadata`` is ``True`` whenever the index advertises a
    ``.metadata`` sidecar for this file, even if it publishes no hash for it
    (in which case ``metadata_sha256`` is ``None``).
    """

    filename: str
    url: str
    wheel_sha256: str | None = None
    metadata_sha256: str | None = None
    has_metadata: bool = False
    size: int | None = None
    upload_time: str | None = None
    requires_python: str | None = None
    yanked: bool = False
    yanked_reason: str | None = None


@dataclass(frozen=True)
class ProjectPage:
    """The parsed response of a project's ``/simple/{name}/`` page."""

    last_serial: int
    files: tuple[ProjectFile, ...]


class PyPIClient:
    """A connection-pooled HTTP client for the PyPI simple index JSON API.

    Every request acquires a token from ``limiter`` first, keyed by the
    request's URL host, so an unconfigured host raises ``KeyError`` rather
    than proceeding unthrottled. Redirects are never followed: PyPI's simple
    API doesn't issue them in normal operation, and following one would mean
    contacting a second host the limiter never got a chance to key on.
    """

    def __init__(
        self,
        limiter: RateLimiter,
        *,
        index_url: str = SIMPLE_INDEX_URL,
        timeout: float = 30.0,
        user_agent: str,
        transport: httpx.BaseTransport | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._limiter = limiter
        self._index_url = index_url
        self._now = now
        self._last_modified: str | None = None
        self._client = httpx.Client(
            http2=True,
            timeout=timeout,
            transport=transport,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=100),
            headers={"User-Agent": user_agent},
            follow_redirects=False,
        )

    def __enter__(self) -> PyPIClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the pooled connection."""
        self._client.close()

    def fetch_simple_index(self, etag: str | None = None) -> SimpleIndex:
        """Fetch and parse the PyPI ``/simple/`` project index.

        Sends ``If-None-Match: etag`` when given, and ``If-Modified-Since``
        when a previous call on this client saw a ``Last-Modified`` header.
        A ``304`` returns ``not_modified=True`` with an empty project tuple
        rather than raising.
        """
        headers = {"Accept": ACCEPT_HEADER}
        if etag is not None:
            headers["If-None-Match"] = etag
        if self._last_modified is not None:
            headers["If-Modified-Since"] = self._last_modified
        response = self._get(self._index_url, headers)
        self._check_status(response)
        if response.status_code == 304:
            return SimpleIndex(
                last_serial=0,
                projects=(),
                etag=response.headers.get("etag", etag),
                not_modified=True,
            )
        self._check_simple_content_type(response)
        data = self._parse_json(response)
        last_serial, projects = _parse_index_body(data)
        self._last_modified = response.headers.get("last-modified")
        return SimpleIndex(
            last_serial=last_serial,
            projects=projects,
            etag=response.headers.get("etag"),
            not_modified=False,
        )

    def fetch_project(self, name: str) -> ProjectPage:
        """Fetch and parse the PyPI ``/simple/{name}/`` file listing for a project."""
        path = _percent_encode(_normalize_project_name(name))
        url = f"{self._index_url}{path}/"
        response = self._get(url, {"Accept": ACCEPT_HEADER})
        self._check_status(response)
        self._check_simple_content_type(response)
        data = self._parse_json(response)
        last_serial, files = _parse_project_body(data)
        return ProjectPage(last_serial=last_serial, files=files)

    def fetch_metadata(self, url: str, expected_sha256: str | None) -> bytes:
        """Download a distribution's ``.metadata`` sidecar file.

        ``url`` is the full ``.metadata`` URL; callers build it from the
        wheel's own URL. Verifies ``expected_sha256`` against the downloaded
        bytes when given, raising :class:`MetadataHashMismatch` on a
        mismatch. Aborts with :class:`PyPIProtocolError` once the response
        exceeds a 32 MB cap, without buffering the rest of it.
        """
        host = _host_of(url)
        if not self._limiter.acquire(host):
            raise PyPIRateLimited(retry_after=None)
        try:
            with self._client.stream("GET", url) as response:
                self._check_status(response)
                content = bytearray()
                for chunk in response.iter_bytes():
                    content.extend(chunk)
                    if len(content) > _MAX_METADATA_BYTES:
                        raise PyPIProtocolError(
                            f"metadata response from {url} exceeded {_MAX_METADATA_BYTES} bytes"
                        )
        except httpx.HTTPError as exc:
            raise PyPITransientError(str(exc)) from exc
        data = bytes(content)
        if expected_sha256 is not None:
            actual = hashlib.sha256(data).hexdigest()
            if actual != expected_sha256:
                raise MetadataHashMismatch(expected_sha256, actual)
        return data

    def _get(self, url: str, headers: dict[str, str]) -> httpx.Response:
        host = _host_of(url)
        if not self._limiter.acquire(host):
            raise PyPIRateLimited(retry_after=None)
        try:
            return self._client.get(url, headers=headers)
        except httpx.HTTPError as exc:
            raise PyPITransientError(str(exc)) from exc

    def _check_status(self, response: httpx.Response) -> None:
        status = response.status_code
        if status == 304 or 200 <= status < 300:
            return
        if status in (404, 410):
            raise PyPINotFound(f"{status} for {response.url}")
        if status == 429 or (status == 403 and _is_rate_limited_body(response)):
            retry_after = _parse_retry_after(response.headers.get("retry-after"), self._now)
            raise PyPIRateLimited(retry_after)
        if 500 <= status < 600:
            raise PyPITransientError(f"{status} for {response.url}")
        raise PyPIProtocolError(f"unexpected status {status} for {response.url}")

    def _check_simple_content_type(self, response: httpx.Response) -> None:
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith(ACCEPT_HEADER):
            raise PyPIProtocolError(f"unexpected content-type {content_type!r} for {response.url}")

    def _parse_json(self, response: httpx.Response) -> dict[str, Any]:
        try:
            return response.json()
        except ValueError as exc:
            raise PyPIProtocolError(f"invalid JSON from {response.url}: {exc}") from exc


def metadata_hashes(raw: dict[str, Any]) -> dict[str, str] | None:
    """Return the ``.metadata`` file's hash algorithm -> hex digest mapping.

    Reads the PEP 714 ``core-metadata`` field and the legacy PEP 658
    ``dist-info-metadata`` field of a file's simple-index entry. Either field
    may be ``False`` (no separate metadata file), ``True`` (a metadata file
    exists but the index publishes no hash for it), or an object such as
    ``{"sha256": "..."}`` (a metadata file exists with its own hash, distinct
    from the wheel's own hash). Returns ``None`` if neither field indicates a
    metadata file is available, ``{}`` if one is available but unhashed, or
    the hash mapping when one is published. An object value from either field
    is preferred over a bare ``True`` from either field.
    """
    values = (raw.get("core-metadata"), raw.get("dist-info-metadata"))
    for value in values:
        if isinstance(value, dict):
            return value
    for value in values:
        if value is True:
            return {}
    return None


def _normalize_project_name(name: str) -> str:
    """Normalize a project name per PEP 503."""
    return _PEP503_SEPARATORS.sub("-", name).lower()


def _host_of(url: str) -> str:
    """Return the host component of ``url``, as the rate limiter's child key.

    Raises :class:`PyPIProtocolError` instead of letting
    :class:`httpx.InvalidURL` escape ``httpx.URL``'s parsing.
    """
    try:
        return httpx.URL(url).host
    except httpx.InvalidURL as exc:
        raise PyPIProtocolError(f"invalid URL {url!r}: {exc}") from exc


def _percent_encode(value: str) -> str:
    """Percent-encode every byte of ``value`` outside RFC 3986's unreserved set."""
    encoded = []
    for byte in value.encode("utf-8"):
        char = chr(byte)
        if char in _URL_UNRESERVED:
            encoded.append(char)
        else:
            encoded.append(f"%{byte:02X}")
    return "".join(encoded)


def _parse_yanked(value: bool | str) -> tuple[bool, str | None]:
    """Parse a simple-index file entry's ``yanked`` field.

    ``yanked`` is ``false``, ``true``, or a string reason. A non-empty
    string means yanked with that reason; an empty string means yanked with
    no reason, the same as a bare ``true``.
    """
    if isinstance(value, str):
        return True, value or None
    return bool(value), None


def _parse_retry_after(value: str | None, now: Callable[[], float]) -> float | None:
    """Parse a ``Retry-After`` header in either delta-seconds or HTTP-date form.

    Returns ``None`` when ``value`` is missing or unparseable. HTTP-date
    values are converted to a seconds-from-now delta using ``now``.
    """
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max(0.0, parsed.timestamp() - now())


def _is_rate_limited_body(response: httpx.Response) -> bool:
    response.read()
    return "rate limit" in response.text.lower()


def _parse_index_body(data: dict[str, Any]) -> tuple[int, tuple[IndexProject, ...]]:
    try:
        last_serial = data["meta"]["_last-serial"]
        projects = tuple(
            IndexProject(name=project["name"], serial=project["_last-serial"])
            for project in data["projects"]
        )
    except KeyError as exc:
        raise PyPIProtocolError(f"malformed simple index: missing {exc}") from exc
    return last_serial, projects


def _parse_project_body(data: dict[str, Any]) -> tuple[int, tuple[ProjectFile, ...]]:
    try:
        last_serial = data["meta"]["_last-serial"]
        files = tuple(_parse_file(raw) for raw in data["files"])
    except KeyError as exc:
        raise PyPIProtocolError(f"malformed project page: missing {exc}") from exc
    return last_serial, files


def _parse_file(raw: dict[str, Any]) -> ProjectFile:
    hashes = raw.get("hashes") or {}
    hashes_meta = metadata_hashes(raw)
    yanked, yanked_reason = _parse_yanked(raw.get("yanked", False))
    return ProjectFile(
        filename=raw["filename"],
        url=raw["url"],
        wheel_sha256=hashes.get("sha256"),
        metadata_sha256=hashes_meta.get("sha256") if hashes_meta is not None else None,
        has_metadata=hashes_meta is not None,
        size=raw.get("size"),
        upload_time=raw.get("upload-time"),
        requires_python=raw.get("requires-python"),
        yanked=yanked,
        yanked_reason=yanked_reason,
    )
