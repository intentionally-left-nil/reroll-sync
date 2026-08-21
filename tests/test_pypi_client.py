from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import httpx
import pytest

from reroll_sync.pypi_client import (
    ACCEPT_HEADER,
    SIMPLE_INDEX_URL,
    IndexProject,
    MetadataHashMismatch,
    ProjectFile,
    PyPIClient,
    PyPINotFound,
    PyPIProtocolError,
    PyPIRateLimited,
    PyPITransientError,
    metadata_hashes,
)

_HOSTS = frozenset({"pypi.org", "files.pythonhosted.org"})
_USER_AGENT = "reroll-sync-test (contact@example.invalid)"


class _FakeLimiter:
    """A minimal stand-in for HierarchicalLimiter, sufficient for client tests.

    Raises ``KeyError`` for a host outside ``hosts``, mirroring
    ``HierarchicalLimiter.acquire``'s contract for an unconfigured child.
    """

    def __init__(self, hosts: frozenset[str] = _HOSTS, *, acquire_result: bool = True) -> None:
        self.calls: list[tuple[str, float, float | None]] = []
        self._hosts = hosts
        self._acquire_result = acquire_result

    def acquire(self, child_name: str, n: float = 1, timeout: float | None = None) -> bool:
        if child_name not in self._hosts:
            raise KeyError(child_name)
        self.calls.append((child_name, n, timeout))
        return self._acquire_result


def _make_client(handler, *, limiter: _FakeLimiter | None = None, **kwargs) -> PyPIClient:
    transport = httpx.MockTransport(handler)
    return PyPIClient(
        limiter if limiter is not None else _FakeLimiter(),
        user_agent=_USER_AGENT,
        transport=transport,
        **kwargs,
    )


def _json_handler(payload: dict, *, status: int = 200, headers: dict | None = None, calls=None):
    def handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(request)
        return httpx.Response(
            status,
            headers={"content-type": ACCEPT_HEADER, **(headers or {})},
            json=payload,
            request=request,
        )

    return handler


def _index_payload(*, last_serial: int = 1, projects: tuple[dict, ...] = ()) -> dict:
    return {"meta": {"_last-serial": last_serial, "api-version": "1.4"}, "projects": list(projects)}


def _project_payload(*, last_serial: int = 1, files: tuple[dict, ...] = ()) -> dict:
    return {"meta": {"_last-serial": last_serial, "api-version": "1.4"}, "files": list(files)}


# --- metadata_hashes (ported from the pre-rewrite client; semantics unchanged) ---


def test_metadata_hashes_returns_none_when_unavailable():
    assert metadata_hashes({"core-metadata": False, "dist-info-metadata": False}) is None


def test_metadata_hashes_returns_none_when_fields_absent():
    assert metadata_hashes({}) is None


def test_metadata_hashes_returns_hashes_from_core_metadata_object():
    raw = {"core-metadata": {"sha256": "abc123"}}
    assert metadata_hashes(raw) == {"sha256": "abc123"}


def test_metadata_hashes_returns_empty_dict_when_core_metadata_is_true():
    raw = {"core-metadata": True}
    assert metadata_hashes(raw) == {}


def test_metadata_hashes_falls_back_to_legacy_dist_info_metadata_object():
    raw = {"core-metadata": False, "dist-info-metadata": {"sha256": "legacy123"}}
    assert metadata_hashes(raw) == {"sha256": "legacy123"}


def test_metadata_hashes_falls_back_to_legacy_dist_info_metadata_true():
    raw = {"core-metadata": False, "dist-info-metadata": True}
    assert metadata_hashes(raw) == {}


def test_metadata_hashes_prefers_core_metadata_object_over_legacy_true():
    raw = {"core-metadata": {"sha256": "new123"}, "dist-info-metadata": True}
    assert metadata_hashes(raw) == {"sha256": "new123"}


def test_metadata_hashes_prefers_any_object_over_any_true():
    raw = {"core-metadata": True, "dist-info-metadata": {"sha256": "legacy123"}}
    assert metadata_hashes(raw) == {"sha256": "legacy123"}


def test_metadata_hashes_returns_non_sha256_hash_dict_unchanged():
    raw = {"core-metadata": {"md5": "deadbeef"}}
    assert metadata_hashes(raw) == {"md5": "deadbeef"}


# --- Index / project page parsing ---


def test_fetch_simple_index_parses_projects_and_last_serial():
    payload = _index_payload(
        last_serial=24888689,
        projects=({"_last-serial": 3075854, "name": "0"}, {"_last-serial": 42, "name": "numpy"}),
    )
    client = _make_client(_json_handler(payload))

    result = client.fetch_simple_index()

    assert result.last_serial == 24888689
    assert result.projects == (
        IndexProject(name="0", serial=3075854),
        IndexProject(name="numpy", serial=42),
    )
    assert result.not_modified is False


def test_fetch_project_parses_every_documented_file_field():
    files = (
        {
            "filename": "widget-1.0-py3-none-any.whl",
            "url": "https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl",
            "hashes": {"sha256": "wheelhash"},
            "core-metadata": {"sha256": "metahash"},
            "size": 1234,
            "upload-time": "2024-01-01T00:00:00Z",
            "requires-python": ">=3.9",
            "yanked": False,
        },
    )
    payload = _project_payload(last_serial=22406780, files=files)
    client = _make_client(_json_handler(payload))

    result = client.fetch_project("widget")

    assert result.last_serial == 22406780
    assert result.files == (
        ProjectFile(
            filename="widget-1.0-py3-none-any.whl",
            url="https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl",
            wheel_sha256="wheelhash",
            metadata_sha256="metahash",
            has_metadata=True,
            size=1234,
            upload_time="2024-01-01T00:00:00Z",
            requires_python=">=3.9",
            yanked=False,
            yanked_reason=None,
        ),
    )


def test_metadata_hash_dict_without_sha256_key_yields_has_metadata_true_and_none_hash():
    files = (
        {
            "filename": "widget-1.0-py3-none-any.whl",
            "url": "https://files.pythonhosted.org/x/widget-1.0-py3-none-any.whl",
            "core-metadata": {"md5": "deadbeef"},
        },
    )
    client = _make_client(_json_handler(_project_payload(files=files)))

    result = client.fetch_project("widget")

    assert result.files[0].has_metadata is True
    assert result.files[0].metadata_sha256 is None


@pytest.mark.parametrize(
    ("yanked_value", "expected_bool", "expected_reason"),
    [
        (False, False, None),
        (True, True, None),
        ("", True, None),
        ("superseded by 1.1", True, "superseded by 1.1"),
    ],
)
def test_yanked_parsing(yanked_value, expected_bool, expected_reason):
    files = (
        {
            "filename": "widget-1.0-py3-none-any.whl",
            "url": "https://files.pythonhosted.org/x/widget-1.0.whl",
            "yanked": yanked_value,
        },
    )
    client = _make_client(_json_handler(_project_payload(files=files)))

    result = client.fetch_project("widget")

    assert result.files[0].yanked is expected_bool
    assert result.files[0].yanked_reason == expected_reason


def test_missing_optional_fields_yield_none_not_keyerror():
    files = (
        {
            "filename": "widget-1.0-py3-none-any.whl",
            "url": "https://files.pythonhosted.org/x/widget-1.0.whl",
        },
    )
    client = _make_client(_json_handler(_project_payload(files=files)))

    result = client.fetch_project("widget")

    file = result.files[0]
    assert file.size is None
    assert file.upload_time is None
    assert file.requires_python is None
    assert file.yanked is False
    assert file.yanked_reason is None


def test_file_entry_with_no_hashes_key_at_all():
    files = (
        {
            "filename": "widget-1.0-py3-none-any.whl",
            "url": "https://files.pythonhosted.org/x/widget-1.0.whl",
        },
    )
    client = _make_client(_json_handler(_project_payload(files=files)))

    result = client.fetch_project("widget")

    assert result.files[0].wheel_sha256 is None
    assert result.files[0].has_metadata is False
    assert result.files[0].metadata_sha256 is None


def test_non_whl_filenames_returned_unchanged():
    files = (
        {
            "filename": "widget-1.0.tar.gz",
            "url": "https://files.pythonhosted.org/x/widget-1.0.tar.gz",
        },
    )
    client = _make_client(_json_handler(_project_payload(files=files)))

    result = client.fetch_project("widget")

    assert result.files == (
        ProjectFile(
            filename="widget-1.0.tar.gz",
            url="https://files.pythonhosted.org/x/widget-1.0.tar.gz",
        ),
    )


def test_project_page_with_empty_files_list():
    client = _make_client(_json_handler(_project_payload(files=())))

    result = client.fetch_project("widget")

    assert result.files == ()


def test_unicode_project_name_is_percent_encoded_in_url():
    captured: list[httpx.Request] = []
    client = _make_client(_json_handler(_project_payload(), calls=captured))

    client.fetch_project("café")

    assert str(captured[0].url) == f"{SIMPLE_INDEX_URL}caf%C3%A9/"


def test_project_name_needing_pep503_normalization_is_requested_normalized():
    captured: list[httpx.Request] = []
    client = _make_client(_json_handler(_project_payload(), calls=captured))

    client.fetch_project("Foo_Bar.Baz")

    assert str(captured[0].url) == f"{SIMPLE_INDEX_URL}foo-bar-baz/"


# --- Conditional GET ---


def test_no_etag_sends_no_if_none_match_header():
    captured: list[httpx.Request] = []
    client = _make_client(_json_handler(_index_payload(), calls=captured))

    client.fetch_simple_index()

    assert "if-none-match" not in captured[0].headers


def test_etag_is_sent_verbatim_as_if_none_match():
    captured: list[httpx.Request] = []
    client = _make_client(_json_handler(_index_payload(), calls=captured))

    client.fetch_simple_index(etag='"abc123"')

    assert captured[0].headers["if-none-match"] == '"abc123"'


def test_304_yields_not_modified_with_no_exception():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(304, headers={"etag": '"abc123"'}, request=request)

    client = _make_client(handler)

    result = client.fetch_simple_index(etag='"abc123"')

    assert result.not_modified is True
    assert result.projects == ()
    assert result.etag == '"abc123"'


def test_200_yields_new_etag_from_response_header():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER, "etag": '"new-etag"'},
            json=_index_payload(),
            request=request,
        )

    client = _make_client(handler)

    result = client.fetch_simple_index()

    assert result.etag == '"new-etag"'


def test_last_modified_from_a_prior_200_is_sent_as_if_modified_since():
    captured: list[httpx.Request] = []
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        calls["n"] += 1
        headers = {"content-type": ACCEPT_HEADER}
        if calls["n"] == 1:
            headers["last-modified"] = "Wed, 01 Jan 2025 00:00:00 GMT"
        return httpx.Response(200, headers=headers, json=_index_payload(), request=request)

    client = _make_client(handler)

    client.fetch_simple_index()
    assert "if-modified-since" not in captured[0].headers
    client.fetch_simple_index()
    assert captured[1].headers["if-modified-since"] == "Wed, 01 Jan 2025 00:00:00 GMT"


# --- Errors ---


@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_raises_transient_error(status):
    client = _make_client(_json_handler(_index_payload(), status=status))

    with pytest.raises(PyPITransientError):
        client.fetch_simple_index()


@pytest.mark.parametrize(
    "exc", [httpx.ConnectTimeout("connect timed out"), httpx.ReadTimeout("read timed out")]
)
def test_connect_and_read_timeout_raise_transient_error(exc):
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc

    client = _make_client(handler)

    with pytest.raises(PyPITransientError):
        client.fetch_simple_index()


@pytest.mark.parametrize("status", [404, 410])
def test_404_and_410_raise_not_found(status):
    client = _make_client(_json_handler(_index_payload(), status=status))

    with pytest.raises(PyPINotFound):
        client.fetch_simple_index()


def test_429_with_delta_seconds_retry_after():
    client = _make_client(
        _json_handler(_index_payload(), status=429, headers={"retry-after": "30"})
    )

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_simple_index()

    assert exc_info.value.retry_after == 30.0


def test_429_with_http_date_retry_after_computed_against_injected_clock():
    now_value = 1_700_000_000.0
    retry_at = datetime.fromtimestamp(now_value, tz=UTC) + timedelta(seconds=120)
    header_value = format_datetime(retry_at, usegmt=True)
    client = _make_client(
        _json_handler(_index_payload(), status=429, headers={"retry-after": header_value}),
        now=lambda: now_value,
    )

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_simple_index()

    assert exc_info.value.retry_after == pytest.approx(120.0)


def test_429_with_unparseable_retry_after_yields_none():
    client = _make_client(
        _json_handler(_index_payload(), status=429, headers={"retry-after": "not-a-value"})
    )

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_simple_index()

    assert exc_info.value.retry_after is None


def test_429_with_naive_http_date_retry_after_is_treated_as_utc():
    now_value = 1_700_000_000.0
    retry_at = datetime.fromtimestamp(now_value, tz=UTC) + timedelta(seconds=60)
    header_value = retry_at.strftime("%a, %d %b %Y %H:%M:%S")
    client = _make_client(
        _json_handler(_index_payload(), status=429, headers={"retry-after": header_value}),
        now=lambda: now_value,
    )

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_simple_index()

    assert exc_info.value.retry_after == pytest.approx(60.0)


def test_429_with_negative_delta_seconds_retry_after_clamps_to_zero():
    client = _make_client(
        _json_handler(_index_payload(), status=429, headers={"retry-after": "-5"})
    )

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_simple_index()

    assert exc_info.value.retry_after == 0.0


def test_429_with_past_http_date_retry_after_clamps_to_zero():
    now_value = 1_700_000_000.0
    retry_at = datetime.fromtimestamp(now_value, tz=UTC) - timedelta(seconds=120)
    header_value = format_datetime(retry_at, usegmt=True)
    client = _make_client(
        _json_handler(_index_payload(), status=429, headers={"retry-after": header_value}),
        now=lambda: now_value,
    )

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_simple_index()

    assert exc_info.value.retry_after == 0.0


def test_429_with_no_retry_after_yields_none():
    client = _make_client(_json_handler(_index_payload(), status=429))

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_simple_index()

    assert exc_info.value.retry_after is None


def test_403_with_rate_limit_body_raises_rate_limited():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            content=b"You have exceeded a rate limit, slow down.",
            request=request,
        )

    client = _make_client(handler)

    with pytest.raises(PyPIRateLimited):
        client.fetch_simple_index()


def test_403_without_rate_limit_body_raises_protocol_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, content=b"Forbidden", request=request)

    client = _make_client(handler)

    with pytest.raises(PyPIProtocolError):
        client.fetch_simple_index()


def test_redirect_response_raises_protocol_error_and_is_not_followed():
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            302, headers={"location": "https://example.com/simple/"}, request=request
        )

    client = _make_client(handler)

    with pytest.raises(PyPIProtocolError):
        client.fetch_simple_index()

    assert len(calls) == 1


def test_200_with_html_content_type_raises_protocol_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": "text/html"}, content=b"<html></html>", request=request
        )

    client = _make_client(handler)

    with pytest.raises(PyPIProtocolError):
        client.fetch_simple_index()


def test_200_with_invalid_json_raises_protocol_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": ACCEPT_HEADER},
            content=b"{not valid json",
            request=request,
        )

    client = _make_client(handler)

    with pytest.raises(PyPIProtocolError):
        client.fetch_simple_index()


def test_200_missing_meta_raises_protocol_error():
    client = _make_client(_json_handler({"projects": []}))

    with pytest.raises(PyPIProtocolError):
        client.fetch_simple_index()


def test_project_page_200_missing_meta_raises_protocol_error():
    client = _make_client(_json_handler({"files": []}))

    with pytest.raises(PyPIProtocolError):
        client.fetch_project("widget")


def test_client_never_retries_after_a_single_failure():
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ConnectError("boom")
        return httpx.Response(
            200, headers={"content-type": ACCEPT_HEADER}, json=_index_payload(), request=request
        )

    client = _make_client(handler)

    with pytest.raises(PyPITransientError):
        client.fetch_simple_index()

    assert len(calls) == 1


def test_no_httpx_exception_escapes_fetch_simple_index():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    client = _make_client(handler)

    with pytest.raises(PyPITransientError):
        client.fetch_simple_index()


def test_no_httpx_exception_escapes_fetch_metadata():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("boom")

    client = _make_client(handler)

    with pytest.raises(PyPITransientError):
        client.fetch_metadata("https://files.pythonhosted.org/x/widget.whl.metadata", None)


def test_malformed_url_passed_to_fetch_metadata_raises_protocol_error_not_httpx():
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request should be sent for a malformed URL")

    client = _make_client(handler)

    with pytest.raises(PyPIProtocolError):
        client.fetch_metadata("http://[::1/widget.whl.metadata", None)


# --- Rate limiting integration ---


def test_every_request_acquires_one_token_from_host_matching_limiter_child():
    limiter = _FakeLimiter()
    client = _make_client(_json_handler(_index_payload()), limiter=limiter)

    client.fetch_simple_index()

    assert limiter.calls == [("pypi.org", 1, None)]


def test_metadata_fetch_acquires_from_files_pythonhosted_org_child():
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"data", request=request)

    client = _make_client(handler, limiter=limiter)

    client.fetch_metadata("https://files.pythonhosted.org/x/widget.whl.metadata", None)

    assert limiter.calls == [("files.pythonhosted.org", 1, None)]


def test_request_to_unexpected_host_raises_rather_than_proceeding():
    limiter = _FakeLimiter(hosts=frozenset({"pypi.org", "files.pythonhosted.org"}))
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, content=b"data", request=request)

    client = _make_client(handler, limiter=limiter)

    with pytest.raises(KeyError):
        client.fetch_metadata("https://example.com/x/widget.whl.metadata", None)

    assert calls == []


def test_limiter_acquire_returning_false_raises_rate_limited_without_requesting():
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, content=b"data", request=request)

    limiter = _FakeLimiter(acquire_result=False)
    client = _make_client(handler, limiter=limiter)

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_simple_index()

    assert exc_info.value.retry_after is None
    assert calls == []


def test_limiter_acquire_returning_false_for_metadata_raises_rate_limited():
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, content=b"data", request=request)

    limiter = _FakeLimiter(acquire_result=False)
    client = _make_client(handler, limiter=limiter)

    with pytest.raises(PyPIRateLimited) as exc_info:
        client.fetch_metadata("https://files.pythonhosted.org/x/widget.whl.metadata", None)

    assert exc_info.value.retry_after is None
    assert calls == []


# --- fetch_metadata ---


def test_fetch_metadata_returns_bytes_and_verifies_sha256():
    data = b"Metadata-Version: 2.1\nName: widget\n"
    import hashlib

    expected = hashlib.sha256(data).hexdigest()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data, request=request)

    client = _make_client(handler)

    result = client.fetch_metadata("https://files.pythonhosted.org/x/widget.whl.metadata", expected)

    assert result == data


def test_fetch_metadata_mismatching_sha256_raises_with_both_digests():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"wrong bytes", request=request)

    client = _make_client(handler)

    with pytest.raises(MetadataHashMismatch) as exc_info:
        client.fetch_metadata(
            "https://files.pythonhosted.org/x/widget.whl.metadata", "expectedhash"
        )

    assert exc_info.value.expected == "expectedhash"
    import hashlib

    assert exc_info.value.actual == hashlib.sha256(b"wrong bytes").hexdigest()


def test_fetch_metadata_with_no_expected_sha256_skips_verification():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"anything at all", request=request)

    client = _make_client(handler)

    result = client.fetch_metadata("https://files.pythonhosted.org/x/widget.whl.metadata", None)

    assert result == b"anything at all"


def test_fetch_metadata_exceeding_size_cap_raises_protocol_error():
    oversized = b"x" * (32 * 1024 * 1024 + 1)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=oversized, request=request)

    client = _make_client(handler)

    with pytest.raises(PyPIProtocolError):
        client.fetch_metadata("https://files.pythonhosted.org/x/widget.whl.metadata", None)


# --- Client object structure ---


def test_user_agent_is_a_required_constructor_argument():
    missing_kwargs: dict[str, Any] = {}
    with pytest.raises(TypeError):
        PyPIClient(_FakeLimiter(), **missing_kwargs)


def test_context_manager_closes_the_pooled_client():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": ACCEPT_HEADER}, json=_index_payload(), request=request
        )

    with _make_client(handler) as client:
        client.fetch_simple_index()
        assert client._client.is_closed is False
    assert client._client.is_closed is True


def test_single_pooled_httpx_client_constructed_for_multiple_requests(monkeypatch):
    construction_count = 0
    original_init = httpx.Client.__init__

    def counting_init(self, *args, **kwargs):
        nonlocal construction_count
        construction_count += 1
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.Client, "__init__", counting_init)

    client = _make_client(
        _json_handler(_index_payload()),
    )
    client.fetch_simple_index()
    client.fetch_simple_index()

    assert construction_count == 1
