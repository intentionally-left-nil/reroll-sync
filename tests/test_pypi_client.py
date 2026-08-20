import io
import json

from reroll_sync.pypi_client import (
    ACCEPT_HEADER,
    SIMPLE_INDEX_URL,
    IndexProject,
    fetch_project,
    fetch_simple_index,
    metadata_hashes,
)


class _FakeResponse:
    def __init__(self, data: bytes):
        self._buffer = io.BytesIO(data)

    def __enter__(self) -> io.BytesIO:
        return self._buffer

    def __exit__(self, *exc_info: object) -> None:
        self._buffer.close()


def _fake_urlopen(payload: dict, captured: list):
    def _urlopen(request, timeout=None):
        captured.append((request, timeout))
        return _FakeResponse(json.dumps(payload).encode())

    return _urlopen


def test_fetch_simple_index_parses_projects(monkeypatch):
    payload = {
        "meta": {"_last-serial": 24888689, "api-version": "1.4"},
        "projects": [
            {"_last-serial": 3075854, "name": "0"},
            {"_last-serial": 42, "name": "numpy"},
        ],
    }
    captured: list = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen(payload, captured),
    )

    result = fetch_simple_index(timeout=5)

    assert result.last_serial == 24888689
    assert result.projects == (
        IndexProject(name="0", serial=3075854),
        IndexProject(name="numpy", serial=42),
    )
    request, timeout = captured[0]
    assert request.full_url == SIMPLE_INDEX_URL
    assert request.get_header("Accept") == ACCEPT_HEADER
    assert timeout == 5


def test_fetch_project_parses_files(monkeypatch):
    payload = {
        "files": [
            {
                "filename": "beautifulsoup4-4.0.1.tar.gz",
                "hashes": {"sha256": "abc"},
                "url": "https://files.pythonhosted.org/x",
            }
        ],
        "meta": {"_last-serial": 22406780, "api-version": "1.4"},
    }
    captured: list = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen(payload, captured),
    )

    result = fetch_project("beautifulsoup4", timeout=None)

    assert result.last_serial == 22406780
    assert len(result.files) == 1
    assert result.files[0].filename == "beautifulsoup4-4.0.1.tar.gz"
    assert result.files[0].raw == payload["files"][0]
    request, timeout = captured[0]
    assert request.full_url == f"{SIMPLE_INDEX_URL}beautifulsoup4/"
    assert timeout is None


def test_fetch_project_url_encodes_name(monkeypatch):
    payload = {"files": [], "meta": {"_last-serial": 1, "api-version": "1.4"}}
    captured: list = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen(payload, captured),
    )

    fetch_project("weird name/slash", timeout=None)

    request, _timeout = captured[0]
    assert request.full_url == f"{SIMPLE_INDEX_URL}weird%20name%2Fslash/"


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
