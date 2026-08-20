import io

from reroll_sync.metadata_download import fetch_metadata


class _FakeResponse:
    def __init__(self, data: bytes):
        self._buffer = io.BytesIO(data)

    def __enter__(self) -> io.BytesIO:
        return self._buffer

    def __exit__(self, *exc_info: object) -> None:
        self._buffer.close()


def test_fetch_metadata_returns_raw_bytes(monkeypatch):
    captured: list = []

    def _urlopen(request, timeout=None):
        captured.append((request, timeout))
        return _FakeResponse(b"Metadata-Version: 2.1\nName: numpy\n")

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)

    result = fetch_metadata("https://files.pythonhosted.org/x/numpy.whl.metadata", timeout=5)

    assert result == b"Metadata-Version: 2.1\nName: numpy\n"
    request, timeout = captured[0]
    assert request.full_url == "https://files.pythonhosted.org/x/numpy.whl.metadata"
    assert timeout == 5


def test_fetch_metadata_passes_no_timeout_through(monkeypatch):
    captured: list = []

    def _urlopen(request, timeout=None):
        captured.append(timeout)
        return _FakeResponse(b"")

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)

    fetch_metadata("https://example.invalid/x.whl.metadata", timeout=None)

    assert captured == [None]
