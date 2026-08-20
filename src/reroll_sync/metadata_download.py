"""Downloads the raw bytes of a distribution's ``.metadata`` sidecar file."""

from __future__ import annotations

import urllib.request


def fetch_metadata(url: str, timeout: float | None = None) -> bytes:
    """Download and return the raw bytes at ``url``."""
    request = urllib.request.Request(url)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()
