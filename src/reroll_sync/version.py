"""The installed reroll-sync package version, for tagging provenance columns."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    REROLL_VERSION = version("reroll-sync")
except PackageNotFoundError:
    REROLL_VERSION = "unknown"
