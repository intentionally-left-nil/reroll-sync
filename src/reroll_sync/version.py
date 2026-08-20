"""The installed ``reroll`` (parsing library) version, for tagging provenance columns.

``reroll``'s version varies over the database's lifetime as it's upgraded, so
it's worth recording per-row (to know when a row was parsed by an older
version and might be worth reprocessing). reroll-sync's own version isn't
recorded anywhere: only one version of reroll-sync ever runs against the
database at a time, so it carries no information beyond what a row's
``updated_at``/``created_at`` already implies.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    REROLL_VERSION = version("py-reroll")
except PackageNotFoundError:
    REROLL_VERSION = "unknown"
