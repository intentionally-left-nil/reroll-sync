import importlib

import reroll_sync.version as version_module


def test_reroll_version_matches_installed_package_metadata():
    from importlib.metadata import version

    assert version("reroll-sync") == version_module.REROLL_VERSION


def test_reroll_version_falls_back_when_package_not_installed(monkeypatch):
    from importlib.metadata import PackageNotFoundError

    def _raise(_name: str) -> str:
        raise PackageNotFoundError

    monkeypatch.setattr("importlib.metadata.version", _raise)

    importlib.reload(version_module)

    assert version_module.REROLL_VERSION == "unknown"

    importlib.reload(version_module)
