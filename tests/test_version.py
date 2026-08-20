import reroll

import reroll_sync.version as version_module


def test_reroll_version_matches_reroll_dunder_version():
    assert reroll.__version__ == version_module.REROLL_VERSION
