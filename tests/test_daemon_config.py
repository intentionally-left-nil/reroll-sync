"""Tests for daemon.Config: validation and environment-variable loading."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from reroll_sync.daemon.config import (
    Config,
    ConfigError,
    config_from_env,
    default_convert_workers,
)

_REQUIRED: dict[str, Any] = {
    "db_path": Path("db.sqlite"),
    "segments_dir": Path("segments"),
    "socket_path": Path("reroll_sync.sock"),
}


def _config(**overrides: Any) -> Config:
    kwargs: dict[str, Any] = {
        **_REQUIRED,
        "user_agent": "reroll-sync-test (contact@example.invalid)",
    }
    kwargs.update(overrides)
    return Config(**kwargs)


def test_missing_user_agent_raises_type_error():
    with pytest.raises(TypeError):
        Config(**_REQUIRED)


def test_defaults_construct_a_valid_config():
    config = _config()
    assert config.global_rate == 2000.0
    assert config.domain_reserves == {"pypi.org": 200.0, "files.pythonhosted.org": 1800.0}
    assert config.fetch_workers == 64
    assert config.project_workers == 32
    assert config.convert_workers >= 1
    assert config.metrics_port is None


def test_metrics_port_defaults_to_none_meaning_disabled():
    assert _config().metrics_port is None


def test_metrics_port_can_be_set():
    assert _config(metrics_port=9110).metrics_port == 9110


def test_metrics_port_zero_is_valid_meaning_os_assigned():
    assert _config(metrics_port=0).metrics_port == 0


def test_metrics_port_must_not_be_negative():
    with pytest.raises(ConfigError):
        _config(metrics_port=-1)


def test_reserves_summing_above_global_rate_raises():
    with pytest.raises(ConfigError):
        _config(
            global_rate=100.0, domain_reserves={"pypi.org": 60.0, "files.pythonhosted.org": 50.0}
        )


def test_reserves_summing_to_exactly_global_rate_is_allowed():
    config = _config(
        global_rate=100.0, domain_reserves={"pypi.org": 60.0, "files.pythonhosted.org": 40.0}
    )
    assert config.global_rate == 100.0


@pytest.mark.parametrize("global_rate", [0.0, -1.0])
def test_non_positive_global_rate_raises(global_rate):
    with pytest.raises(ConfigError):
        _config(global_rate=global_rate)


@pytest.mark.parametrize("reserve", [0.0, -5.0])
def test_non_positive_domain_reserve_raises(reserve):
    with pytest.raises(ConfigError):
        _config(domain_reserves={"pypi.org": reserve})


@pytest.mark.parametrize("field_name", ["fetch_workers", "project_workers", "convert_workers"])
def test_zero_worker_count_raises(field_name):
    with pytest.raises(ConfigError):
        _config(**{field_name: 0})


def test_negative_worker_count_raises():
    with pytest.raises(ConfigError):
        _config(fetch_workers=-1)


def test_negative_handoff_budget_raises():
    with pytest.raises(ConfigError):
        _config(handoff_budget_bytes=-1)


def test_zero_handoff_budget_raises():
    with pytest.raises(ConfigError):
        _config(handoff_budget_bytes=0)


@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_segment_seal_bytes_raises(value):
    with pytest.raises(ConfigError):
        _config(segment_seal_bytes=value)


@pytest.mark.parametrize("value", [0.0, -1.0])
def test_non_positive_segment_seal_seconds_raises(value):
    with pytest.raises(ConfigError):
        _config(segment_seal_seconds=value)


def test_negative_disk_free_floor_bytes_raises():
    with pytest.raises(ConfigError):
        _config(disk_free_floor_bytes=-1)


def test_zero_disk_free_floor_bytes_is_allowed():
    config = _config(disk_free_floor_bytes=0)
    assert config.disk_free_floor_bytes == 0


@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_batch_size_raises(value):
    with pytest.raises(ConfigError):
        _config(batch_size=value)


@pytest.mark.parametrize("value", [0.0, -1.0])
def test_non_positive_checkpoint_interval_raises(value):
    with pytest.raises(ConfigError):
        _config(checkpoint_interval=value)


@pytest.mark.parametrize("value", [0.0, -1.0])
def test_non_positive_vacuum_interval_raises(value):
    with pytest.raises(ConfigError):
        _config(vacuum_interval=value)


@pytest.mark.parametrize("value", [0.0, -1.0])
def test_non_positive_index_poll_interval_raises(value):
    with pytest.raises(ConfigError):
        _config(index_poll_interval=value)


@pytest.mark.parametrize("value", [0, -1])
def test_max_attempts_below_one_raises(value):
    with pytest.raises(ConfigError):
        _config(max_attempts=value)


def test_max_attempts_of_one_is_allowed():
    config = _config(max_attempts=1)
    assert config.max_attempts == 1


@pytest.mark.parametrize("value", [0.0, -1.0])
def test_non_positive_backoff_base_raises(value):
    with pytest.raises(ConfigError):
        _config(backoff_base=value)


def test_backoff_cap_smaller_than_base_raises():
    with pytest.raises(ConfigError):
        _config(backoff_base=100.0, backoff_cap=50.0)


def test_backoff_cap_equal_to_base_is_allowed():
    config = _config(backoff_base=100.0, backoff_cap=100.0)
    assert config.backoff_cap == 100.0


def test_convert_workers_is_at_least_one_on_a_hypothetical_one_core_machine():
    assert default_convert_workers(cpu_count=1) == 1


def test_convert_workers_is_at_least_one_when_cpu_count_is_unknown():
    assert default_convert_workers(cpu_count=None) == 1


def test_convert_workers_default_subtracts_two_on_a_many_core_machine():
    assert default_convert_workers(cpu_count=8) == 6


def test_config_convert_workers_default_factory_calls_real_os_cpu_count(monkeypatch):
    """`default_convert_workers` itself is covered in isolation above; this
    proves `Config`'s `default_factory` actually wires up to the real
    `os.cpu_count()` at construction time when `convert_workers` is not
    passed explicitly, end to end.
    """
    monkeypatch.setattr("os.cpu_count", lambda: 4)
    config = Config(**_REQUIRED, user_agent="reroll-sync-test (contact@example.invalid)")
    assert config.convert_workers == 2


def test_config_from_env_requires_user_agent():
    with pytest.raises(ConfigError):
        config_from_env({})


def test_config_from_env_rejects_empty_user_agent():
    with pytest.raises(ConfigError):
        config_from_env({"REROLL_SYNC_USER_AGENT": ""})


def test_config_from_env_reads_required_user_agent_and_defaults_the_rest():
    config = config_from_env({"REROLL_SYNC_USER_AGENT": "ua (contact@example.invalid)"})
    assert config.user_agent == "ua (contact@example.invalid)"
    assert config.global_rate == 2000.0


def test_config_from_env_overrides_scalar_fields():
    config = config_from_env(
        {
            "REROLL_SYNC_USER_AGENT": "ua",
            "REROLL_SYNC_DB_PATH": "/tmp/x.db",
            "REROLL_SYNC_SEGMENTS_DIR": "/tmp/segs",
            "REROLL_SYNC_SOCKET_PATH": "/tmp/x.sock",
            "REROLL_SYNC_GLOBAL_RATE": "500.0",
            "REROLL_SYNC_DOMAIN_RESERVES": '{"pypi.org": 100.0, "files.pythonhosted.org": 200.0}',
            "REROLL_SYNC_FETCH_WORKERS": "8",
            "REROLL_SYNC_PROJECT_WORKERS": "4",
            "REROLL_SYNC_CONVERT_WORKERS": "2",
            "REROLL_SYNC_HANDOFF_BUDGET_BYTES": "1024",
            "REROLL_SYNC_BATCH_SIZE": "10",
            "REROLL_SYNC_BATCH_INTERVAL": "0.5",
            "REROLL_SYNC_CHECKPOINT_INTERVAL": "30.0",
            "REROLL_SYNC_VACUUM_INTERVAL": "60.0",
            "REROLL_SYNC_INDEX_POLL_INTERVAL": "10.0",
            "REROLL_SYNC_MAX_ATTEMPTS": "3",
            "REROLL_SYNC_BACKOFF_BASE": "1.0",
            "REROLL_SYNC_BACKOFF_CAP": "60.0",
            "REROLL_SYNC_SEGMENT_SEAL_BYTES": "100",
            "REROLL_SYNC_SEGMENT_SEAL_SECONDS": "5.0",
            "REROLL_SYNC_DISK_FREE_FLOOR_BYTES": "1000",
            "REROLL_SYNC_METRICS_PORT": "9110",
        }
    )
    assert config.db_path == Path("/tmp/x.db")
    assert config.segments_dir == Path("/tmp/segs")
    assert config.socket_path == Path("/tmp/x.sock")
    assert config.global_rate == 500.0
    assert config.fetch_workers == 8
    assert config.project_workers == 4
    assert config.convert_workers == 2
    assert config.handoff_budget_bytes == 1024
    assert config.batch_size == 10
    assert config.batch_interval == 0.5
    assert config.checkpoint_interval == 30.0
    assert config.vacuum_interval == 60.0
    assert config.index_poll_interval == 10.0
    assert config.max_attempts == 3
    assert config.backoff_base == 1.0
    assert config.backoff_cap == 60.0
    assert config.segment_seal_bytes == 100
    assert config.segment_seal_seconds == 5.0
    assert config.disk_free_floor_bytes == 1000
    assert config.metrics_port == 9110


def test_config_from_env_metrics_port_defaults_to_none_when_unset():
    config = config_from_env({"REROLL_SYNC_USER_AGENT": "ua"})
    assert config.metrics_port is None


def test_config_from_env_parses_domain_reserves_as_json():
    config = config_from_env(
        {
            "REROLL_SYNC_USER_AGENT": "ua",
            "REROLL_SYNC_DOMAIN_RESERVES": '{"pypi.org": 10.0, "files.pythonhosted.org": 20.0}',
        }
    )
    assert dict(config.domain_reserves) == {"pypi.org": 10.0, "files.pythonhosted.org": 20.0}


def test_config_from_env_defaults_to_os_environ(monkeypatch):
    monkeypatch.setenv("REROLL_SYNC_USER_AGENT", "ua-from-os-environ")
    config = config_from_env()
    assert config.user_agent == "ua-from-os-environ"


def test_config_from_env_invalid_values_raise_config_error():
    with pytest.raises(ConfigError):
        config_from_env({"REROLL_SYNC_USER_AGENT": "ua", "REROLL_SYNC_GLOBAL_RATE": "-1"})
