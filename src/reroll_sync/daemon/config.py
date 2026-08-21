"""Daemon configuration: every runtime knob, loaded from environment variables.

No other daemon module reads ``os.environ`` directly; everything the daemon
needs is threaded through an already-constructed :class:`Config` instance.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from frozendict import frozendict

DEFAULT_DOMAIN_RESERVES: frozendict[str, float] = frozendict(
    {"pypi.org": 200.0, "files.pythonhosted.org": 1800.0}
)

_ENV_PREFIX = "REROLL_SYNC_"


class ConfigError(ValueError):
    """Raised for a missing or invalid configuration value."""


def default_convert_workers(cpu_count: int | None) -> int:
    """Return the default convert-pool size for a machine with ``cpu_count`` CPUs.

    Never returns below 1, even for a single-core host or ``cpu_count=None``
    (an unknown CPU count, e.g. what ``os.cpu_count()`` returns in a
    container without visibility into the host's core count).
    """
    return max(1, (cpu_count or 1) - 2)


@dataclass(frozen=True)
class Config:
    """Every daemon runtime knob, validated at construction.

    ``user_agent`` has no default: PyPI asks for a descriptive one with
    contact info, and it is what keeps the service from being blocked, so it
    must never be silently forgotten.
    """

    db_path: Path
    segments_dir: Path
    socket_path: Path
    user_agent: str
    global_rate: float = 2000.0
    domain_reserves: Mapping[str, float] = field(default_factory=lambda: DEFAULT_DOMAIN_RESERVES)
    fetch_workers: int = 64
    project_workers: int = 32
    convert_workers: int = field(default_factory=lambda: default_convert_workers(os.cpu_count()))
    handoff_budget_bytes: int = 256 * 1024 * 1024
    batch_size: int = 1000
    batch_interval: float = 0.1
    checkpoint_interval: float = 60.0
    vacuum_interval: float = 3600.0
    index_poll_interval: float = 300.0
    max_attempts: int = 8
    backoff_base: float = 30.0
    backoff_cap: float = 21600.0
    segment_seal_bytes: int = 64 * 1024 * 1024
    segment_seal_seconds: float = 21600.0
    disk_free_floor_bytes: int = 20 * 1024**3

    def __post_init__(self) -> None:
        _require_positive(self.global_rate, "global_rate")
        for name, rate in self.domain_reserves.items():
            _require_positive(rate, f"domain_reserves[{name!r}]")
        reserved_total = sum(self.domain_reserves.values())
        if reserved_total > self.global_rate:
            raise ConfigError(
                f"domain_reserves sum to {reserved_total}, exceeding global_rate {self.global_rate}"
            )
        _require_min_workers(self.fetch_workers, "fetch_workers")
        _require_min_workers(self.project_workers, "project_workers")
        _require_min_workers(self.convert_workers, "convert_workers")
        _require_positive(self.handoff_budget_bytes, "handoff_budget_bytes")
        _require_positive(self.segment_seal_bytes, "segment_seal_bytes")
        _require_positive(self.segment_seal_seconds, "segment_seal_seconds")
        _require_non_negative(self.disk_free_floor_bytes, "disk_free_floor_bytes")
        _require_positive(self.batch_size, "batch_size")
        _require_positive(self.checkpoint_interval, "checkpoint_interval")
        _require_positive(self.vacuum_interval, "vacuum_interval")
        _require_positive(self.index_poll_interval, "index_poll_interval")
        _require_at_least(self.max_attempts, 1, "max_attempts")
        _require_positive(self.backoff_base, "backoff_base")
        if self.backoff_cap < self.backoff_base:
            raise ConfigError(
                f"backoff_cap ({self.backoff_cap}) must be >= backoff_base ({self.backoff_base})"
            )


def config_from_env(env: Mapping[str, str] | None = None) -> Config:
    """Build a :class:`Config` from ``REROLL_SYNC_*`` environment variables.

    ``env`` defaults to ``os.environ``. Every field has a default except
    ``user_agent``, which raises :class:`ConfigError` when unset or empty.
    """
    source = env if env is not None else os.environ
    user_agent = source.get(f"{_ENV_PREFIX}USER_AGENT")
    if not user_agent:
        raise ConfigError(f"{_ENV_PREFIX}USER_AGENT is required and has no default")

    defaults = Config(
        db_path=Path("reroll_sync.db"),
        segments_dir=Path("segments"),
        socket_path=Path("reroll_sync.sock"),
        user_agent=user_agent,
    )

    domain_reserves = defaults.domain_reserves
    raw_domain_reserves = source.get(f"{_ENV_PREFIX}DOMAIN_RESERVES")
    if raw_domain_reserves is not None:
        domain_reserves = frozendict(json.loads(raw_domain_reserves))

    return Config(
        db_path=Path(_str(source, "DB_PATH", str(defaults.db_path))),
        segments_dir=Path(_str(source, "SEGMENTS_DIR", str(defaults.segments_dir))),
        socket_path=Path(_str(source, "SOCKET_PATH", str(defaults.socket_path))),
        user_agent=user_agent,
        global_rate=_float(source, "GLOBAL_RATE", defaults.global_rate),
        domain_reserves=domain_reserves,
        fetch_workers=_int(source, "FETCH_WORKERS", defaults.fetch_workers),
        project_workers=_int(source, "PROJECT_WORKERS", defaults.project_workers),
        convert_workers=_int(source, "CONVERT_WORKERS", defaults.convert_workers),
        handoff_budget_bytes=_int(source, "HANDOFF_BUDGET_BYTES", defaults.handoff_budget_bytes),
        batch_size=_int(source, "BATCH_SIZE", defaults.batch_size),
        batch_interval=_float(source, "BATCH_INTERVAL", defaults.batch_interval),
        checkpoint_interval=_float(source, "CHECKPOINT_INTERVAL", defaults.checkpoint_interval),
        vacuum_interval=_float(source, "VACUUM_INTERVAL", defaults.vacuum_interval),
        index_poll_interval=_float(source, "INDEX_POLL_INTERVAL", defaults.index_poll_interval),
        max_attempts=_int(source, "MAX_ATTEMPTS", defaults.max_attempts),
        backoff_base=_float(source, "BACKOFF_BASE", defaults.backoff_base),
        backoff_cap=_float(source, "BACKOFF_CAP", defaults.backoff_cap),
        segment_seal_bytes=_int(source, "SEGMENT_SEAL_BYTES", defaults.segment_seal_bytes),
        segment_seal_seconds=_float(source, "SEGMENT_SEAL_SECONDS", defaults.segment_seal_seconds),
        disk_free_floor_bytes=_int(source, "DISK_FREE_FLOOR_BYTES", defaults.disk_free_floor_bytes),
    )


def _str(source: Mapping[str, str], suffix: str, default: str) -> str:
    return source.get(f"{_ENV_PREFIX}{suffix}", default)


def _float(source: Mapping[str, str], suffix: str, default: float) -> float:
    raw = source.get(f"{_ENV_PREFIX}{suffix}")
    return default if raw is None else float(raw)


def _int(source: Mapping[str, str], suffix: str, default: int) -> int:
    raw = source.get(f"{_ENV_PREFIX}{suffix}")
    return default if raw is None else int(raw)


def _require_positive(value: float, name: str) -> None:
    if value <= 0:
        raise ConfigError(f"{name} must be positive, got {value}")


def _require_non_negative(value: float, name: str) -> None:
    if value < 0:
        raise ConfigError(f"{name} must be >= 0, got {value}")


def _require_min_workers(value: int, name: str) -> None:
    _require_at_least(value, 1, name)


def _require_at_least(value: float, minimum: float, name: str) -> None:
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
