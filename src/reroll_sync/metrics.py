"""Prometheus text-format exporter for a `health.Health` snapshot.

`render_metrics` is the module's one entry point. It reads the same
`Health` object `health.snapshot()` builds -- there is exactly one source
for every number this module exposes, never a second parallel
computation. Names are prefixed ``reroll_sync_``; a scalar field becomes
one gauge/counter, a mapping field becomes one metric family per
dataclass sub-field, labeled by the mapping's key (and, for a field whose
value itself nests a mapping -- e.g. per-stage, per-lane queue depth --
labeled by both keys).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from typing import Any

from .health import Health

METRIC_PREFIX = "reroll_sync_"

_COUNTER_FAMILIES: frozenset[str] = frozenset(
    {
        "writer_failed_ops",
        "read_txn_budget_violations",
        "segments_sealed",
        "archive_bytes",
        "queues_ok_count",
        "queues_skip_count",
        "queues_retry_count",
        "queues_rate_limited_count",
        "limiter_children_acquired",
        "limiter_children_denied",
    }
)
"""Flattened metric family names that are monotonic counters.

Rule: a name belongs here exactly when the underlying value only ever
increases across a process's lifetime. `segments_sealed` and
`archive_bytes` are both derived from the same never-shrinking sealed-
segment set (segments are sealed, never unsealed or compacted -- see
`archive/store.py`'s module docstring), so both get the same
classification here, not one counter and one gauge. Everything not
listed is a gauge -- the safe default for a value that can both rise and
fall (a queue depth, a current state, and so on).
"""

_DEPENDENCY_STATE_CODES: Mapping[str, int] = {"closed": 0, "half_open": 1, "open": 2}
"""Numeric encoding for `DependencyHealth.state`, since a Prometheus sample
value must be numeric. Documented on the ``dependencies_state`` family's
``# HELP`` line.
"""

_LABEL_NAMES: Mapping[str, str] = {
    "state_counts": "state",
    "error_counts_1h": "category",
    "error_counts_24h": "category",
    "queues": "stage",
    "stages": "stage",
    "dependencies": "dependency",
    "limiter_children": "child",
}

_INNER_LABEL_NAMES: Mapping[tuple[str, str], str] = {
    ("queues", "depth_by_lane"): "lane",
}


def render_metrics(health: Health) -> str:
    """Render ``health`` as Prometheus text-format exposition output."""
    lines: list[str] = []
    emitted_types: set[str] = set()

    for field in dataclasses.fields(Health):
        value = getattr(health, field.name)
        if isinstance(value, Mapping):
            _render_mapping(lines, emitted_types, field.name, value)
        else:
            _emit(lines, emitted_types, field.name, value, {})

    return "\n".join(lines) + "\n"


def _render_mapping(
    lines: list[str], emitted_types: set[str], field_name: str, mapping: Mapping[Any, Any]
) -> None:
    label_name = _LABEL_NAMES.get(field_name, "key")
    for key, item in mapping.items():
        if dataclasses.is_dataclass(item):
            _render_dataclass_item(lines, emitted_types, field_name, label_name, key, item)
        else:
            _emit(lines, emitted_types, field_name, item, {label_name: key})


def _render_dataclass_item(
    lines: list[str],
    emitted_types: set[str],
    field_name: str,
    label_name: str,
    key: Any,
    item: Any,
) -> None:
    for sub in dataclasses.fields(item):
        sub_value = getattr(item, sub.name)
        metric_name = f"{field_name}_{sub.name}"
        if isinstance(sub_value, Mapping):
            inner_label = _INNER_LABEL_NAMES.get((field_name, sub.name), "item")
            for inner_key, inner_value in sub_value.items():
                _emit(
                    lines,
                    emitted_types,
                    metric_name,
                    inner_value,
                    {label_name: key, inner_label: inner_key},
                )
        else:
            _emit(lines, emitted_types, metric_name, sub_value, {label_name: key})


def _emit(
    lines: list[str],
    emitted_types: set[str],
    metric_name: str,
    value: Any,
    labels: Mapping[Any, Any],
) -> None:
    numeric = _to_numeric(value)
    if numeric is None:
        return
    full_name = f"{METRIC_PREFIX}{metric_name}"
    if full_name not in emitted_types:
        metric_type = "counter" if metric_name in _COUNTER_FAMILIES else "gauge"
        lines.append(f"# TYPE {full_name} {metric_type}")
        emitted_types.add(full_name)
    label_str = _render_labels(labels)
    lines.append(f"{full_name}{label_str} {numeric}")


def _render_labels(labels: Mapping[Any, Any]) -> str:
    if not labels:
        return ""
    rendered = ",".join(f'{name}="{_escape_label_value(value)}"' for name, value in labels.items())
    return f"{{{rendered}}}"


def _escape_label_value(value: Any) -> str:
    """Escape a label value per the Prometheus text-exposition format.

    Order matters: backslashes must be escaped before the quotes/newlines
    that escaping itself introduces are, or a value's own backslash would
    be doubled a second time.
    """
    text = str(value)
    text = text.replace("\\", "\\\\")
    text = text.replace('"', '\\"')
    text = text.replace("\n", "\\n")
    return text


def _to_numeric(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        return _DEPENDENCY_STATE_CODES.get(value)
    return None
