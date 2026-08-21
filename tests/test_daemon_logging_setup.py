"""Tests for structured JSON logging setup: quieting reroll's noisy
loggers, and JSON-lines output with correlation-key extras.
"""

from __future__ import annotations

import io
import json
import logging

from reroll_sync.daemon.logging_setup import (
    NOISY_REROLL_LOGGERS,
    JsonLogFormatter,
    configure_logging,
)


def test_configure_logging_sets_noisy_reroll_loggers_to_error():
    for name in NOISY_REROLL_LOGGERS:
        logging.getLogger(name).setLevel(logging.NOTSET)

    configure_logging()

    for name in NOISY_REROLL_LOGGERS:
        assert logging.getLogger(name).level == logging.ERROR


def test_configure_logging_installs_a_single_json_handler_on_the_root_logger():
    stream = io.StringIO()
    configure_logging(stream=stream)
    configure_logging(stream=stream)  # idempotent: re-configuring doesn't stack handlers

    root = logging.getLogger("reroll_sync")
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonLogFormatter)


def test_log_record_is_emitted_as_one_json_line():
    stream = io.StringIO()
    configure_logging(stream=stream)
    logger = logging.getLogger("reroll_sync.fetch")

    logger.info("fetched %s", "widget-1.0-py3-none-any.whl")

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["message"] == "fetched widget-1.0-py3-none-any.whl"
    assert payload["logger"] == "reroll_sync.fetch"
    assert payload["level"] == "INFO"


def test_log_record_carries_correlation_key_extras():
    stream = io.StringIO()
    configure_logging(stream=stream)
    logger = logging.getLogger("reroll_sync.fetch")

    logger.error(
        "fetch failed",
        extra={
            "wheel_id": 42,
            "wheel_filename": "widget-1.0-py3-none-any.whl",
            "wheel_project": "widget",
        },
    )

    payload = json.loads(stream.getvalue().splitlines()[0])
    assert payload["wheel_id"] == 42
    assert payload["filename"] == "widget-1.0-py3-none-any.whl"
    assert payload["project"] == "widget"


def test_log_record_without_correlation_keys_omits_them():
    stream = io.StringIO()
    configure_logging(stream=stream)
    logger = logging.getLogger("reroll_sync.fetch")

    logger.info("no correlation keys here")

    payload = json.loads(stream.getvalue().splitlines()[0])
    assert "wheel_id" not in payload
    assert "filename" not in payload
    assert "project" not in payload


def test_log_record_with_exception_includes_formatted_traceback():
    stream = io.StringIO()
    configure_logging(stream=stream)
    logger = logging.getLogger("reroll_sync.fetch")

    try:
        raise RuntimeError("boom")
    except RuntimeError:
        logger.error("unhandled", exc_info=True)

    payload = json.loads(stream.getvalue().splitlines()[0])
    assert "boom" in payload["exc_info"]


def test_configure_logging_sets_level():
    stream = io.StringIO()
    configure_logging(stream=stream, level=logging.WARNING)
    logger = logging.getLogger("reroll_sync.fetch")

    logger.info("should not appear")
    logger.warning("should appear")

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["message"] == "should appear"
