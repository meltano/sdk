"""Test the legacy singer-python logger bootstrap."""

from __future__ import annotations

import json
import logging

import pytest

import singer.logger


@pytest.fixture(autouse=True)
def _reset_logging_state(monkeypatch: pytest.MonkeyPatch):
    """Reset the one-shot configuration flag and preserve root logger state."""
    monkeypatch.setattr(singer.logger, "_configured", False)
    monkeypatch.delenv("SINGER_SDK_LOG_CONFIG", raising=False)
    monkeypatch.delenv("LOGGING_CONF_FILE", raising=False)

    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def test_get_logger_returns_named_logger():
    logger = singer.logger.get_logger("my_tap")
    assert logger.name == "my_tap"


def test_get_logger_defaults_to_root():
    assert singer.logger.get_logger() is logging.getLogger()


def test_configures_once(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []
    monkeypatch.setattr(singer.logger, "_default_config", lambda: calls.append("x"))
    singer.logger.get_logger()
    singer.logger.get_logger()
    assert len(calls) == 1


def test_dict_config_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path):
    config_path = tmp_path / "logging.json"
    config_path.write_text(
        json.dumps({
            "version": 1,
            "disable_existing_loggers": False,
            "root": {"level": "WARNING"},
        }),
    )
    monkeypatch.setenv("SINGER_SDK_LOG_CONFIG", str(config_path))
    singer.logger.setup_logging()
    assert logging.getLogger().level == logging.WARNING


def test_file_config_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path):
    config_path = tmp_path / "logging.conf"
    config_path.write_text(
        "[loggers]\n"
        "keys=root\n"
        "[handlers]\n"
        "keys=console\n"
        "[formatters]\n"
        "keys=default\n"
        "[logger_root]\n"
        "level=ERROR\n"
        "handlers=console\n"
        "[handler_console]\n"
        "class=StreamHandler\n"
        "level=ERROR\n"
        "formatter=default\n"
        "args=(sys.stderr,)\n"
        "[formatter_default]\n"
        "format=%(message)s\n",
    )
    monkeypatch.setenv("LOGGING_CONF_FILE", str(config_path))
    singer.logger.setup_logging()
    assert logging.getLogger().level == logging.ERROR


def test_remap_sdk_factories_when_sdk_missing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(singer.logger, "_sdk_available", lambda: False)
    config = {
        "version": 1,
        "formatters": {
            "structured": {"()": "singer_sdk.logging.StructuredFormatter"},
            "console": {"class": "singer_sdk.logging.ConsoleFormatter"},
            "plain": {"format": "%(message)s"},
        },
    }
    remapped = singer.logger._remap_sdk_factories(config)
    assert remapped["formatters"]["structured"]["()"] == (
        "singer.logging.StructuredFormatter"
    )
    assert remapped["formatters"]["console"]["class"] == (
        "singer.logging.ConsoleFormatter"
    )
    assert remapped["formatters"]["plain"] == {"format": "%(message)s"}


def test_remap_is_noop_when_sdk_installed(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(singer.logger, "_sdk_available", lambda: True)
    config = {
        "version": 1,
        "formatters": {
            "structured": {"()": "singer_sdk.logging.StructuredFormatter"},
        },
    }
    remapped = singer.logger._remap_sdk_factories(config)
    assert remapped["formatters"]["structured"]["()"] == (
        "singer_sdk.logging.StructuredFormatter"
    )


def test_default_config_adds_stderr_handler(monkeypatch: pytest.MonkeyPatch):
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    singer.logger.setup_logging()
    assert len(root.handlers) == 1
