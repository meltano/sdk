"""Logger setup for Singer taps and targets.

Provides the legacy ``singer.get_logger`` entry point, with support for
configuring logging from a file passed through environment variables:

1. ``SINGER_SDK_LOG_CONFIG``: path to a JSON (or YAML, when PyYAML is
   installed) :func:`logging.config.dictConfig` file. This is the file
   Meltano generates for Singer plugins.
2. ``LOGGING_CONF_FILE``: path to an INI :func:`logging.config.fileConfig`
   file, as supported by ``pipelinewise-singer-python``.
"""

from __future__ import annotations

import importlib.util
import logging
import logging.config
import os
import sys
import typing as t
from pathlib import Path

import yaml

__all__ = [
    "get_logger",
]

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

#: Formatter references that are rewritten to their ``singer.logging``
#: equivalents when ``singer_sdk`` is not installed, so that logging configs
#: generated for the full SDK (e.g. by Meltano) keep working with this
#: lightweight package.
_SDK_FACTORY_ALIASES = {
    "singer_sdk.logging.ConsoleFormatter": "singer.logging.ConsoleFormatter",
    "singer_sdk.logging.StructuredFormatter": "singer.logging.StructuredFormatter",
}

_configured = False


def _load_dict_config(path: Path) -> dict[str, t.Any]:
    """Load a dictConfig mapping from a JSON or YAML file.

    Args:
        path: Path to the configuration file.

    Returns:
        The configuration dictionary.
    """
    return yaml.safe_load(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _sdk_available() -> bool:
    """Check whether the full Singer SDK is installed.

    Returns:
        True if ``singer_sdk`` is importable.
    """
    return importlib.util.find_spec("singer_sdk") is not None


def _remap_sdk_factories(config: dict[str, t.Any]) -> dict[str, t.Any]:
    """Remap ``singer_sdk`` factory references when the SDK is not installed.

    Args:
        config: A dictConfig mapping.

    Returns:
        The same mapping, with known ``singer_sdk.logging`` factory references
        replaced by their ``singer.logging`` equivalents if ``singer_sdk`` is
        not importable.
    """
    if _sdk_available():
        return config

    for section in ("formatters", "handlers", "filters"):
        for options in config.get(section, {}).values():
            for key in ("()", "class"):
                if (value := options.get(key)) in _SDK_FACTORY_ALIASES:
                    options[key] = _SDK_FACTORY_ALIASES[value]

    return config


def _default_config() -> None:
    """Apply the default legacy logging configuration."""
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        root.addHandler(handler)
        root.setLevel(logging.INFO)


def setup_logging() -> None:
    """Configure logging once, from the environment if possible."""
    global _configured  # noqa: PLW0603
    if _configured:
        return
    _configured = True

    if dict_config := os.environ.get("SINGER_SDK_LOG_CONFIG"):
        config = _load_dict_config(Path(dict_config))
        logging.config.dictConfig(_remap_sdk_factories(config))
    elif file_config := os.environ.get("LOGGING_CONF_FILE"):
        logging.config.fileConfig(file_config, disable_existing_loggers=False)
    else:
        _default_config()


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger, configuring logging on first use.

    Args:
        name: The logger name. If omitted, the root logger is returned, as in
            legacy ``singer-python``.

    Returns:
        A logger instance.
    """
    setup_logging()
    return logging.getLogger(name)
