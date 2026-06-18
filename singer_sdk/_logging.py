from __future__ import annotations

import logging
import logging.config
import os
import sys
import typing as t
from pathlib import Path

import singer_sdk.logging

if t.TYPE_CHECKING:
    from singer_sdk.helpers._compat import Traversable

logger = logging.getLogger(__name__)


def _load_yaml_logging_config(path: Traversable | Path) -> dict:  # pragma: no cover
    """Load the logging config from the YAML file.

    Args:
        path: A path to the YAML file.

    Returns:
        The logging config.
    """
    import yaml  # noqa: PLC0415

    with path.open() as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError("Logging config must be a dictionary.")

    allowed_keys = {"version", "formatters", "handlers", "loggers", "root"}
    if any(key not in allowed_keys for key in config):
        raise ValueError(
            f"Logging config contains invalid keys: "
            f"{[key for key in config if key not in allowed_keys]!r}"
        )

    allowed_handler_types = {
        "logging.StreamHandler",
        "logging.FileHandler",
        "logging.handlers.RotatingFileHandler",
        "logging.handlers.TimedRotatingFileHandler",
        "logging.NullHandler",
    }
    handlers = config.get("handlers", {})
    if not isinstance(handlers, dict):
        raise ValueError("Logging config 'handlers' must be a dictionary.")

    for handler_name, handler_config in handlers.items():
        if not isinstance(handler_config, dict):
            raise ValueError(f"Handler config for {handler_name!r} must be a dictionary.")
        handler_class = handler_config.get("class")
        if handler_class not in allowed_handler_types:
            raise ValueError(
                f"Handler class {handler_class!r} for handler {handler_name!r} is not allowed. "
                f"Allowed handler classes are: {', '.join(sorted(allowed_handler_types))}"
            )

    return config


def _setup_console_logging(*, log_level: str | int | None = None) -> None:
    """Setup logging.

    Args:
        log_level: The log level to set.
    """
    level = log_level or logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    root_formatter = singer_sdk.logging.ConsoleFormatter()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(root_formatter)
    root.addHandler(handler)

    if "SINGER_SDK_LOG_CONFIG" in os.environ:  # pragma: no cover
        log_config_path = Path(os.environ["SINGER_SDK_LOG_CONFIG"])
        try:
            logging.config.dictConfig(_load_yaml_logging_config(log_config_path))
        except FileNotFoundError:
            logger.warning("Logging config file not found: %s", log_config_path)
