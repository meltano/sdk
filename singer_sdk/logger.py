"""Logging configuration for the Singer SDK."""

from __future__ import annotations

import logging
import os
import typing as t
from pathlib import Path

import yaml

from singer_sdk.metrics import METRICS_LOG_LEVEL_SETTING, METRICS_LOGGER_NAME

if t.TYPE_CHECKING:
    from singer_sdk.helpers._compat import Traversable

__all__ = ["setup_logging"]


def load_yaml_logging_config(path: Traversable | Path) -> t.Any:  # noqa: ANN401
    """Load the logging config from the YAML file.

    Args:
        path: A path to the YAML file.

    Returns:
        The logging config.
    """
    with path.open() as f:
        return yaml.safe_load(f)


def setup_logging(
    config: t.Mapping[str, t.Any],
    default_logging_config: dict[str, t.Any],
) -> None:
    """Setup logging.

    Args:
        default_logging_config: A default
            :py:std:label:`Python logging configuration dictionary`.
        config: A plugin configuration dictionary.
    """
    logging.config.dictConfig(default_logging_config)

    config = config or {}
    metrics_log_level = config.get(METRICS_LOG_LEVEL_SETTING, "INFO").upper()
    logging.getLogger(METRICS_LOGGER_NAME).setLevel(metrics_log_level)

    if "SINGER_SDK_LOG_CONFIG" in os.environ:  # pragma: no cover
        log_config_path = Path(os.environ["SINGER_SDK_LOG_CONFIG"])
        logging.config.dictConfig(load_yaml_logging_config(log_config_path))
