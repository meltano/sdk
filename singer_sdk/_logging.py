from __future__ import annotations

import logging
import os
import sys
import typing as t
from pathlib import Path

from singer_sdk.metrics import METRICS_LOGGER_NAME, SingerMetricsFormatter

if t.TYPE_CHECKING:
    from singer_sdk.helpers._compat import Traversable


def _load_yaml_logging_config(path: Traversable | Path) -> t.Any:  # noqa: ANN401 # pragma: no cover
    """Load the logging config from the YAML file.

    Args:
        path: A path to the YAML file.

    Returns:
        The logging config.
    """
    import yaml  # noqa: PLC0415

    with path.open() as f:
        return yaml.safe_load(f)


def _setup_console_logging() -> None:
    """Setup logging.

    Args:
        package: The package name to get the logging configuration for.
        config: A plugin configuration dictionary.
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root_formatter = logging.Formatter(
        "{asctime:23s} | {levelname:8s} | {name:20s} | {message}",
        style="{",
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(root_formatter)
    root.addHandler(handler)

    metrics_logger = logging.getLogger(METRICS_LOGGER_NAME)
    metrics_logger.setLevel(logging.INFO)
    metrics_formatter = SingerMetricsFormatter(
        "{asctime:23s} | {levelname:8s} | {name:20s} | {message}: {metric_json}",
        style="{",
    )
    handler.setFormatter(metrics_formatter)
    metrics_logger.addHandler(handler)

    if "SINGER_SDK_LOG_CONFIG" in os.environ:  # pragma: no cover
        log_config_path = Path(os.environ["SINGER_SDK_LOG_CONFIG"])
        logging.config.dictConfig(_load_yaml_logging_config(log_config_path))
