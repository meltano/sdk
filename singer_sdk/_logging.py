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
