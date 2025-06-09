from __future__ import annotations

import logging
import os
import sys
import typing as t
from pathlib import Path

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


def _setup_console_logging(*, log_level: str | int | None = None) -> None:
    """Setup logging.

    Args:
        log_level: The log level to set.
    """
    level = log_level or logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    root_formatter = logging.Formatter(
        "{asctime:23s} | {levelname:8s} | {name:20s} | {message}",
        style="{",
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(root_formatter)
    root.addHandler(handler)

    if "SINGER_SDK_LOG_CONFIG" in os.environ:  # pragma: no cover
        log_config_path = Path(os.environ["SINGER_SDK_LOG_CONFIG"])
        logging.config.dictConfig(_load_yaml_logging_config(log_config_path))
