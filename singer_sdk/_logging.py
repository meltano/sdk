from __future__ import annotations

__lazy_modules__ = {
    "logging.config",
    "pathlib",
    "singer_sdk.helpers._compat",
    "singer_sdk.logging",
    "warnings",
}

import logging
import logging.config
import os
import sys
import typing as t
import warnings
from pathlib import Path

import singer_sdk.logging

if t.TYPE_CHECKING:
    from singer_sdk.helpers._compat import Traversable

logger = logging.getLogger(__name__)
warnings_logger = logging.getLogger("singer_sdk.warnings")


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


def _log_warning(
    message: Warning | str,
    category: type[Warning],
    filename: str,
    lineno: int,
    file: t.TextIO | None = None,  # noqa: ARG001
    line: str | None = None,  # noqa: ARG001
) -> None:
    """Route a warning to the `singer_sdk.warnings` logger.

    Replaces the standard library's `logging.captureWarnings`, which emits a
    preformatted string through the `py.warnings` logger. Here the category and
    source location are attached as structured fields instead.

    Args:
        message: The warning message or instance.
        category: The warning category.
        filename: The file where the warning was issued.
        lineno: The line number where the warning was issued.
        file: Unused. Required by the `warnings.showwarning` signature.
        line: Unused. Required by the `warnings.showwarning` signature.
    """
    warnings_logger.warning(
        "%s: %s",
        category.__name__,
        message,
        extra={
            "warning_category": category.__name__,
            "warning_source": f"{filename}:{lineno}",
        },
    )


def _setup_warning_logging() -> None:
    """Send warnings through the logging system as structured records."""
    warnings.showwarning = _log_warning  # ty: ignore[invalid-assignment]
