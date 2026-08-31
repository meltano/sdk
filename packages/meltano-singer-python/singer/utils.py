"""Utility functions for working with Singer streams."""

from __future__ import annotations

import argparse
import json
import sys
import typing as t
from datetime import datetime, timedelta, timezone
from pathlib import Path

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()

DATETIME_FMT = "%04Y-%m-%dT%H:%M:%S.%fZ"
DATETIME_FMT_SAFE = "%Y-%m-%dT%H:%M:%S.%fZ"


class NonUTCDatetimeError(Exception):
    """Raised when a non-UTC datetime is passed to a function expecting UTC."""

    def __init__(self) -> None:
        """Initialize the exception."""
        super().__init__("datetime must be pegged at UTC tzoneinfo")


class ConfigKeyError(Exception):
    """Raised when a required configuration key is missing."""

    def __init__(self, missing_keys: list[str]) -> None:
        """Initialize the exception.

        Args:
            missing_keys: The missing configuration keys.
        """
        super().__init__(f"Config is missing required keys: {missing_keys}")
        self.missing_keys = missing_keys


def strptime_to_utc(dtimestr: str) -> datetime:
    """Parses a provide datetime string into a UTC datetime object.

    Args:
        dtimestr: a string representation of a datetime

    Returns:
        A UTC datetime.datetime object
    """
    d_object: datetime = datetime.fromisoformat(dtimestr)
    if d_object.tzinfo is None:
        return d_object.replace(tzinfo=timezone.utc)

    return d_object.astimezone(tz=timezone.utc)


def strftime(dtime: datetime, format_str: str = DATETIME_FMT) -> str:
    """Formats a provided datetime object as a string.

    Args:
        dtime: a datetime
        format_str: output format specification

    Returns:
        A string in the specified format

    Raises:
        NonUTCDatetimeError: if the datetime is not UTC (if it has a nonzero time zone
            offset)
    """
    if dtime.utcoffset() != timedelta(0):
        raise NonUTCDatetimeError

    dt_str = None
    try:
        dt_str = dtime.strftime(format_str)
        if dt_str.startswith("4Y"):
            dt_str = dtime.strftime(DATETIME_FMT_SAFE)
    except ValueError:
        dt_str = dtime.strftime(DATETIME_FMT_SAFE)
    return dt_str


def now() -> datetime:
    """Return the current UTC time.

    Returns:
        A timezone-aware datetime pegged at UTC.
    """
    return datetime.now(timezone.utc)


def load_json(path: str | Path) -> t.Any:  # noqa: ANN401
    """Load a JSON file.

    Args:
        path: Path to the JSON file.

    Returns:
        The parsed JSON content.
    """
    with Path(path).open(encoding="utf-8") as file:
        return json.load(file)


def check_config(config: dict[str, t.Any], required_keys: t.Iterable[str]) -> None:
    """Check that all required keys are present in the configuration.

    Args:
        config: The configuration dictionary.
        required_keys: The keys that must be present.

    Raises:
        ConfigKeyError: If any required key is missing.
    """
    if missing_keys := [key for key in required_keys if key not in config]:
        raise ConfigKeyError(missing_keys)


def parse_args(required_config_keys: t.Iterable[str]) -> argparse.Namespace:
    """Parse standard Singer command-line arguments.

    Parses ``--config``, ``--state``, ``--catalog`` and ``--discover``,
    loading the file-based arguments as dictionaries (the original paths
    remain available as ``config_path``, ``state_path`` and ``catalog_path``).

    Unlike legacy ``singer-python``, the deprecated ``--properties`` flag is
    not supported; use ``--catalog`` instead.

    Args:
        required_config_keys: Keys that must be present in the config file.

    Returns:
        The parsed arguments namespace.
    """
    from singer.catalog import Catalog  # noqa: PLC0415

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-s", "--state", help="State file")
    parser.add_argument("--catalog", help="Catalog file")
    parser.add_argument(
        "-d",
        "--discover",
        action="store_true",
        help="Do schema discovery",
    )
    args = parser.parse_args()

    args.config_path = args.config
    args.config = load_json(args.config)

    args.state_path = args.state
    args.state = load_json(args.state) if args.state else {}

    args.catalog_path = args.catalog
    if args.catalog:
        args.catalog = Catalog.load(args.catalog)

    check_config(args.config, required_config_keys)

    return args
