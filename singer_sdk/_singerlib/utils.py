from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

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
