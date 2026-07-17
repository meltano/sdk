"""Alias for :mod:`singer.utils`."""

from __future__ import annotations

from singer.utils import (
    DATETIME_FMT,
    DATETIME_FMT_SAFE,
    NonUTCDatetimeError,
    strftime,
    strptime_to_utc,
)

__all__ = [
    "DATETIME_FMT",
    "DATETIME_FMT_SAFE",
    "NonUTCDatetimeError",
    "strftime",
    "strptime_to_utc",
]
