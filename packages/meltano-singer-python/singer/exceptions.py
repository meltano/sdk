"""Singer exceptions."""

from __future__ import annotations

__all__ = [
    "InvalidInputLine",
    "SingerError",
    "SingerReadError",
]


class SingerError(Exception):
    """Root base class for all Singer exceptions."""


class SingerReadError(SingerError):
    """Root base class for all Singer read errors."""


class InvalidInputLine(SingerReadError):
    """Raised when an input line is not a valid Singer message."""
