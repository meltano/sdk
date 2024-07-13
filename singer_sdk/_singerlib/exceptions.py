from __future__ import annotations

__all__ = [
    "InvalidInputLine",
]


class InvalidInputLine(Exception):
    """Raised when an input line is not a valid Singer message."""
