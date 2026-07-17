"""Logging utilities for the Singer SDK.

The implementation lives in :mod:`singer.logging` from
``meltano-singer-python``. This module is a stable alias for it.
"""

from __future__ import annotations

from singer.logging import DEFAULT_FORMAT, ConsoleFormatter, StructuredFormatter

__all__ = [
    "DEFAULT_FORMAT",
    "ConsoleFormatter",
    "StructuredFormatter",
]
