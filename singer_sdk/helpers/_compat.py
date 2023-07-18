"""Compatibility helpers."""

from __future__ import annotations

import sys
from importlib import metadata
from typing import final  # noqa: ICN003

if sys.version_info < (3, 9):
    import importlib_resources as resources
else:
    from importlib import resources

__all__ = ["metadata", "final", "resources"]
