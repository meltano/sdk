"""Compatibility helpers."""

from __future__ import annotations

import sys

if sys.version_info < (3, 8):
    import importlib_metadata as metadata
    from typing_extensions import final
else:
    from importlib import metadata
    from typing import final

__all__ = ["metadata", "final"]
