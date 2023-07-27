"""Compatibility helpers."""

from __future__ import annotations

import sys

if sys.version_info < (3, 8):
    import importlib_metadata as metadata
    from typing_extensions import Protocol, final
else:
    from importlib import metadata
    from typing import Protocol, final  # noqa: ICN003

if sys.version_info < (3, 9):
    import importlib_resources as resources
else:
    from importlib import resources

__all__ = ["metadata", "final", "resources", "Protocol"]
