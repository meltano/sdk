"""Compatibility helpers."""

try:
    from typing import final
except ImportError:
    # Final not available until Python3.8
    final = lambda f: f  # noqa: E731

try:
    from importlib import metadata
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata as metadata  # type: ignore

try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property

__all__ = ["metadata", "final", "cached_property"]
