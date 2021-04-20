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

__all__ = ["metadata", "final"]
