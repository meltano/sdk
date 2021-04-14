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
    from functools import cache  # type: ignore
except ImportError:
    # Running on pre-3.9 Python; use lru_cache(maxsize=None)
    from functools import lru_cache

    cache = lambda fn: lru_cache(maxsize=None)(fn)  # noqa: E731


__all__ = ["metadata", "final", "cache"]
