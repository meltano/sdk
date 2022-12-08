"""Compatibility helpers."""
from __future__ import annotations

import pathlib

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


__all__ = ["metadata", "get_project_distribution", "final"]


# Future: replace with `importlib.metadata.packages_distributions()` introduced in 3.10
def get_project_distribution(file_path=None) -> metadata.Distribution | None:
    """Get project distribution.

    This walks each distribution on `sys.path` looking for one whose installed paths
    include the given path.

    Args:
        file_path: File path to find distribution for.

    Returns:
        A discovered Distribution or None.
    """
    for dist in metadata.distributions():
        try:
            relative = pathlib.Path(file_path or __file__).relative_to(
                dist.locate_file("")
            )
        except ValueError:
            pass
        else:
            if dist.files and relative in dist.files:
                return dist
    return None
