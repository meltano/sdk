from __future__ import annotations

import sys
from types import ModuleType

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
    from importlib.abc import Traversable
else:
    import importlib_resources
    from importlib_resources.abc import Traversable


def get_package_files(package: str | ModuleType) -> Traversable:
    """Load a file from a package.

    Args:
        package: The package to load the file from.
        file: The file to load.

    Returns:
        The file as a Traversable object.
    """
    return importlib_resources.files(package)
