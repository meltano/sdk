from __future__ import annotations

import sys
import typing as t

if t.TYPE_CHECKING:
    from types import ModuleType

    from singer_sdk.helpers._compat import Traversable

if sys.version_info < (3, 10):
    import importlib_resources
else:
    import importlib.resources as importlib_resources


def get_package_files(package: str | ModuleType) -> Traversable:
    """Load a file from a package.

    Args:
        package: The package to load the file from.
        file: The file to load.

    Returns:
        The file as a Traversable object.
    """
    return importlib_resources.files(package)
