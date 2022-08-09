"""General helper functions, helper classes, and decorators."""

import json
import sys
from pathlib import Path, PurePath
from typing import Any, Dict, Union, cast

import pendulum

if sys.version_info >= (3, 9):
    import importlib.resources as resources
    from importlib.abc import Traversable
else:
    import importlib_resources as resources
    from importlib_resources.abc import Traversable


def read_json_file(path: Union[PurePath, str]) -> Dict[str, Any]:
    """Read json file, thowing an error if missing.

    Args:
        path: The path to the file to read.

    Returns:
        The contents of the file as a dictionary.

    Raises:
        RuntimeError: If the path is empty.
        FileNotFoundError: If the file does not exist.
    """
    if not path:
        raise RuntimeError("Could not open file. Filepath not provided.")

    if not Path(path).exists():
        msg = f"File at '{path}' was not found."
        for template in [f"{path}.template"]:
            if Path(template).exists():
                msg += f"\nFor more info, please see the sample template at: {template}"
        raise FileNotFoundError(msg)

    return cast(dict, json.loads(Path(path).read_text()))


def utc_now() -> pendulum.DateTime:
    """Return current time in UTC.

    Returns:
        The current time in UTC.
    """
    return pendulum.now(tz="UTC")


def get_package_files(package: resources.Package) -> Traversable:
    """Load a static files path from a package.

    This is a wrapper around importlib.resources.files().

    Args:
        package: The package to load the resource from.

    Returns:
        The resource as a Traversable object.
    """
    return resources.files(package)
