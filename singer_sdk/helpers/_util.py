"""General helper functions, helper classes, and decorators."""

import json
import os
from typing import Any, Dict, cast

import pendulum

from singer_sdk._python_types import _FilePath


def read_json_file(path: _FilePath) -> Dict[str, Any]:
    """Read json file, throwing an error if missing."""
    if not path:
        raise RuntimeError("Could not open file. Filepath not provided.")

    if not os.path.exists(path):
        msg = f"File at '{path!r}' was not found."
        for template in [f"{path!r}.template"]:
            if os.path.exists(template):
                msg += f"\nFor more info, please see the sample template at: {template}"
        raise FileExistsError(msg)

    with open(path) as f:
        return cast(dict, json.load(f))


def utc_now() -> pendulum.DateTime:
    """Return current time in UTC."""
    return pendulum.now(tz="UTC")
