"""General helper functions, helper classes, and decorators."""

import json
import re
from pathlib import Path, PurePath
from typing import Any, Dict, Union, cast

import pendulum


def read_json_file(path: Union[PurePath, str]) -> Dict[str, Any]:
    """Read json file, thowing an error if missing."""
    if not path:
        raise RuntimeError("Could not open file. Filepath not provided.")

    if not Path(path).exists():
        msg = f"File at '{path}' was not found."
        for template in [f"{path}.template"]:
            if Path(template).exists():
                msg += f"\nFor more info, please see the sample template at: {template}"
        raise FileExistsError(msg)

    return cast(dict, json.loads(Path(path).read_text()))


def utc_now() -> pendulum.DateTime:
    """Return current time in UTC."""
    return pendulum.now(tz="UTC")


def snakecase(string):
    """Convert string into snake case.

    Args:
        string: String to convert.

    Returns:
        string: Snake cased string.
    """
    string = re.sub(r"[\-\.\s]", "_", str(string))
    string = (
        string[0].lower()
        + re.sub(r"[A-Z]", lambda matched: f"_{matched.group(0).lower()}", string[1:])
        if string
        else string
    )
    return re.sub(r"_{2,}", "_", string).rstrip("_")
