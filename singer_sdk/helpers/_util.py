"""General helper functions, helper classes, and decorators."""

import json
from pathlib import Path, PurePath
from typing import Any, Dict, Union

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

    return json.loads(Path(path).read_text())


def utc_now():
    """Return current time in UTC."""
    return pendulum.utcnow()
