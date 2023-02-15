"""General helper functions, helper classes, and decorators."""

from __future__ import annotations

import json
from pathlib import Path, PurePath
from typing import Any, cast

import pendulum


def read_json_file(path: PurePath | str) -> dict[str, Any]:
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
