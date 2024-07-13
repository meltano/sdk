"""General helper functions, helper classes, and decorators."""

from __future__ import annotations

import datetime
import decimal
import typing as t
from pathlib import Path, PurePath

import simplejson


def dump_json(obj: t.Any, **kwargs: t.Any) -> str:  # noqa: ANN401
    """Dump json data to a file.

    Args:
        obj: A Python object, usually a dict.
        **kwargs: Optional key word arguments.

    Returns:
        A string of serialized json.

    .. warning:: Do not use this function to serialize Singer messages or bulk data.
                 Use the functions in ``singer_sdk._singerlib.json`` instead.
    """
    return simplejson.dumps(
        obj,
        use_decimal=True,
        separators=(",", ":"),
        **kwargs,
    )


def load_json(json_str: str, **kwargs: t.Any) -> dict:
    """Load json data from a file.

    Args:
        json_str: A valid JSON string.
        **kwargs: Optional key word arguments.

    Returns:
        A Python object, usually a dict.

    .. warning:: Do not use this function to parse Singer messages or bulk data.
                 Use the functions in ``singer_sdk._singerlib.json`` instead.
    """
    return simplejson.loads(  # type: ignore[no-any-return]
        json_str,
        parse_float=decimal.Decimal,
        **kwargs,
    )


def read_json_file(path: PurePath | str) -> dict[str, t.Any]:
    """Read json file, throwing an error if missing."""
    if not path:
        msg = "Could not open file. Filepath not provided."
        raise RuntimeError(msg)

    if not Path(path).exists():
        msg = f"File at '{path}' was not found."
        for template in [f"{path}.template"]:
            if Path(template).exists():
                msg += f"\nFor more info, please see the sample template at: {template}"
        raise FileExistsError(msg)

    return load_json(Path(path).read_text(encoding="utf-8"))


def utc_now() -> datetime.datetime:
    """Return current time in UTC."""
    return datetime.datetime.now(datetime.timezone.utc)
