"""General helper functions, helper classes, and decorators."""

from __future__ import annotations

import datetime
import decimal
import typing as t
from pathlib import Path

import simplejson

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import StrPath

from collections.abc import Mapping


def dump_json(obj: t.Any, **kwargs: t.Any) -> str:  # noqa: ANN401
    """Dump json data to a file.

    Args:
        obj: A Python object, usually a dict.
        **kwargs: Optional key word arguments.

    Returns:
        A string of serialized json.

    .. warning:: Do not use this function to serialize Singer messages or bulk data.
                 Use the functions in ``singer_sdk.singerlib.json`` instead.
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
                 Use the functions in ``singer_sdk.singerlib.json`` instead.
    """
    return simplejson.loads(  # type: ignore[no-any-return]
        json_str,
        parse_float=decimal.Decimal,
        **kwargs,
    )


def read_json_file(path: StrPath) -> dict[str, t.Any]:
    """Read json file, throwing an error if missing."""
    if not path:
        msg = "Could not open file. Filepath not provided."
        raise RuntimeError(msg)

    if not Path(path).exists():
        msg = f"File at '{path}' was not found."
        for template in [f"{path}.template"]:
            if Path(template).exists():
                msg += f"\nFor more info, please see the sample template at: {template}"
        raise FileNotFoundError(msg)

    return load_json(Path(path).read_text(encoding="utf-8"))


def utc_now() -> datetime.datetime:
    """Return current time in UTC."""
    return datetime.datetime.now(datetime.timezone.utc)


def get_nested_value(
    record: t.Mapping[str, t.Any],
    dotted_key: str,
    *,
    missing: object | None = None,
) -> t.Any | None:  # noqa: ANN401
    """Retrieve a value from a nested mapping using a dotted key path.

    Args:
        record: The record mapping to traverse.
        dotted_key: A dot-separated key path (e.g. ``"attributes.updated"``).
        missing: Sentinel value to return when any level in the path is missing.
            Defaults to ``None`` for backward-compatible behavior.

    Returns:
        The value at the nested path if it exists. If any level in the path is
        missing, returns ``missing`` instead. This allows callers to distinguish
        between an explicitly stored ``None`` value and a missing path by passing
        a dedicated sentinel (e.g. ``missing=object()``).

    Examples:
        >>> get_nested_value({"a": {"b": 1}}, "a.b")
        1
        >>> get_nested_value({"a": 1}, "a.b")
        None
        >>> sentinel = object()
        >>> get_nested_value({"a": 1}, "a.b", missing=sentinel) is sentinel
        True
        >>> get_nested_value({"a": {"b": None}}, "a.b", missing=sentinel) is None
        True
    """
    keys = dotted_key.split(".")
    current: t.Any = record
    for key in keys:
        if not isinstance(current, Mapping):
            return missing
        if key not in current:
            return missing
        current = current[key]
    return current
