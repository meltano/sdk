"""General helper functions, helper classes, and decorators."""

from __future__ import annotations

import decimal
import json
import logging
import sys
import typing as t
from datetime import datetime
from pathlib import Path, PurePath

import pendulum
import simplejson

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()


logger = logging.getLogger(__name__)


def _default_encoding(obj: t.Any) -> str:  # noqa: ANN401
    """Default JSON encoder.

    Args:
        obj: The object to encode.

    Returns:
        The encoded object.
    """
    return obj.isoformat(sep="T") if isinstance(obj, datetime) else str(obj)


def deserialize_json(line: str, **kwargs: t.Any) -> dict:
    """Deserialize a line of json.

    Args:
        line: A single line of json.
        **kwargs: Optional key word arguments.

    Returns:
        A dictionary of the deserialized json.

    Raises:
        json.decoder.JSONDecodeError: raised if any lines are not valid json
    """
    try:
        return json.loads(  # type: ignore[no-any-return]
            line,
            parse_float=decimal.Decimal,
            **kwargs,
        )
    except json.decoder.JSONDecodeError as exc:
        logger.error("Unable to parse:\n%s", line, exc_info=exc)
        raise


def serialize_json(line_dict: dict, **kwargs: t.Any) -> str:
    """Serialize a dictionary into a line of json.

    Args:
        line_dict: A Python dict.
        **kwargs: Optional key word arguments.

    Returns:
        A string of serialized json.
    """
    return simplejson.dumps(
        line_dict,
        use_decimal=True,
        default=_default_encoding,
        separators=(",", ":"),
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

    return t.cast(dict, deserialize_json(Path(path).read_text()))


def utc_now() -> pendulum.DateTime:
    """Return current time in UTC."""
    return pendulum.now(tz="UTC")
