"""General helper functions, helper classes, and decorators."""

from __future__ import annotations

import datetime
import decimal
import json
import logging
import sys
import typing as t
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
    return obj.isoformat(sep="T") if isinstance(obj, datetime.datetime) else str(obj)


def deserialize_json(json_str: str, **kwargs: t.Any) -> dict:
    """Deserialize a line of json.

    Args:
        json_str: A single line of json.
        **kwargs: Optional key word arguments.

    Returns:
        A dictionary of the deserialized json.

    Raises:
        json.decoder.JSONDecodeError: raised if any lines are not valid json
    """
    try:
        return json.loads(  # type: ignore[no-any-return]
            json_str,
            parse_float=decimal.Decimal,
            **kwargs,
        )
    except json.decoder.JSONDecodeError as exc:
        logger.exception("Unable to parse:\n%s", json_str, exc_info=exc)
        raise


def serialize_json(obj: object, **kwargs: t.Any) -> str:
    """Serialize a dictionary into a line of json.

    Args:
        obj: A Python object usually a dict.
        **kwargs: Optional key word arguments.

    Returns:
        A string of serialized json.
    """
    return simplejson.dumps(
        obj,
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

    return deserialize_json(Path(path).read_text(encoding="utf-8"))


def utc_now() -> datetime.datetime:
    """Return current time in UTC."""
    # TODO: replace with datetime.datetime.now(tz=datetime.timezone.utc)
    return pendulum.now(tz="UTC")
