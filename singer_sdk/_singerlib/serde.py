from __future__ import annotations

import datetime
import decimal
import json
import logging
import typing as t

import simplejson

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
