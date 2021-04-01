"""General helper functions, helper classes, and decorators."""

import datetime
import json
import logging
from decimal import Decimal
from functools import lru_cache
from pathlib import Path, PurePath
from typing import Any, Dict, List, Optional, Union, cast

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


def get_property_schema(schema: dict, property: str) -> Optional[dict]:
    """Given the provided JSON Schema, return the property by name specified.

    If property name does not exist in schema, return None.
    """
    if property not in schema["properties"]:
        return None
    return schema["properties"][property]


def is_boolean_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a boolean or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "boolean" in property_type or property_type == "boolean":
            return True
    return False


@lru_cache()
def _warn_unmapped_property(
    stream_name: str, property_name: str, logger: logging.Logger
):
    logger.warning(
        f"Property '{property_name}' was present in the '{stream_name}' stream but "
        "not found in catalog schema. Ignoring."
    )


def conform_record_data_types(  # noqa: C901
    stream_name: str, row: Dict[str, Any], schema: dict, logger: logging.Logger
) -> Dict[str, Any]:
    """Translate values in record dictionary to singer-compatible data types.

    Any property names not found in the schema catalog will be removed, and a
    warning will be logged exactly once per unmapped property name.
    """
    rec: Dict[str, Any] = {}
    for property_name, elem in row.items():
        property_schema = get_property_schema(schema or {}, property_name)
        if not property_schema:
            _warn_unmapped_property(stream_name, property_name, logger)
            continue
        if isinstance(elem, datetime.datetime):
            rec[property_name] = elem.isoformat() + "+00:00"
        elif isinstance(elem, datetime.date):
            rec[property_name] = elem.isoformat() + "T00:00:00+00:00"
        elif isinstance(elem, datetime.timedelta):
            epoch = datetime.datetime.utcfromtimestamp(0)
            timedelta_from_epoch = epoch + elem
            rec[property_name] = timedelta_from_epoch.isoformat() + "+00:00"
        elif isinstance(elem, datetime.time):
            rec[property_name] = str(elem)
        elif isinstance(elem, bytes):
            # for BIT value, treat 0 as False and anything else as True
            bit_representation: bool
            if is_boolean_type(property_schema):
                bit_representation = elem != b"\x00"
                rec[property_name] = bit_representation
            else:
                rec[property_name] = elem.hex()
        elif is_boolean_type(property_schema):
            boolean_representation: Optional[bool]
            if elem is None:
                boolean_representation = None
            elif elem == 0:
                boolean_representation = False
            else:
                boolean_representation = True
            rec[property_name] = boolean_representation
        else:
            rec[property_name] = elem
    return rec
