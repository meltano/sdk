"""General helper functions for json typing."""

import copy
import datetime
import logging
from functools import lru_cache
from typing import Optional, Dict, Any


def append_type(type_dict: dict, new_type: str) -> dict:
    """Return a combined type definition using the 'anyOf' JSON Schema construct."""
    result = copy.deepcopy(type_dict)
    if "anyOf" in result:
        if isinstance(result["anyOf"], list) and new_type not in result["anyOf"]:
            result["anyOf"].append(new_type)
        elif new_type != result["anyOf"]:
            result["anyOf"] = [result["anyOf"], new_type]
    elif "type" in result:
        if isinstance(result["type"], list) and new_type not in result["type"]:
            result["type"].append(new_type)
        elif new_type != result["type"]:
            result["type"] = [result["type"], new_type]
    else:
        raise ValueError("Could not append type because type was not detected.")
    return result


def is_datetime_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type definition is a 'date-time' type.

    Also returns True if 'date-time' is nested within an 'anyOf' type Array.
    """
    if not type_dict:
        raise ValueError("Could not detect type from empty type_dict param.")
    if "anyOf" in type_dict:
        for type_dict in type_dict["anyOf"]:
            if is_datetime_type(type_dict):
                return True
        return False
    elif "type" in type_dict:
        return type_dict.get("format") == "date-time"
    raise ValueError(
        f"Could not detect type of replication key using schema '{type_dict}'"
    )


def is_string_array_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type definition is a string array."""
    if not type_dict:
        raise ValueError("Could not detect type from empty type_dict param.")

    if "anyOf" in type_dict:
        return any([is_string_array_type(t) for t in type_dict["anyOf"]])

    if "type" not in type_dict:
        raise ValueError(f"Could not detect type from schema '{type_dict}'")

    return type_dict["type"] == "array" and is_string_type(type_dict["items"])


def is_boolean_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a boolean or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "boolean" in property_type or property_type == "boolean":
            return True
    return False


def is_string_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a boolean or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "string" in property_type or property_type == "string":
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
        if property_name not in schema["properties"]:
            _warn_unmapped_property(stream_name, property_name, logger)
            continue

        property_schema = schema["properties"][property_name]
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
