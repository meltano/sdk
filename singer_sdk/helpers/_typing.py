"""General helper functions for json typing."""

import copy
import datetime
import logging
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, cast

import pendulum

_MAX_TIMESTAMP = "9999-12-31 23:59:59.999999"
_MAX_TIME = "23:59:59.999999"
JSONSCHEMA_ANNOTATION_SECRET = "secret"
JSONSCHEMA_ANNOTATION_WRITEONLY = "writeOnly"


class DatetimeErrorTreatmentEnum(Enum):
    """Enum for treatment options for date parsing error."""

    ERROR = "error"
    MAX = "max"
    NULL = "null"


def to_json_compatible(val: Any) -> Any:
    """Return as string if datetime. JSON does not support proper datetime types.

    If given a naive datetime object, pendulum automatically makes it utc
    """
    if isinstance(val, (datetime.datetime, pendulum.DateTime)):
        val = pendulum.instance(val).isoformat()
    return val


def append_type(type_dict: dict, new_type: str) -> dict:
    """Return a combined type definition using the 'anyOf' JSON Schema construct."""
    result = copy.deepcopy(type_dict)
    if "anyOf" in result:
        if isinstance(result["anyOf"], list) and new_type not in result["anyOf"]:
            result["anyOf"].append(new_type)
        elif new_type != result["anyOf"]:
            result["anyOf"] = [result["anyOf"], new_type]
        return result

    elif "type" in result:
        if isinstance(result["type"], list) and new_type not in result["type"]:
            result["type"].append(new_type)
        elif new_type != result["type"]:
            result["type"] = [result["type"], new_type]
        return result

    raise ValueError(
        "Could not append type because the JSON schema for the dictionary "
        f"`{type_dict}` appears to be invalid."
    )


def is_secret_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type definition appears to be a secret.

    Will return true if either `writeOnly` or `secret` are true on this type
    or any of the type's subproperties.

    Args:
        type_dict: The JSON Schema type to check.

    Raises:
        ValueError: If type_dict is None or empty.

    Returns:
        True if we detect any sensitive property nodes.
    """
    if type_dict.get(JSONSCHEMA_ANNOTATION_WRITEONLY) or type_dict.get(
        JSONSCHEMA_ANNOTATION_SECRET
    ):
        return True

    if "properties" in type_dict:
        # Recursively check subproperties and return True if any child is secret.
        return any(
            is_secret_type(child_type_dict)
            for child_type_dict in type_dict["properties"].values()
        )

    return False


def is_object_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is an object or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "object" in property_type or property_type == "object":
            return True
    return False


def is_datetime_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type definition is a 'date-time' type.

    Also returns True if 'date-time' is nested within an 'anyOf' type Array.
    """
    if not type_dict:
        raise ValueError(
            "Could not detect type from empty type_dict. "
            "Did you forget to define a property in the stream schema?"
        )
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


def is_date_or_datetime_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type definition is a 'date'/'date-time' type.

    Also returns True if type is nested within an 'anyOf' type Array.

    Args:
        type_dict: The JSON Schema definition.

    Raises:
        ValueError: If type is empty or null.

    Returns:
        True if date or date-time, else False.
    """
    if "anyOf" in type_dict:
        for type_dict in type_dict["anyOf"]:
            if is_date_or_datetime_type(type_dict):
                return True
        return False

    if "type" in type_dict:
        return type_dict.get("format") in {"date", "date-time"}

    raise ValueError(
        f"Could not detect type of replication key using schema '{type_dict}'"
    )


def get_datelike_property_type(property_schema: Dict) -> Optional[str]:
    """Return one of 'date-time', 'time', or 'date' if property is date-like.

    Otherwise return None.
    """
    if _is_string_with_format(property_schema):
        return cast(str, property_schema["format"])
    elif "anyOf" in property_schema:
        for type_dict in property_schema["anyOf"]:
            if _is_string_with_format(type_dict):
                return cast(str, type_dict["format"])
    return None


def _is_string_with_format(type_dict):
    if "string" in type_dict.get("type", []) and type_dict.get("format") in {
        "date-time",
        "time",
        "date",
    }:
        return True


def handle_invalid_timestamp_in_record(
    record,
    key_breadcrumb: List[str],
    invalid_value: str,
    datelike_typename: str,
    ex: Exception,
    treatment: Optional[DatetimeErrorTreatmentEnum],
    logger: logging.Logger,
) -> Any:
    """Apply treatment or raise an error for invalid time values."""
    treatment = treatment or DatetimeErrorTreatmentEnum.ERROR
    msg = (
        f"Could not parse value '{invalid_value}' for "
        f"field '{':'.join(key_breadcrumb)}'."
    )
    if treatment == DatetimeErrorTreatmentEnum.MAX:
        logger.warning(f"{msg}. Replacing with MAX value.\n{ex}\n")
        return _MAX_TIMESTAMP if datelike_typename != "time" else _MAX_TIME

    if treatment == DatetimeErrorTreatmentEnum.NULL:
        logger.warning(f"{msg}. Replacing with NULL.\n{ex}\n")
        return None

    raise ValueError(msg)


def is_string_array_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type definition is a string array."""
    if not type_dict:
        raise ValueError(
            "Could not detect type from empty type_dict. "
            "Did you forget to define a property in the stream schema?"
        )

    if "anyOf" in type_dict:
        return any([is_string_array_type(t) for t in type_dict["anyOf"]])

    if "type" not in type_dict:
        raise ValueError(f"Could not detect type from schema '{type_dict}'")

    return "array" in type_dict["type"] and bool(is_string_type(type_dict["items"]))


def is_array_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type definition is a string array."""
    if not type_dict:
        raise ValueError(
            "Could not detect type from empty type_dict. "
            "Did you forget to define a property in the stream schema?"
        )

    if "anyOf" in type_dict:
        return any([is_array_type(t) for t in type_dict["anyOf"]])

    if "type" not in type_dict:
        raise ValueError(f"Could not detect type from schema '{type_dict}'")

    return "array" in type_dict["type"]


def is_boolean_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a boolean or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "boolean" in property_type or property_type == "boolean":
            return True
    return False


def is_integer_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a boolean or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "integer" in property_type or property_type == "integer":
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
def _warn_unmapped_properties(
    stream_name: str, property_names: Tuple[str], logger: logging.Logger
):
    logger.info(
        f"Properties {property_names} were present in the '{stream_name}' stream but "
        "not found in catalog schema. Ignoring."
    )


def conform_record_data_types(  # noqa: C901
    stream_name: str, record: Dict[str, Any], schema: dict, logger: logging.Logger
) -> Dict[str, Any]:
    """Translate values in record dictionary to singer-compatible data types.

    Any property names not found in the schema catalog will be removed, and a
    warning will be logged exactly once per unmapped property name.
    """
    rec: Dict[str, Any] = {}
    unmapped_properties: List[str] = []
    for property_name, elem in record.items():
        if property_name not in schema["properties"]:
            unmapped_properties.append(property_name)
            continue

        property_schema = schema["properties"][property_name]
        if isinstance(elem, (datetime.datetime, pendulum.DateTime)):
            rec[property_name] = to_json_compatible(elem)
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
    _warn_unmapped_properties(stream_name, tuple(unmapped_properties), logger)
    return rec
