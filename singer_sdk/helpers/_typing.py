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
        type_array = (
            result["type"] if isinstance(result["type"], list) else [result["type"]]
        )
        if new_type not in type_array:
            result["type"] = [*type_array, new_type]
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


def is_uniform_list(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is an array with a single schema.

    This is as opposed to 'tuples' where different indices have different schemas;
    https://json-schema.org/understanding-json-schema/reference/array.html#array
    """
    return (
        is_array_type(property_schema) is True
        and "items" in property_schema
        and "prefixItems" not in property_schema
        and isinstance(property_schema["items"], dict)
    )


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
    """Return True if JSON Schema type is an array."""
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
        if isinstance(property_type, dict):
            property_type = property_type.get("type")
        if "boolean" in property_type or property_type == "boolean":
            return True
    return False


def is_integer_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is an integer or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if isinstance(property_type, dict):
            property_type = property_type.get("type")
        if "integer" in property_type or property_type == "integer":
            return True
    return False


def is_string_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a string or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if isinstance(property_type, dict):
            property_type = property_type.get("type")
        if "string" in property_type or property_type == "string":
            return True
    return False


def is_null_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a null or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if isinstance(property_type, dict):
            property_type = property_type.get("type")
        if "null" in property_type or property_type == "null":
            return True
    return False


def is_number_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a number or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if isinstance(property_type, dict):
            property_type = property_type.get("type")
        if "number" in property_type or property_type == "number":
            return True
    return False


@lru_cache()
def _warn_unmapped_properties(
    stream_name: str, property_names: Tuple[str], logger: logging.Logger
):
    logger.warning(
        f"Properties {property_names} were present in the '{stream_name}' stream but "
        "not found in catalog schema. Ignoring."
    )


class TypeConformanceLevel(Enum):
    """Used to configure how data is conformed to json compatible types.

    Before outputting data as JSON, it is conformed to types that are valid in json,
    based on the current types and the schema. For example, dates are converted to
    strings.

    By default, all data is conformed recursively. If this is not necessary (because
    data is already valid types, or you are manually converting it) then it may be more
    performant to use a lesser conformance level.
    """

    RECURSIVE = 1
    """
    All data is recursively conformed
    """

    ROOT_ONLY = 2
    """
    Only properties on the root object, excluding array elements, are conformed
    """

    NONE = 3
    """
    No conformance is performed
    """


def conform_record_data_types(  # noqa: C901
    stream_name: str,
    record: Dict[str, Any],
    schema: dict,
    level: TypeConformanceLevel,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Translate values in record dictionary to singer-compatible data types.

    Any property names not found in the schema catalog will be removed, and a single
    warning will be logged listing each unmapped property name.
    """
    rec, unmapped_properties = _conform_record_data_types(record, schema, level, None)

    if len(unmapped_properties) > 0:
        _warn_unmapped_properties(stream_name, tuple(unmapped_properties), logger)

    return rec


def _conform_record_data_types(
    input_object: Dict[str, Any],
    schema: dict,
    level: TypeConformanceLevel,
    parent: Optional[str],
) -> Tuple[Dict[str, Any], List[str]]:  # noqa: C901
    """Translate values in record dictionary to singer-compatible data types.

    Any property names not found in the schema catalog will be removed, and a single
    warning will be logged listing each unmapped property name.

    This is called recursively to process nested objects and arrays.

    Args:
        input_object: A single record
        schema: JSON schema the given input_object is expected to meet
        level:  Specifies how recursive the conformance process should be
        parent: '.' seperated path to this element from the object root (for logging)
    """
    output_object: Dict[str, Any] = {}
    unmapped_properties: List[str] = []

    if level == TypeConformanceLevel.NONE:
        return input_object, unmapped_properties

    for property_name, elem in input_object.items():
        property_path = (
            property_name if parent is None else parent + "." + property_name
        )
        if property_name not in schema["properties"]:
            unmapped_properties.append(property_path)
            continue

        property_schema = schema["properties"][property_name]
        if isinstance(elem, list) and is_uniform_list(property_schema):
            if level == TypeConformanceLevel.RECURSIVE:
                item_schema = property_schema["items"]
                output = []
                for item in elem:
                    if is_object_type(item_schema) and isinstance(item, dict):
                        (
                            output_item,
                            sub_unmapped_properties,
                        ) = _conform_record_data_types(
                            item, item_schema, level, property_path
                        )
                        unmapped_properties.extend(sub_unmapped_properties)
                        output.append(output_item)
                    else:
                        output.append(_conform_primitive_property(item, item_schema))
                output_object[property_name] = output
            else:
                output_object[property_name] = elem
        elif (
            isinstance(elem, dict)
            and is_object_type(property_schema)
            and "properties" in property_schema
        ):
            if level == TypeConformanceLevel.RECURSIVE:
                (
                    output_object[property_name],
                    sub_unmapped_properties,
                ) = _conform_record_data_types(
                    elem, property_schema, level, property_path
                )
                unmapped_properties.extend(sub_unmapped_properties)
            else:
                output_object[property_name] = elem
        else:
            output_object[property_name] = _conform_primitive_property(
                elem, property_schema
            )
    return output_object, unmapped_properties


def _conform_primitive_property(elem: Any, property_schema: dict) -> Any:
    """Converts a primitive (i.e. not object or array) to a json compatible type."""
    if isinstance(elem, (datetime.datetime, pendulum.DateTime)):
        return to_json_compatible(elem)
    elif isinstance(elem, datetime.date):
        return elem.isoformat() + "T00:00:00+00:00"
    elif isinstance(elem, datetime.timedelta):
        epoch = datetime.datetime.utcfromtimestamp(0)
        timedelta_from_epoch = epoch + elem
        return timedelta_from_epoch.isoformat() + "+00:00"
    elif isinstance(elem, datetime.time):
        return str(elem)
    elif isinstance(elem, bytes):
        # for BIT value, treat 0 as False and anything else as True
        bit_representation: bool
        if is_boolean_type(property_schema):
            bit_representation = elem != b"\x00"
            return bit_representation
        else:
            return elem.hex()
    elif is_boolean_type(property_schema):
        boolean_representation: Optional[bool]
        if elem is None:
            boolean_representation = None
        elif elem == 0:
            boolean_representation = False
        else:
            boolean_representation = True
        return boolean_representation
    else:
        return elem
