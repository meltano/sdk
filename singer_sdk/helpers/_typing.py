"""General helper functions for json typing."""

from __future__ import annotations

import copy
import datetime
import logging
import typing as t
from enum import Enum
from functools import lru_cache

import pendulum

_MAX_TIMESTAMP = "9999-12-31 23:59:59.999999"
_MAX_TIME = "23:59:59.999999"
JSONSCHEMA_ANNOTATION_SECRET = "secret"  # noqa: S105
JSONSCHEMA_ANNOTATION_WRITEONLY = "writeOnly"
UTC = datetime.timezone.utc

logger = logging.getLogger(__name__)


class DatetimeErrorTreatmentEnum(Enum):
    """Enum for treatment options for date parsing error."""

    ERROR = "error"
    MAX = "max"
    NULL = "null"


class EmptySchemaTypeError(Exception):
    """Exception for when trying to detect type from empty type_dict."""

    def __init__(self, *args: object) -> None:
        msg = (
            "Could not detect type from empty type_dict. Did you forget to define a "
            "property in the stream schema?"
        )
        super().__init__(msg, *args)


def to_json_compatible(val: t.Any) -> t.Any:
    """Return as string if datetime. JSON does not support proper datetime types.

    If given a naive datetime object, pendulum automatically makes it utc
    """
    if isinstance(val, (datetime.datetime, pendulum.DateTime)):
        return pendulum.instance(val).isoformat()
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

    if "type" in result:
        type_array = (
            result["type"] if isinstance(result["type"], list) else [result["type"]]
        )
        if new_type not in type_array:
            result["type"] = [*type_array, new_type]
        return result

    logger.warning(
        "Could not append type because the JSON schema for the dictionary "
        "`%s` appears to be invalid.",
        type_dict,
    )
    return result


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
        JSONSCHEMA_ANNOTATION_SECRET,
    ):
        return True

    if "properties" in type_dict:
        # Recursively check subproperties and return True if any child is secret.
        return any(
            is_secret_type(child_type_dict)
            for child_type_dict in type_dict["properties"].values()
        )

    return False


def is_object_type(property_schema: dict) -> bool | None:
    """Return true if the JSON Schema type is an object or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    return any(
        "object" in property_type or property_type == "object"
        for property_type in property_schema.get(
            "anyOf",
            [property_schema.get("type")],
        )
    )


def is_uniform_list(property_schema: dict) -> bool | None:
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
        raise EmptySchemaTypeError
    if "anyOf" in type_dict:
        return any(is_datetime_type(type_dict) for type_dict in type_dict["anyOf"])
    if "type" in type_dict:
        return type_dict.get("format") == "date-time"
    msg = f"Could not detect type of replication key using schema '{type_dict}'"
    raise ValueError(msg)


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
        return any(is_date_or_datetime_type(option) for option in type_dict["anyOf"])

    if "type" in type_dict:
        return type_dict.get("format") in {"date", "date-time"}

    msg = f"Could not detect type of replication key using schema '{type_dict}'"
    raise ValueError(msg)


def get_datelike_property_type(property_schema: dict) -> str | None:
    """Return one of 'date-time', 'time', or 'date' if property is date-like.

    Otherwise return None.
    """
    if _is_string_with_format(property_schema):
        return t.cast(str, property_schema["format"])
    if "anyOf" in property_schema:
        for type_dict in property_schema["anyOf"]:
            if _is_string_with_format(type_dict):
                return t.cast(str, type_dict["format"])
    return None


def _is_string_with_format(type_dict):
    if "string" in type_dict.get("type", []) and type_dict.get("format") in {
        "date-time",
        "time",
        "date",
    }:
        return True
    return None


def handle_invalid_timestamp_in_record(
    record,  # noqa: ARG001
    key_breadcrumb: list[str],
    invalid_value: str,
    datelike_typename: str,
    ex: Exception,
    treatment: DatetimeErrorTreatmentEnum | None,
    logger: logging.Logger,
) -> t.Any:
    """Apply treatment or raise an error for invalid time values."""
    treatment = treatment or DatetimeErrorTreatmentEnum.ERROR
    msg = (
        f"Could not parse value '{invalid_value}' for "
        f"field '{':'.join(key_breadcrumb)}'."
    )
    if treatment == DatetimeErrorTreatmentEnum.MAX:
        logger.warning("%s. Replacing with MAX value.\n%s\n", msg, ex)
        return _MAX_TIMESTAMP if datelike_typename != "time" else _MAX_TIME

    if treatment == DatetimeErrorTreatmentEnum.NULL:
        logger.warning("%s. Replacing with NULL.\n%s\n", msg, ex)
        return None

    raise ValueError(msg)


def is_string_array_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type definition is a string array."""
    if not type_dict:
        raise EmptySchemaTypeError

    if "anyOf" in type_dict:
        return any(is_string_array_type(t) for t in type_dict["anyOf"])

    if "type" not in type_dict:
        msg = f"Could not detect type from schema '{type_dict}'"
        raise ValueError(msg)

    return "array" in type_dict["type"] and bool(is_string_type(type_dict["items"]))


def is_array_type(type_dict: dict) -> bool:
    """Return True if JSON Schema type is an array."""
    if not type_dict:
        raise EmptySchemaTypeError

    if "anyOf" in type_dict:
        return any(is_array_type(t) for t in type_dict["anyOf"])

    if "type" not in type_dict:
        msg = f"Could not detect type from schema '{type_dict}'"
        raise ValueError(msg)

    return "array" in type_dict["type"]


def is_boolean_type(property_schema: dict) -> bool | None:
    """Return true if the JSON Schema type is a boolean or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        schema_type = (
            property_type.get("type", [])
            if isinstance(property_type, dict)
            else property_type
        )
        if "boolean" in schema_type or schema_type == "boolean":
            return True
    return False


def is_integer_type(property_schema: dict) -> bool | None:
    """Return true if the JSON Schema type is an integer or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        schema_type = (
            property_type.get("type", [])
            if isinstance(property_type, dict)
            else property_type
        )
        if "integer" in schema_type or schema_type == "integer":
            return True
    return False


def is_string_type(property_schema: dict) -> bool | None:
    """Return true if the JSON Schema type is a string or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        schema_type = (
            property_type.get("type", [])
            if isinstance(property_type, dict)
            else property_type
        )
        if "string" in schema_type or schema_type == "string":
            return True
    return False


def is_null_type(property_schema: dict) -> bool | None:
    """Return true if the JSON Schema type is a null or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        schema_type = (
            property_type.get("type", [])
            if isinstance(property_type, dict)
            else property_type
        )
        if "null" in schema_type or schema_type == "null":
            return True
    return False


def is_number_type(property_schema: dict) -> bool | None:
    """Return true if the JSON Schema type is a number or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        schema_type = (
            property_type.get("type", [])
            if isinstance(property_type, dict)
            else property_type
        )
        if "number" in schema_type or schema_type == "number":
            return True
    return False


@lru_cache()
def _warn_unmapped_properties(
    stream_name: str,
    property_names: tuple[str],
    logger: logging.Logger,
):
    logger.warning(
        "Properties %s were present in the '%s' stream but "
        "not found in catalog schema. Ignoring.",
        property_names,
        stream_name,
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


def conform_record_data_types(
    stream_name: str,
    record: dict[str, t.Any],
    schema: dict,
    level: TypeConformanceLevel,
    logger: logging.Logger,
) -> dict[str, t.Any]:
    """Translate values in record dictionary to singer-compatible data types.

    Any property names not found in the schema catalog will be removed, and a single
    warning will be logged listing each unmapped property name.
    """
    rec, unmapped_properties = _conform_record_data_types(record, schema, level, None)

    if len(unmapped_properties) > 0:
        _warn_unmapped_properties(stream_name, tuple(unmapped_properties), logger)

    return rec


def _conform_record_data_types(  # noqa: PLR0912
    input_object: dict[str, t.Any],
    schema: dict,
    level: TypeConformanceLevel,
    parent: str | None,
) -> tuple[dict[str, t.Any], list[str]]:
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
    output_object: dict[str, t.Any] = {}
    unmapped_properties: list[str] = []

    if level == TypeConformanceLevel.NONE:
        return input_object, unmapped_properties

    for property_name, elem in input_object.items():
        property_path = property_name if parent is None else f"{parent}.{property_name}"
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
                            item,
                            item_schema,
                            level,
                            property_path,
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
                    elem,
                    property_schema,
                    level,
                    property_path,
                )
                unmapped_properties.extend(sub_unmapped_properties)
            else:
                output_object[property_name] = elem
        else:
            output_object[property_name] = _conform_primitive_property(
                elem,
                property_schema,
            )
    return output_object, unmapped_properties


def _conform_primitive_property(  # noqa: PLR0911
    elem: t.Any,
    property_schema: dict,
) -> t.Any:
    """Converts a primitive (i.e. not object or array) to a json compatible type."""
    if isinstance(elem, (datetime.datetime, pendulum.DateTime)):
        return to_json_compatible(elem)
    if isinstance(elem, datetime.date):
        return f"{elem.isoformat()}T00:00:00+00:00"
    if isinstance(elem, datetime.timedelta):
        epoch = datetime.datetime.fromtimestamp(0, UTC)
        timedelta_from_epoch = epoch + elem
        if timedelta_from_epoch.tzinfo is None:
            timedelta_from_epoch = timedelta_from_epoch.replace(tzinfo=UTC)
        return timedelta_from_epoch.isoformat()
    if isinstance(elem, datetime.time):
        return str(elem)
    if isinstance(elem, bytes):
        # for BIT value, treat 0 as False and anything else as True
        return elem != b"\x00" if is_boolean_type(property_schema) else elem.hex()
    if is_boolean_type(property_schema):
        return None if elem is None else elem != 0
    return elem
