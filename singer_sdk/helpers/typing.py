"""Helpers for JSONSchema typing.

Usage example:

```py
    jsonschema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("name", StringType),
        Property("tags", ArrayType(StringType)),
        Property("ratio", NumberType),
        Property("days_active", IntegerType),
        Property("updated_on", DateTimeType),
        Property("is_deleted", BooleanType),
        Property(
            "author",
            Objectype(
                Property("id", StringType),
                Property("name", StringType),
            )
        ),
        Property(
            "groups",
            ArrayType(
                ObjectType(
                    Property("id", StringType),
                    Property("name", StringType),
                )
            )
        ),
    ).to_dict()
```

Note:
- These helpers are designed to output json in the traditional Singer dialect.
- Due to the expansive set of capabilities within the JSONSchema spec, there may be
  other valid implementations which are not syntactically identical to those generated
  here.
"""

import copy
import json
from typing import Any, Iterable, Tuple

from singer_sdk.helpers.classproperty import classproperty


def _append_type(type_dict: dict, new_type: str) -> dict:
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
        raise ValueError(f"Could not detect type from empty type_dict param.")
    if "anyOf" in type_dict:
        for type_dict in type_dict["anyOf"]:
            if is_datetime_type(type_dict):
                return True
        return False
    elif "type" in type_dict:
        if type_dict.get("format") == "date-time":
            return True
        return False
    raise ValueError(
        f"Could not detect type of replication key using schema '{type_dict}'"
    )


class JSONTypeHelper(object):
    """Type helper base class for JSONSchema types."""

    @classproperty
    def type_dict(cls) -> dict:
        raise NotImplementedError()

    def to_dict(self) -> dict:
        return self.type_dict


class DateTimeType(JSONTypeHelper):
    @classproperty
    def type_dict(cls) -> dict:
        return {
            "type": ["string"],
            "format": "date-time",
        }


class StringType(JSONTypeHelper):
    @classproperty
    def type_dict(cls) -> dict:
        return {"type": ["string"]}


class BooleanType(JSONTypeHelper):
    @classproperty
    def type_dict(cls) -> dict:
        return {"type": ["boolean"]}


class IntegerType(JSONTypeHelper):
    @classproperty
    def type_dict(cls) -> dict:
        return {"type": ["integer"]}


class NumberType(JSONTypeHelper):
    @classproperty
    def type_dict(cls) -> dict:
        return {"type": ["number"]}


class ArrayType(JSONTypeHelper):
    """Datetime type."""

    def __init__(self, wrapped_type) -> None:
        self.wrapped_type = wrapped_type

    @property
    def type_dict(self) -> dict:
        return {"type": "array", "items": self.wrapped_type.type_dict}


class Property(JSONTypeHelper):
    """Generic Property. Should be nested within a `PropertiesList`."""

    def __init__(self, name, wrapped, required: bool = False, default=None) -> None:
        self.name = name
        self.wrapped = wrapped
        self.optional = not required
        self.default = default

    @property
    def type_dict(self) -> dict:
        return self.wrapped.type_dict

    def to_dict(self) -> dict:
        type_dict = self.type_dict
        if self.optional:
            type_dict = _append_type(type_dict, "null")
        if self.default:
            type_dict.update({"default": self.default})
        return {self.name: type_dict}


class ObjectType(JSONTypeHelper):
    """Object type, which wraps one or more named properties."""

    def __init__(self, *properties) -> None:
        self.wrapped: List[Property] = properties

    @property
    def type_dict(self) -> dict:
        merged_props = {}
        required = []
        for w in self.wrapped:
            merged_props.update(w.to_dict())
            if not w.optional:
                required.append(w.name)
        return {"type": "object", "properties": merged_props, "required": required}


class PropertiesList(ObjectType):
    """Properties list. A convenience wrapper around the ObjectType class."""

    def items(self) -> Iterable[Tuple[str, Property]]:
        return [(p.name, p) for p in self.wrapped]
