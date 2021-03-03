"""Helpers for JSONSchema typing.

Usage example:

```py
    jsonschema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("name", StringType,
        Property("tags", ArrayType(StringType)),
        Property("updated_on", DateTimeType),
        Property("is_deleted", BooleanType,
        Property(
            "author",
            ComplexType(
                Property("id", StringType),
                Property("name", StringType),
            )
        ),
    ).to_json()
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


class JSONTypeHelper(object):
    """Type helper base class for JSONSchema types."""

    def __init__(self, name: str, required: bool = False) -> None:
        self.name = name
        self.optional = not required

    @classproperty
    def type_dict(cls) -> dict:
        raise NotImplementedError()

    def to_dict(self) -> dict:
        type_dict = self.type_dict
        if self.optional:
            type_dict = _append_type(type_dict, "null")
        return {self.name: type_dict}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


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


class ComplexType(JSONTypeHelper):
    """ComplexType. This is deprecated in favor of ObjectType."""

    def __init__(self, name, *wrapped, required: bool = False) -> None:
        self.name = name
        self.wrapped = wrapped
        self.optional = not required

    @property
    def type_dict(self) -> dict:
        merged_props = {}
        required = []
        for w in self.wrapped:
            merged_props.update(w.to_dict())
            if not w.optional:
                required.append(w.name)
        return {"type": "object", "properties": merged_props, "required": required}


class ArrayType(JSONTypeHelper):
    """Datetime type."""

    def __init__(self, wrapped_type, name: str = None, required: bool = False) -> None:
        self.name = name
        self.wrapped_type = wrapped_type
        self.optional = not required

    @property
    def type_dict(self) -> dict:
        return {"type": "array", "items": self.wrapped_type.type_dict}



class ObjectType(JSONTypeHelper):
    """Object type. This supercedes ComplexType."""

    def __init__(self, *properties, required: bool = False) -> None:
        self.wrapped = properties
        self.optional = not required

    @property
    def type_dict(self) -> dict:
        merged_props = {}
        required = []
        for w in self.wrapped:
            merged_props.update(w.to_dict())
            if not w.optional:
                required.append(w.name)
        return {"type": "object", "properties": merged_props, "required": required}


class Property(JSONTypeHelper):
    """Generic Property. Should be nested within a `PropertiesList` or `ComplexType`."""

    def __init__(self, name, wrapped, required: bool = False) -> None:
        self.name = name
        self.wrapped = wrapped
        self.optional = not required

    @property
    def type_dict(self) -> dict:
        return self.wrapped.type_dict


class PropertiesList(ComplexType):
    def __init__(self, *wrapped) -> None:
        super().__init__(None, *wrapped)
        self.wrapped = wrapped

    @property
    def type_dict(self) -> dict:
        # return super().type_dict["properties"]
        return super().type_dict

    def to_dict(self) -> dict:
        return self.type_dict

    def items(self) -> Iterable[Tuple[str, Any]]:
        return self.to_dict().items()
