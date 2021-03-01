"""Helpers for JSONSchema typing.

Usage example:

```py
    jsonschema = PropertiesList(
        IntegerType("id", required=True),
        StringType("name"),
        ArrayType("tags", StringType),
        DateTimeType("updated_on"),
        BooleanType("is_deleted"),
        ComplexType(
            "author",
            StringType("id"),
            StringType("name"),
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

    @property
    def type_dict(self) -> dict:
        raise NotImplementedError()

    def to_dict(self) -> dict:
        type_dict = self.type_dict
        if self.optional:
            type_dict = _append_type(type_dict, "null")
            type_dict["required"] = False
        else:
            type_dict["required"] = True
        return {self.name: type_dict}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class DateTimeType(JSONTypeHelper):
    @property
    def type_dict(self) -> dict:
        return {
            "type": ["string"],
            "format": "date-time",
        }


class StringType(JSONTypeHelper):
    @property
    def type_dict(self) -> dict:
        return {"type": ["string"]}


class BooleanType(JSONTypeHelper):
    @property
    def type_dict(self) -> dict:
        return {"type": ["boolean"]}


class IntegerType(JSONTypeHelper):
    @property
    def type_dict(self) -> dict:
        return {"type": ["integer"]}


class NumberType(JSONTypeHelper):
    @property
    def type_dict(self) -> dict:
        return {"type": ["number"]}


class ComplexType(JSONTypeHelper):
    """Datetime type."""

    def __init__(self, name, *wrapped) -> None:
        self.name = name
        self.wrapped = wrapped
        self.optional = True

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

    def __init__(self, name, wrapped_type, required: bool = False) -> None:
        self.name = name
        self.wrapped_type = wrapped_type
        self.optional = not required

    @property
    def type_dict(self) -> dict:
        wrapped = self.wrapped_type(name=self.name)
        return {"type": "array", "items": wrapped.type_dict}


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
