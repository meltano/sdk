"""Helpers for JSONSchema typing.

Usage example:
----------
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
----
- These helpers are designed to output json in the traditional Singer dialect.
- Due to the expansive set of capabilities within the JSONSchema spec, there may be
  other valid implementations which are not syntactically identical to those generated
  here.

"""

import copy
from jsonschema import validators
from typing import List, Tuple

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
        raise ValueError("Could not detect type from empty type_dict param.")
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


def extend_with_default(validator_class):
    """Fill in defaults,  before validating.

    See https://python-jsonschema.readthedocs.io/en/latest/faq/#why-doesn-t-my-schema-s-default-property-set-the-default-on-my-instance  # noqa
    for details.
    """
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])

        for error in validate_properties(
            validator,
            properties,
            instance,
            schema,
        ):
            yield error

    return validators.extend(
        validator_class,
        {"properties": set_defaults},
    )


class JSONTypeHelper(object):
    """Type helper base class for JSONSchema types."""

    @classproperty
    def type_dict(cls) -> dict:
        """Return dict describing the type."""
        raise NotImplementedError()

    def to_dict(self) -> dict:
        """Return dict describing the object."""
        return self.type_dict


class DateTimeType(JSONTypeHelper):
    """DateTime type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Return dict describing the type."""
        return {
            "type": ["string"],
            "format": "date-time",
        }


class StringType(JSONTypeHelper):
    """String type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Return dict describing the type."""
        return {"type": ["string"]}


class BooleanType(JSONTypeHelper):
    """Boolean type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Return dict describing the type."""
        return {"type": ["boolean"]}


class IntegerType(JSONTypeHelper):
    """Integer type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Return dict describing the type."""
        return {"type": ["integer"]}


class NumberType(JSONTypeHelper):
    """Number type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Return dict describing the type."""
        return {"type": ["number"]}


class ArrayType(JSONTypeHelper):
    """Array type."""

    def __init__(self, wrapped_type) -> None:
        """Initialize Array type with wrapped inner type."""
        self.wrapped_type = wrapped_type

    @property
    def type_dict(self) -> dict:
        """Return dict describing the type."""
        return {"type": "array", "items": self.wrapped_type.type_dict}


class Property(JSONTypeHelper):
    """Generic Property. Should be nested within a `PropertiesList`."""

    def __init__(self, name, wrapped, required: bool = False, default=None) -> None:
        """Initialize Property object."""
        self.name = name
        self.wrapped = wrapped
        self.optional = not required
        self.default = default

    @property
    def type_dict(self) -> dict:
        """Return dict describing the type."""
        return self.wrapped.type_dict

    def to_dict(self) -> dict:
        """Return a dict mapping the property name to its definition."""
        type_dict = self.type_dict
        if self.optional:
            type_dict = _append_type(type_dict, "null")
        if self.default:
            type_dict.update({"default": self.default})
        return {self.name: type_dict}


class ObjectType(JSONTypeHelper):
    """Object type, which wraps one or more named properties."""

    def __init__(self, *properties) -> None:
        """Initialize ObjectType from its list of properties."""
        self.wrapped: List[Property] = list(properties)

    @property
    def type_dict(self) -> dict:
        """Return dict describing the type."""
        merged_props = {}
        required = []
        for w in self.wrapped:
            merged_props.update(w.to_dict())
            if not w.optional:
                required.append(w.name)
        return {"type": "object", "properties": merged_props, "required": required}


class PropertiesList(ObjectType):
    """Properties list. A convenience wrapper around the ObjectType class."""

    def items(self) -> List[Tuple[str, Property]]:
        """Return list of (name, property) tuples."""
        return [(p.name, p) for p in self.wrapped]
