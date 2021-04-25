"""Classes and functions to streamline JSONSchema typing.

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

from jsonschema import validators
from typing import List, Tuple

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._typing import append_type


def extend_validator_with_defaults(validator_class):
    """Fill in defaults, before validating with the provided JSON Schema Validator.

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
            type_dict = append_type(type_dict, "null")
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
