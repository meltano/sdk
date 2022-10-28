"""Classes and functions to streamline JSONSchema typing.

Usage example:
--------------
.. code-block:: python

    jsonschema = PropertiesList(
        Property("username", StringType, required=True),
        Property("password", StringType, required=True, secret=True),

        Property("id", IntegerType, required=True),
        Property("foo_or_bar", StringType, allowed_values=["foo", "bar"]),
        Property("ratio", NumberType, examples=[0.25, 0.75, 1.0]),
        Property("days_active", IntegerType),
        Property("updated_on", DateTimeType),
        Property("is_deleted", BooleanType),

        Property(
            "author",
            ObjectType(
                Property("id", StringType),
                Property("name", StringType),
            )
        ),
        Property("tags", ArrayType(StringType)),
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

Note:
-----
- These helpers are designed to output json in the traditional Singer dialect.
- Due to the expansive set of capabilities within the JSONSchema spec, there may be
  other valid implementations which are not syntactically identical to those generated
  here.

"""

from __future__ import annotations

import sys
from typing import Any, Generic, Mapping, TypeVar, Union, cast

import sqlalchemy
from jsonschema import validators

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._typing import (
    JSONSCHEMA_ANNOTATION_SECRET,
    JSONSCHEMA_ANNOTATION_WRITEONLY,
    append_type,
    get_datelike_property_type,
)

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

__all__ = [
    "extend_validator_with_defaults",
    "to_jsonschema_type",
    "to_sql_type",
    "JSONTypeHelper",
    "StringType",
    "DateTimeType",
    "TimeType",
    "DateType",
    "DurationType",
    "EmailType",
    "HostnameType",
    "IPv4Type",
    "IPv6Type",
    "UUIDType",
    "URIType",
    "URIReferenceType",
    "URITemplateType",
    "JSONPointerType",
    "RelativeJSONPointerType",
    "RegexType",
    "BooleanType",
    "IntegerType",
    "NumberType",
    "ArrayType",
    "Property",
    "ObjectType",
    "CustomType",
    "PropertiesList",
]

_JsonValue: TypeAlias = Union[
    str,
    int,
    float,
    bool,
    list,
    dict,
    None,
]


def extend_validator_with_defaults(validator_class):  # noqa
    """Fill in defaults, before validating with the provided JSON Schema Validator.

    See https://python-jsonschema.readthedocs.io/en/latest/faq/#why-doesn-t-my-schema-s-default-property-set-the-default-on-my-instance  # noqa
    for details.
    """
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):  # noqa
        for property, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])

        yield from validate_properties(
            validator,
            properties,
            instance,
            schema,
        )

    return validators.extend(
        validator_class,
        {"properties": set_defaults},
    )


class JSONTypeHelper:
    """Type helper base class for JSONSchema types."""

    @classproperty
    def type_dict(cls) -> dict:
        """Return dict describing the type.

        Raises:
            NotImplementedError: If the derived class does not override this method.
        """
        raise NotImplementedError()

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            A JSON Schema dictionary describing the object.
        """
        return cast(dict, self.type_dict)


class StringType(JSONTypeHelper):
    """String type."""

    string_format: str | None = None
    """String format.

    See the [formats built into the JSON Schema\
    specification](https://json-schema.org/understanding-json-schema/reference/string.html#built-in-formats).

    Returns:
        A string describing the format.
    """

    @classproperty
    def _format(cls) -> dict:
        return {"format": cls.string_format} if cls.string_format else {}

    @classproperty
    def type_dict(cls) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {
            "type": ["string"],
            **cls._format,
        }


class DateTimeType(StringType):
    """DateTime type.

    Example: `2018-11-13T20:20:39+00:00`
    """

    string_format = "date-time"


class TimeType(StringType):
    """Time type.

    Example: `20:20:39+00:00`
    """

    string_format = "time"


class DateType(StringType):
    """Date type.

    Example: `2018-11-13`
    """

    string_format = "date"


class DurationType(StringType):
    """Duration type.

    Example: `P3D`
    """

    string_format = "duration"


class EmailType(StringType):
    """Email type."""

    string_format = "email"


class HostnameType(StringType):
    """Hostname type."""

    string_format = "hostname"


class IPv4Type(StringType):
    """IPv4 address type."""

    string_format = "ipv4"


class IPv6Type(StringType):
    """IPv6 type."""

    string_format = "ipv6"


class UUIDType(StringType):
    """UUID type.

    Example: `3e4666bf-d5e5-4aa7-b8ce-cefe41c7568a`
    """

    string_format = "uuid"


class URIType(StringType):
    """URI type."""

    string_format = "uri"


class URIReferenceType(StringType):
    """URIReference type."""

    string_format = "uri-reference"


class URITemplateType(StringType):
    """URITemplate type."""

    string_format = "uri-template"


class JSONPointerType(StringType):
    """JSONPointer type."""

    string_format = "json-pointer"


class RelativeJSONPointerType(StringType):
    """RelativeJSONPointer type."""

    string_format = "relative-json-pointer"


class RegexType(StringType):
    """Regex type."""

    string_format = "regex"


class BooleanType(JSONTypeHelper):
    """Boolean type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": ["boolean"]}


class IntegerType(JSONTypeHelper):
    """Integer type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": ["integer"]}


class NumberType(JSONTypeHelper):
    """Number type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": ["number"]}


W = TypeVar("W", bound=JSONTypeHelper)


class ArrayType(JSONTypeHelper, Generic[W]):
    """Array type."""

    def __init__(self, wrapped_type: W | type[W]) -> None:
        """Initialize Array type with wrapped inner type.

        Args:
            wrapped_type: JSON Schema item type inside the array.
        """
        self.wrapped_type = wrapped_type

    @property
    def type_dict(self) -> dict:  # type: ignore  # OK: @classproperty vs @property
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": "array", "items": self.wrapped_type.type_dict}


class Property(JSONTypeHelper, Generic[W]):
    """Generic Property. Should be nested within a `PropertiesList`."""

    def __init__(
        self,
        name: str,
        wrapped: W | type[W],
        required: bool = False,
        default: _JsonValue = None,
        description: str | None = None,
        secret: bool | None = False,
        allowed_values: list[Any] | None = None,
        examples: list[Any] | None = None,
    ) -> None:
        """Initialize Property object.

        Note: Properties containing secrets should be specified with `secret=True`.
        Doing so will add the annotation `writeOnly=True`, in accordance with JSON
        Schema Draft 7 and later, and `secret=True` as an additional hint to readers.

        More info: https://json-schema.org/draft-07/json-schema-release-notes.html

        Args:
            name: Property name.
            wrapped: JSON Schema type of the property.
            required: Whether this is a required property.
            default: Default value in the JSON Schema.
            description: Long-text property description.
            secret: True if this is a credential or other secret.
            allowed_values: A list of allowed value options, if only specific values
                are permitted. This will define the type as an 'enum'.
            examples: Optional. A list of one or more sample values. These may be
                displayed to the user as hints of the expected format of inputs.
        """
        self.name = name
        self.wrapped = wrapped
        self.optional = not required
        self.default = default
        self.description = description
        self.secret = secret
        self.allowed_values = allowed_values or None
        self.examples = examples or None

    @property
    def type_dict(self) -> dict:  # type: ignore  # OK: @classproperty vs @property
        """Get type dictionary.

        Returns:
            A dictionary describing the type.

        Raises:
            ValueError: If the type dict is not valid.
        """
        wrapped = self.wrapped

        if isinstance(wrapped, type) and not isinstance(wrapped.type_dict, Mapping):
            raise ValueError(
                f"Type dict for {wrapped} is not defined. "
                + "Try instantiating it with a nested type such as "
                + f"{wrapped.__name__}(StringType)."
            )

        return cast(dict, wrapped.type_dict)

    def to_dict(self) -> dict:
        """Return a dict mapping the property name to its definition.

        Returns:
            A JSON Schema dictionary describing the object.
        """
        type_dict = self.type_dict
        if self.optional:
            type_dict = append_type(type_dict, "null")
        if self.default is not None:
            type_dict.update({"default": self.default})
        if self.description:
            type_dict.update({"description": self.description})
        if self.secret:
            type_dict.update(
                {
                    JSONSCHEMA_ANNOTATION_SECRET: True,
                    JSONSCHEMA_ANNOTATION_WRITEONLY: True,
                }
            )
        if self.allowed_values:
            type_dict.update({"enum": self.allowed_values})
        if self.examples:
            type_dict.update({"examples": self.examples})
        return {self.name: type_dict}


class ObjectType(JSONTypeHelper):
    """Object type, which wraps one or more named properties."""

    def __init__(
        self,
        *properties: Property,
        additional_properties: W | type[W] | None = None,
    ) -> None:
        """Initialize ObjectType from its list of properties.

        Args:
            properties: Zero or more attributes for this JSON object.
            additional_properties: A schema to match against unnamed properties in
                this object.
        """
        self.wrapped: list[Property] = list(properties)
        self.additional_properties = additional_properties

    @property
    def type_dict(self) -> dict:  # type: ignore  # OK: @classproperty vs @property
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        merged_props = {}
        required = []
        for w in self.wrapped:
            merged_props.update(w.to_dict())
            if not w.optional:
                required.append(w.name)
        result = {"type": "object", "properties": merged_props}

        if required:
            result["required"] = required

        if self.additional_properties:
            result["additionalProperties"] = self.additional_properties.type_dict

        return result


class CustomType(JSONTypeHelper):
    """Accepts an arbitrary JSON Schema dictionary."""

    def __init__(self, jsonschema_type_dict: dict) -> None:
        """Initialize JSONTypeHelper by importing an existing JSON Schema type.

        Args:
            jsonschema_type_dict: TODO
        """
        self._jsonschema_type_dict = jsonschema_type_dict

    @property
    def type_dict(self) -> dict:  # type: ignore  # OK: @classproperty vs @property
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return self._jsonschema_type_dict


class PropertiesList(ObjectType):
    """Properties list. A convenience wrapper around the ObjectType class."""

    def items(self) -> list[tuple[str, Property]]:
        """Get wrapped properties.

        Returns:
            List of (name, property) tuples.
        """
        return [(p.name, p) for p in self.wrapped]

    def append(self, property: Property) -> None:
        """Append a property to the property list.

        Args:
            property: Property to add
        """
        self.wrapped.append(property)


def to_jsonschema_type(
    from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
) -> dict:
    """Return the JSON Schema dict that describes the sql type.

    Args:
        from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
            provided, it may be provided as a class or a specific object instance.

    Raises:
        ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

    Returns:
        A compatible JSON Schema type definition.
    """
    sqltype_lookup: dict[str, dict] = {
        # NOTE: This is an ordered mapping, with earlier mappings taking precedence.
        #       If the SQL-provided type contains the type name on the left, the mapping
        #       will return the respective singer type.
        "timestamp": DateTimeType.type_dict,
        "datetime": DateTimeType.type_dict,
        "date": DateType.type_dict,
        "int": IntegerType.type_dict,
        "number": NumberType.type_dict,
        "decimal": NumberType.type_dict,
        "double": NumberType.type_dict,
        "float": NumberType.type_dict,
        "string": StringType.type_dict,
        "text": StringType.type_dict,
        "char": StringType.type_dict,
        "bool": BooleanType.type_dict,
        "variant": StringType.type_dict,
    }
    if isinstance(from_type, str):
        type_name = from_type
    elif isinstance(from_type, sqlalchemy.types.TypeEngine):
        type_name = type(from_type).__name__
    elif isinstance(from_type, type) and issubclass(
        from_type, sqlalchemy.types.TypeEngine
    ):
        type_name = from_type.__name__
    else:
        raise ValueError("Expected `str` or a SQLAlchemy `TypeEngine` object or type.")

    # Look for the type name within the known SQL type names:
    for sqltype, jsonschema_type in sqltype_lookup.items():
        if sqltype.lower() in type_name.lower():
            return jsonschema_type

    return sqltype_lookup["string"]  # safe failover to str


def _jsonschema_type_check(jsonschema_type: dict, type_check: tuple[str]) -> bool:
    """Return True if the jsonschema_type supports the provided type.

    Args:
        jsonschema_type: The type dict.
        type_check: A tuple of type strings to look for.

    Returns:
        True if the schema suports the type.
    """
    if "type" in jsonschema_type:
        if isinstance(jsonschema_type["type"], (list, tuple)):
            for t in jsonschema_type["type"]:
                if t in type_check:
                    return True
        else:
            if jsonschema_type.get("type") in type_check:
                return True

    if any(t in type_check for t in jsonschema_type.get("anyOf", ())):
        return True

    return False


def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
    """Convert JSON Schema type to a SQL type.

    Args:
        jsonschema_type: The JSON Schema object.

    Returns:
        The SQL type.
    """
    if _jsonschema_type_check(jsonschema_type, ("string",)):
        datelike_type = get_datelike_property_type(jsonschema_type)
        if datelike_type:
            if datelike_type == "date-time":
                return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATETIME())
            if datelike_type in "time":
                return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TIME())
            if datelike_type == "date":
                return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATE())

        maxlength = jsonschema_type.get("maxLength")
        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR(maxlength))

    if _jsonschema_type_check(jsonschema_type, ("integer",)):
        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.INTEGER())
    if _jsonschema_type_check(jsonschema_type, ("number",)):
        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DECIMAL())
    if _jsonschema_type_check(jsonschema_type, ("boolean",)):
        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.BOOLEAN())

    if _jsonschema_type_check(jsonschema_type, ("object",)):
        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())

    if _jsonschema_type_check(jsonschema_type, ("array",)):
        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())

    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())
