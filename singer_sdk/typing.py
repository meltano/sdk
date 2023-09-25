"""Classes and functions to streamline JSONSchema typing.

Usage example:
--------------
.. code-block:: python

    jsonschema = PropertiesList(
        Property("username", StringType, required=True),
        Property("password", StringType, required=True, secret=True),

        Property("id", IntegerType, required=True),
        Property("foo_or_bar", StringType, allowed_values=["foo", "bar"]),
        Property(
            "permissions",
            ArrayType(
                StringType(
                    allowed_values=["create", "delete", "insert", "update"],
                    examples=["insert", "update"],
                ),
            ),
        ),
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

import json
import typing as t

import sqlalchemy
from jsonschema import ValidationError, validators

if t.TYPE_CHECKING:
    from jsonschema.protocols import Validator

from singer_sdk.helpers._typing import (
    JSONSCHEMA_ANNOTATION_SECRET,
    JSONSCHEMA_ANNOTATION_WRITEONLY,
    append_type,
    get_datelike_property_type,
)

if t.TYPE_CHECKING:
    import sys

    if sys.version_info >= (3, 10):
        from typing import TypeAlias  # noqa: ICN003
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

_JsonValue: TypeAlias = t.Union[
    str,
    int,
    float,
    bool,
    list,
    dict,
    None,
]

T = t.TypeVar("T", bound=_JsonValue)
P = t.TypeVar("P")


def extend_validator_with_defaults(validator_class):  # noqa: ANN001, ANN201
    """Fill in defaults, before validating with the provided JSON Schema Validator.

    See
    https://python-jsonschema.readthedocs.io/en/latest/faq/#why-doesn-t-my-schema-s-default-property-set-the-default-on-my-instance
    for details.

    Args:
        validator_class: The JSON Schema Validator class to extend.

    Returns:
        The extended JSON Schema Validator class.
    """
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(
        validator: Validator,
        properties: t.Mapping[str, dict],
        instance: t.MutableMapping[str, t.Any],
        schema: dict,
    ) -> t.Generator[ValidationError, None, None]:
        for prop, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(prop, subschema["default"])

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


class DefaultInstanceProperty:
    """Property of default instance.

    Descriptor similar to ``property`` that decorates an instance method to retrieve
    a property from the instance initialized with default parameters, if the called on
    the class.
    """

    def __init__(self, fget: t.Callable) -> None:
        """Initialize the decorator.

        Args:
            fget: The function to decorate.
        """
        self.fget = fget

    def __get__(self, instance: P, owner: type[P]) -> t.Any:  # noqa: ANN401
        """Get the property value.

        Args:
            instance: The instance to get the property value from.
            owner: The class to get the property value from.

        Returns:
            The property value.
        """
        if instance is None:
            instance = owner()
        return self.fget(instance)


class JSONTypeHelper(t.Generic[T]):
    """Type helper base class for JSONSchema types."""

    def __init__(
        self,
        *,
        allowed_values: list[T] | None = None,
        examples: list[T] | None = None,
    ) -> None:
        """Initialize the type helper.

        Args:
            allowed_values: A list of allowed values.
            examples: A list of example values.
        """
        self.allowed_values = allowed_values
        self.examples = examples

    @DefaultInstanceProperty
    def type_dict(self) -> dict:
        """Return dict describing the type.

        Raises:
            NotImplementedError: If the derived class does not override this method.
        """
        raise NotImplementedError

    @property
    def extras(self) -> dict:
        """Return dict describing the JSON Schema extras.

        Returns:
            A dictionary containing the JSON Schema extras.
        """
        result = {}
        if self.allowed_values:
            result["enum"] = self.allowed_values

        if self.examples:
            result["examples"] = self.examples

        return result

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            A JSON Schema dictionary describing the object.
        """
        return self.type_dict  # type: ignore[no-any-return]

    def to_json(self, **kwargs: t.Any) -> str:
        """Convert to JSON.

        Args:
            kwargs: Additional keyword arguments to pass to json.dumps().

        Returns:
            A JSON string describing the object.
        """
        return json.dumps(self.to_dict(), **kwargs)


class StringType(JSONTypeHelper[str]):
    """String type.

    Examples:
        >>> StringType.type_dict
        {'type': ['string']}
        >>> StringType().type_dict
        {'type': ['string']}
        >>> StringType(allowed_values=["a", "b"]).type_dict
        {'type': ['string'], 'enum': ['a', 'b']}
    """

    string_format: str | None = None
    """String format.

    See the `formats built into the JSON Schema specification`_.

    Returns:
        A string describing the format.

    .. _`formats built into the JSON Schema specification`:
        https://json-schema.org/understanding-json-schema/reference/string.html#built-in-formats
    """

    @property
    def _format(self) -> dict:
        return {"format": self.string_format} if self.string_format else {}

    @DefaultInstanceProperty
    def type_dict(self) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {
            "type": ["string"],
            **self._format,
            **self.extras,
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


class BooleanType(JSONTypeHelper[bool]):
    """Boolean type.

    Examples:
        >>> BooleanType.type_dict
        {'type': ['boolean']}
        >>> BooleanType().type_dict
        {'type': ['boolean']}
    """

    @DefaultInstanceProperty
    def type_dict(self) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": ["boolean"], **self.extras}


class IntegerType(JSONTypeHelper):
    """Integer type.

    Examples:
        >>> IntegerType.type_dict
        {'type': ['integer']}
        >>> IntegerType().type_dict
        {'type': ['integer']}
        >>> IntegerType(allowed_values=[1, 2]).type_dict
        {'type': ['integer'], 'enum': [1, 2]}
    """

    @DefaultInstanceProperty
    def type_dict(self) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": ["integer"], **self.extras}


class NumberType(JSONTypeHelper[float]):
    """Number type.

    Examples:
        >>> NumberType.type_dict
        {'type': ['number']}
        >>> NumberType().type_dict
        {'type': ['number']}
        >>> NumberType(allowed_values=[1.0, 2.0]).type_dict
        {'type': ['number'], 'enum': [1.0, 2.0]}
    """

    @DefaultInstanceProperty
    def type_dict(self) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": ["number"], **self.extras}


W = t.TypeVar("W", bound=JSONTypeHelper)


class ArrayType(JSONTypeHelper[list], t.Generic[W]):
    """Array type."""

    def __init__(self, wrapped_type: W | type[W], **kwargs: t.Any) -> None:
        """Initialize Array type with wrapped inner type.

        Args:
            wrapped_type: JSON Schema item type inside the array.
            **kwargs: Additional keyword arguments to pass to the parent class.
        """
        self.wrapped_type = wrapped_type
        super().__init__(**kwargs)

    @property
    def type_dict(self) -> dict:  # type: ignore[override]
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": "array", "items": self.wrapped_type.type_dict, **self.extras}


class AnyType(JSONTypeHelper):
    """Any type."""

    def __init__(
        self,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(*args, **kwargs)

    @DefaultInstanceProperty
    def type_dict(self) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {**self.extras}


class Property(JSONTypeHelper[T], t.Generic[T]):
    """Generic Property. Should be nested within a `PropertiesList`."""

    # TODO: Make some of these arguments keyword-only. This is a breaking change.
    def __init__(
        self,
        name: str,
        wrapped: JSONTypeHelper[T] | type[JSONTypeHelper[T]],
        required: bool = False,  # noqa: FBT001, FBT002
        default: T | None = None,
        description: str | None = None,
        secret: bool | None = False,  # noqa: FBT002
        allowed_values: list[T] | None = None,
        examples: list[T] | None = None,
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
    def type_dict(self) -> dict:  # type: ignore[override]
        """Get type dictionary.

        Returns:
            A dictionary describing the type.

        Raises:
            ValueError: If the type dict is not valid.
        """
        wrapped = self.wrapped

        if isinstance(wrapped, type) and not isinstance(wrapped.type_dict, t.Mapping):
            msg = (
                f"Type dict for {wrapped} is not defined. Try instantiating it with a "
                f"nested type such as {wrapped.__name__}(StringType)."
            )
            raise ValueError(msg)

        return t.cast(dict, wrapped.type_dict)

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
                },
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
        additional_properties: W | type[W] | bool | None = None,
        pattern_properties: t.Mapping[str, W | type[W]] | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Initialize ObjectType from its list of properties.

        Args:
            properties: Zero or more attributes for this JSON object.
            additional_properties: A schema to match against unnamed properties in
                this object, or a boolean indicating if extra properties are allowed.
            pattern_properties: A dictionary of regex patterns to match against
                property names, and the schema to match against the values.
            **kwargs: Additional keyword arguments to pass to the `JSONTypeHelper`.

        Examples:
            >>> t = ObjectType(
            ...     Property("name", StringType, required=True),
            ...     Property("age", IntegerType),
            ...     Property("height", NumberType),
            ...     additional_properties=False,
            ... )
            >>> print(t.to_json(indent=2))
            {
              "type": "object",
              "properties": {
                "name": {
                  "type": [
                    "string"
                  ]
                },
                "age": {
                  "type": [
                    "integer",
                    "null"
                  ]
                },
                "height": {
                  "type": [
                    "number",
                    "null"
                  ]
                }
              },
              "required": [
                "name"
              ],
              "additionalProperties": false
            }
            >>> t = ObjectType(
            ...     Property("name", StringType, required=True),
            ...     Property("age", IntegerType),
            ...     Property("height", NumberType),
            ...     additional_properties=StringType,
            ... )
            >>> print(t.to_json(indent=2))
            {
              "type": "object",
              "properties": {
                "name": {
                  "type": [
                    "string"
                  ]
                },
                "age": {
                  "type": [
                    "integer",
                    "null"
                  ]
                },
                "height": {
                  "type": [
                    "number",
                    "null"
                  ]
                }
              },
              "required": [
                "name"
              ],
              "additionalProperties": {
                "type": [
                  "string"
                ]
              }
            }
        """
        self.wrapped: dict[str, Property] = {prop.name: prop for prop in properties}
        self.additional_properties = additional_properties
        self.pattern_properties = pattern_properties
        super().__init__(**kwargs)

    @property
    def type_dict(self) -> dict:  # type: ignore[override]
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        merged_props = {}
        required = []
        for w in self.wrapped.values():
            merged_props.update(w.to_dict())
            if not w.optional:
                required.append(w.name)
        result: dict[str, t.Any] = {"type": "object", "properties": merged_props}

        if required:
            result["required"] = required

        if self.additional_properties is not None:
            if isinstance(self.additional_properties, bool):
                result["additionalProperties"] = self.additional_properties
            else:
                result["additionalProperties"] = self.additional_properties.type_dict

        if self.pattern_properties:
            result["patternProperties"] = {
                k: v.type_dict for k, v in self.pattern_properties.items()
            }

        return result


class OneOf(JSONPointerType):
    """OneOf type.

    This type allows for a value to be one of a set of types.

    Examples:
        >>> t = OneOf(StringType, IntegerType)
        >>> print(t.to_json(indent=2))
        {
            "oneOf": [
                {
                    "type": [
                        "string"
                    ]
                },
                {
                    "type": [
                        "integer"
                    ]
                }
            ]
        }
    """

    def __init__(self, *types: W | type[W]) -> None:
        """Initialize OneOf type.

        Args:
            types: Types to choose from.
        """
        self.wrapped = types

    @property
    def type_dict(self) -> dict:  # type: ignore[override]
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"oneOf": [t.type_dict for t in self.wrapped]}


class Constant(JSONTypeHelper):
    """A constant property.

    A property that is always the same value.

    Examples:
        >>> t = Constant("foo")
        >>> print(t.to_json(indent=2))
        {
            "const": "foo"
        }
    """

    def __init__(self, value: _JsonValue) -> None:
        """Initialize Constant.

        Args:
            value: Value of the constant.
        """
        self.value = value

    @property
    def type_dict(self) -> dict:  # type: ignore[override]
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"const": self.value}


class DiscriminatedUnion(OneOf):
    """A discriminator property.

    This is a special case of :class:`singer_sdk.typing.OneOf`, where values are
    JSON objects, and the type of the object is determined by a property in the
    object.

    The property is a :class:`singer_sdk.typing.Constant` called the discriminator
    property.
    """

    def __init__(self, key: str, **options: ObjectType) -> None:
        """Initialize a discriminated union type.

        Args:
            key: Name of the discriminator property.
            options: Mapping of discriminator values to object types.

        Examples:
            >>> t = DiscriminatedUnion("species", cat=ObjectType(), dog=ObjectType())
            >>> print(t.to_json(indent=2))
            {
              "oneOf": [
                {
                  "type": "object",
                  "properties": {
                    "species": {
                      "const": "cat",
                      "description": "Discriminator for object of type 'cat'."
                    }
                  },
                  "required": [
                    "species"
                  ]
                },
                {
                  "type": "object",
                  "properties": {
                    "species": {
                        "const": "dog",
                        "description": "Discriminator for object of type 'dog'."
                    }
                  },
                  "required": [
                    "species"
                  ]
                }
              ]
            }
        """
        self.key = key
        self.options = options

        super().__init__(
            *(
                ObjectType(
                    Property(
                        key,
                        Constant(k),
                        required=True,
                        description=f"Discriminator for object of type '{k}'.",
                    ),
                    *v.wrapped.values(),
                    additional_properties=v.additional_properties,
                    pattern_properties=v.pattern_properties,
                )
                for k, v in options.items()
            ),
        )


class CustomType(JSONTypeHelper):
    """Accepts an arbitrary JSON Schema dictionary."""

    def __init__(self, jsonschema_type_dict: dict) -> None:
        """Initialize JSONTypeHelper by importing an existing JSON Schema type.

        Args:
            jsonschema_type_dict: TODO
        """
        self._jsonschema_type_dict = jsonschema_type_dict

    @property
    def type_dict(self) -> dict:  # type: ignore[override]
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return self._jsonschema_type_dict


class PropertiesList(ObjectType):
    """Properties list. A convenience wrapper around the ObjectType class."""

    def items(self) -> t.ItemsView[str, Property]:
        """Get wrapped properties.

        Returns:
            List of (name, property) tuples.
        """
        return self.wrapped.items()

    def append(self, property: Property) -> None:  # noqa: A002
        """Append a property to the property list.

        Args:
            property: Property to add
        """
        self.wrapped[property.name] = property


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
        from_type,
        sqlalchemy.types.TypeEngine,
    ):
        type_name = from_type.__name__
    else:
        msg = "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
        raise ValueError(msg)

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
            for schema_type in jsonschema_type["type"]:
                if schema_type in type_check:
                    return True
        elif jsonschema_type.get("type") in type_check:
            return True

    if any(
        _jsonschema_type_check(t, type_check) for t in jsonschema_type.get("anyOf", ())
    ):
        return True

    return False


def to_sql_type(  # noqa: PLR0911, C901
    jsonschema_type: dict,
) -> sqlalchemy.types.TypeEngine:
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
                return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATETIME())
            if datelike_type in "time":
                return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TIME())
            if datelike_type == "date":
                return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATE())

        maxlength = jsonschema_type.get("maxLength")
        return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR(maxlength))

    if _jsonschema_type_check(jsonschema_type, ("integer",)):
        return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.INTEGER())
    if _jsonschema_type_check(jsonschema_type, ("number",)):
        return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DECIMAL())
    if _jsonschema_type_check(jsonschema_type, ("boolean",)):
        return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.BOOLEAN())

    if _jsonschema_type_check(jsonschema_type, ("object",)):
        return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())

    if _jsonschema_type_check(jsonschema_type, ("array",)):
        return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())

    return t.cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())
