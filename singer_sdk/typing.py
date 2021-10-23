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
            ObjectType(
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
from typing import Any, Generic, List, Tuple, Type, TypeVar, Union, cast

from jsonschema import validators

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._typing import append_type


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


class DateTimeType(JSONTypeHelper):
    """DateTime type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {
            "type": ["string"],
            "format": "date-time",
        }


class StringType(JSONTypeHelper):
    """String type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return {"type": ["string"]}


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

    def __init__(self, wrapped_type: W) -> None:
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
        wrapped: Union[W, Type[W]],
        required: bool = False,
        default: Any = None,
        description: str = None,
    ) -> None:
        """Initialize Property object.

        Args:
            name: Property name.
            wrapped: JSON Schema type of the property.
            required: Whether this is a required property.
            default: Default value in the JSON Schema.
            description: Long-text property description.
        """
        self.name = name
        self.wrapped = wrapped
        self.optional = not required
        self.default = default
        self.description = description

    @property
    def type_dict(self) -> dict:  # type: ignore  # OK: @classproperty vs @property
        """Get type dictionary.

        Returns:
            A dictionary describing the type.
        """
        return cast(dict, self.wrapped.type_dict)

    def to_dict(self) -> dict:
        """Return a dict mapping the property name to its definition.

        Returns:
            A JSON Schema dictionary describing the object.
        """
        type_dict = self.type_dict
        if self.optional:
            type_dict = append_type(type_dict, "null")
        if self.default:
            type_dict.update({"default": self.default})
        if self.description:
            type_dict.update({"description": self.description})
        return {self.name: type_dict}


class ObjectType(JSONTypeHelper):
    """Object type, which wraps one or more named properties."""

    def __init__(self, *properties: Property) -> None:
        """Initialize ObjectType from its list of properties.

        Args:
            properties: TODO
        """
        self.wrapped: List[Property] = list(properties)

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

    def items(self) -> List[Tuple[str, Property]]:
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
