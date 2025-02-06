"""Provides an object model for JSON Schema."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

from referencing import Registry
from referencing.jsonschema import DRAFT202012

if t.TYPE_CHECKING:
    from referencing._core import Resolver

META_KEYS = [
    "id",
    "schema",
]

# These are keys defined in the JSON Schema spec that do not themselves contain
# schemas (or lists of schemas)
STANDARD_KEYS = [
    "title",
    "description",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "maxLength",
    "minLength",
    "format",
    "type",
    "default",
    "required",
    "enum",
    "pattern",
    "contentMediaType",
    "contentEncoding",
    # These are NOT simple keys (they can contain schemas themselves). We could
    # consider adding extra handling to them.
    "additionalProperties",
    "anyOf",
    "patternProperties",
    "allOf",
    # JSON Schema extensions
    "x-sql-datatype",
]


@dataclass
class Schema:
    """Object model for JSON Schema.

    Tap and Target authors may find this to be more convenient than
    working directly with JSON Schema data structures.

    This is based on, and overwrites
    https://github.com/transferwise/pipelinewise-singer-python/blob/master/singer/schema.py.
    This is because we wanted to expand it with extra STANDARD_KEYS.
    """

    id: str | None = None
    schema: str | None = None

    type: str | list[str] | None = None
    default: t.Any | None = None
    properties: dict | None = None
    items: t.Any | None = None
    description: str | None = None
    minimum: float | None = None
    maximum: float | None = None
    exclusiveMinimum: float | None = None  # noqa: N815
    exclusiveMaximum: float | None = None  # noqa: N815
    multipleOf: float | None = None  # noqa: N815
    maxLength: int | None = None  # noqa: N815
    minLength: int | None = None  # noqa: N815
    anyOf: t.Any | None = None  # noqa: N815
    allOf: t.Any | None = None  # noqa: N815
    format: str | None = None
    additionalProperties: t.Any | None = None  # noqa: N815
    patternProperties: t.Any | None = None  # noqa: N815
    required: list[str] | None = None
    enum: list[t.Any] | None = None
    title: str | None = None
    pattern: str | None = None
    contentMediaType: str | None = None  # noqa: N815
    contentEncoding: str | None = None  # noqa: N815

    # JSON Schema extensions
    x_sql_datatype: str | None = None

    def to_dict(self) -> dict[str, t.Any]:
        """Return the raw JSON Schema as a (possibly nested) dict.

        Returns:
            The raw JSON Schema as a (possibly nested) dict.
        """
        result = {}

        if self.properties is not None:
            result["properties"] = {k: v.to_dict() for k, v in self.properties.items()}

        if self.items is not None:
            result["items"] = self.items.to_dict()

        for key in STANDARD_KEYS:
            attr = key.replace("-", "_")
            if (val := self.__dict__.get(attr)) is not None:
                result[key] = val

        for key in META_KEYS:
            attr = key.replace("-", "_")
            if (val := self.__dict__.get(attr)) is not None:
                result[f"${key}"] = val

        return result

    @classmethod
    def from_dict(
        cls: t.Type[Schema],  # noqa: UP006
        data: dict,
        **schema_defaults: t.Any,
    ) -> Schema:
        """Initialize a Schema object based on the JSON Schema structure.

        Args:
            data: The JSON Schema structure.
            schema_defaults: Default values for the schema.

        Returns:
            The initialized Schema object.

        Example:
            >>> data = {
            ...     "$id": "https://example.com/person.schema.json",
            ...     "$schema": "http://json-schema.org/draft/2020-12/schema",
            ...     "title": "Person",
            ...     "type": "object",
            ...     "properties": {
            ...         "firstName": {
            ...             "type": "string",
            ...             "description": "The person's first name.",
            ...         },
            ...         "lastName": {
            ...             "type": "string",
            ...             "description": "The person's last name.",
            ...         },
            ...         "age": {
            ...             "description": "Age in years which must be equal to or greater than zero.",
            ...             "type": "integer",
            ...             "minimum": 0,
            ...             "x-sql-datatype": "smallint",
            ...         },
            ...     },
            ...     "required": ["firstName", "lastName"],
            ... }
            >>> schema = Schema.from_dict(data)
            >>> schema.title
            'Person'
            >>> schema.properties["firstName"].description
            "The person's first name."
            >>> schema.properties["age"].minimum
            0
            >>> schema.properties["age"].x_sql_datatype
            'smallint'
            >>> schema.schema
            'http://json-schema.org/draft/2020-12/schema'
        """  # noqa: E501
        kwargs = schema_defaults.copy()
        properties = data.get("properties")
        items = data.get("items")

        if properties is not None:
            kwargs["properties"] = {
                k: cls.from_dict(v, **schema_defaults) for k, v in properties.items()
            }
        if items is not None:
            kwargs["items"] = cls.from_dict(items, **schema_defaults)

        for key in STANDARD_KEYS:
            attr = key.replace("-", "_")
            if key in data:
                kwargs[attr] = data[key]

        for key in META_KEYS:
            attr = key.replace("-", "_")
            if f"${key}" in data:
                kwargs[attr] = data[f"${key}"]

        return cls(**kwargs)


class _SchemaKey:
    ref = "$ref"
    items = "items"
    properties = "properties"
    pattern_properties = "patternProperties"
    any_of = "anyOf"
    all_of = "allOf"


def resolve_schema_references(
    schema: dict[str, t.Any],
    refs: dict[str, str] | None = None,
) -> dict:
    """Resolves and replaces json-schema $refs with the appropriate dict.

    Recursively walks the given schema dict, converting every instance of $ref in a
    'properties' structure with a resolved dict.

    This modifies the input schema and also returns it.

    Args:
        schema: The schema dict
        refs: A dict of <string, dict> which forms a store of referenced schemata.

    Returns:
        A schema dict with all $refs replaced with the appropriate dict.
    """
    refs = refs or {}
    registry: Registry = Registry()
    schema_resource = DRAFT202012.create_resource(schema)
    registry = registry.with_resource("", schema_resource)
    registry = registry.with_resources(
        [(k, DRAFT202012.create_resource(v)) for k, v in refs.items()]
    )

    resolver = registry.resolver()
    return _resolve_schema_references(schema, resolver)


def _resolve_schema_references(  # noqa: C901
    schema: dict[str, t.Any],
    resolver: Resolver,
) -> dict[str, t.Any]:
    if _SchemaKey.ref in schema:
        reference_path = schema.pop(_SchemaKey.ref, None)
        resolved = resolver.lookup(reference_path)
        schema.update(resolved.contents)
        return _resolve_schema_references(schema, resolver)

    if _SchemaKey.properties in schema:
        for k, val in schema[_SchemaKey.properties].items():
            schema[_SchemaKey.properties][k] = _resolve_schema_references(val, resolver)

    if _SchemaKey.pattern_properties in schema:
        for k, val in schema[_SchemaKey.pattern_properties].items():
            schema[_SchemaKey.pattern_properties][k] = _resolve_schema_references(
                val,
                resolver,
            )

    if _SchemaKey.items in schema:
        schema[_SchemaKey.items] = _resolve_schema_references(
            schema[_SchemaKey.items],
            resolver,
        )

    if _SchemaKey.any_of in schema:
        for i, element in enumerate(schema[_SchemaKey.any_of]):
            schema[_SchemaKey.any_of][i] = _resolve_schema_references(element, resolver)

    if _SchemaKey.all_of in schema:
        for i, element in enumerate(schema[_SchemaKey.all_of]):
            schema[_SchemaKey.all_of][i] = _resolve_schema_references(element, resolver)

    return schema
