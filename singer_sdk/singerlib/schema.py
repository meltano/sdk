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
    "deprecated",
    # These are NOT simple keys (they can contain schemas themselves). We could
    # consider adding extra handling to them.
    "additionalProperties",
    "anyOf",
    "patternProperties",
    "allOf",
    "oneOf",
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

    deprecated: bool | None = None
    oneOf: t.Any | None = None  # noqa: N815

    # TODO: Use dataclass.KW_ONLY when Python 3.9 support is dropped
    # _: KW_ONLY  # noqa: ERA001

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
            ...         "deprecatedField": {
            ...             "type": "string",
            ...             "deprecated": True,
            ...         },
            ...         "price": {
            ...             "oneOf": [
            ...                 {"type": "number", "deprecated": True},
            ...                 {"type": "null"},
            ...             ],
            ...         },
            ...     },
            ...     "required": ["firstName", "lastName"],
            ... }
            >>> schema = Schema.from_dict(data)
            >>> schema.schema
            'http://json-schema.org/draft/2020-12/schema'
            >>> schema.title
            'Person'
            >>> schema.properties["firstName"].description
            "The person's first name."
            >>> schema.properties["age"].minimum
            0
            >>> schema.properties["age"].x_sql_datatype
            'smallint'
            >>> schema.properties["deprecatedField"].deprecated
            True
            >>> schema.properties["price"].oneOf[0]["deprecated"]
            True
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
    one_of = "oneOf"


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


def _resolve_schema_references(  # noqa: C901, PLR0912
    schema: dict[str, t.Any],
    resolver: Resolver,
    visited_refs: set[str] | None = None,
) -> dict[str, t.Any]:
    """Recursively resolve schema references while handling circular references.

    Args:
        schema: The schema dict to resolve references in
        resolver: The JSON Schema resolver to use
        visited_refs: Set of already visited reference paths to prevent infinite
            recursion

    Returns:
        The schema with all references resolved
    """
    if visited_refs is None:
        visited_refs = set()

    if _SchemaKey.ref in schema:
        reference_path = schema.pop(_SchemaKey.ref, None)
        if reference_path in visited_refs:
            # We've already seen this reference, return the schema as-is
            # to prevent infinite recursion
            return schema

        visited_refs.add(reference_path)
        resolved = resolver.lookup(reference_path)
        schema.update(resolved.contents)
        return _resolve_schema_references(schema, resolver, visited_refs)

    if _SchemaKey.properties in schema:
        for k, val in schema[_SchemaKey.properties].items():
            schema[_SchemaKey.properties][k] = _resolve_schema_references(
                val,
                resolver,
                visited_refs,
            )

    if _SchemaKey.pattern_properties in schema:
        for k, val in schema[_SchemaKey.pattern_properties].items():
            schema[_SchemaKey.pattern_properties][k] = _resolve_schema_references(
                val,
                resolver,
                visited_refs,
            )

    if _SchemaKey.items in schema:
        schema[_SchemaKey.items] = _resolve_schema_references(
            schema[_SchemaKey.items],
            resolver,
            visited_refs,
        )

    if _SchemaKey.any_of in schema:
        for i, element in enumerate(schema[_SchemaKey.any_of]):
            schema[_SchemaKey.any_of][i] = _resolve_schema_references(
                element,
                resolver,
                visited_refs,
            )

    if _SchemaKey.all_of in schema:
        for i, element in enumerate(schema[_SchemaKey.all_of]):
            schema[_SchemaKey.all_of][i] = _resolve_schema_references(
                element,
                resolver,
                visited_refs,
            )

    if _SchemaKey.one_of in schema:
        for i, element in enumerate(schema[_SchemaKey.one_of]):
            schema[_SchemaKey.one_of][i] = _resolve_schema_references(
                element,
                resolver,
                visited_refs,
            )

    return schema
