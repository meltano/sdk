"""Provides an object model for JSON Schema."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

from jsonschema import RefResolver

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
    "required",
    "enum",
    # These are NOT simple keys (they can contain schemas themselves). We could
    # consider adding extra handling to them.
    "additionalProperties",
    "anyOf",
    "patternProperties",
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

    type: str | list[str] | None = None
    properties: dict | None = None
    items: t.Any | None = None
    description: str | None = None
    minimum: float | None = None
    maximum: float | None = None
    exclusiveMinimum: float | None = None
    exclusiveMaximum: float | None = None
    multipleOf: float | None = None
    maxLength: int | None = None
    minLength: int | None = None
    anyOf: t.Any | None = None
    format: str | None = None
    additionalProperties: t.Any | None = None
    patternProperties: t.Any | None = None
    required: list[str] | None = None
    enum: list[t.Any] | None = None
    title: str | None = None

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
            if self.__dict__.get(key) is not None:
                result[key] = self.__dict__[key]

        return result

    @classmethod
    def from_dict(cls: t.Type[Schema], data: dict, **schema_defaults: t.Any) -> Schema:
        """Initialize a Schema object based on the JSON Schema structure.

        Args:
            data: The JSON Schema structure.
            schema_defaults: Default values for the schema.

        Returns:
            The initialized Schema object.
        """
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
            if key in data:
                kwargs[key] = data[key]
        return cls(**kwargs)


class _SchemaKey:
    ref = "$ref"
    items = "items"
    properties = "properties"
    pattern_properties = "patternProperties"
    any_of = "anyOf"


def resolve_schema_references(
    schema: dict[str, t.Any],
    refs: dict[str, dict] | None = None,
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
    return _resolve_schema_references(schema, RefResolver("", schema, store=refs))


def _resolve_schema_references(
    schema: dict[str, t.Any],
    resolver: RefResolver,
) -> dict[str, t.Any]:
    if _SchemaKey.ref in schema:
        reference_path = schema.pop(_SchemaKey.ref, None)
        resolved = resolver.resolve(reference_path)[1]
        schema.update(resolved)
        return _resolve_schema_references(schema, resolver)

    if _SchemaKey.properties in schema:
        for k, val in schema[_SchemaKey.properties].items():
            schema[_SchemaKey.properties][k] = _resolve_schema_references(val, resolver)

    if _SchemaKey.pattern_properties in schema:
        for k, val in schema[_SchemaKey.pattern_properties].items():
            schema[_SchemaKey.pattern_properties][k] = _resolve_schema_references(
                val, resolver
            )

    if _SchemaKey.items in schema:
        schema[_SchemaKey.items] = _resolve_schema_references(
            schema[_SchemaKey.items], resolver
        )

    if _SchemaKey.any_of in schema:
        for i, element in enumerate(schema[_SchemaKey.any_of]):
            schema[_SchemaKey.any_of][i] = _resolve_schema_references(element, resolver)

    return schema
