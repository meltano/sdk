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
    not_ = "not"


def resolve_schema_references(
    schema: dict[str, t.Any],
    refs: dict[str, str] | None = None,
    *,
    normalize: bool = False,
) -> dict:
    """Resolves and replaces json-schema $refs with the appropriate dict.

    Recursively walks the given schema dict, converting every instance of $ref in a
    'properties' structure with a resolved dict.

    This modifies the input schema and also returns it.

    Args:
        schema: The schema dict
        refs: A dict of <string, dict> which forms a store of referenced schemata.
        normalize: Whether to normalize the schema by flattening combined schemas
            (oneOf, allOf, etc.) into the most appropriate flat schema.

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
    return _resolve_schema_references(schema, resolver, normalize=normalize)


def _resolve_schema_references(  # noqa: C901, PLR0912
    schema: dict[str, t.Any],
    resolver: Resolver,
    *,
    visited_refs: tuple[str, ...] | None = None,
    normalize: bool = False,
) -> dict[str, t.Any]:
    """Recursively resolve schema references while handling circular references.

    Args:
        schema: The schema dict to resolve references in
        resolver: The JSON Schema resolver to use
        visited_refs: Tuple of already visited reference paths in the current call stack
            to prevent infinite recursion
        normalize: Whether to normalize the schema by flattening combined schemas
            (oneOf, allOf, etc.) into the most appropriate flat schema in a best-effort
            manner.

    Returns:
        The schema with all references resolved
    """
    if visited_refs is None:
        visited_refs = ()

    if _SchemaKey.ref in schema:
        reference_path = schema.pop(_SchemaKey.ref, None)
        if reference_path in visited_refs:
            # We've already seen this reference in the current call stack, return
            # the schema as-is to prevent infinite recursion
            return schema

        # Add this reference to the current call stack
        new_visited_refs = (*visited_refs, reference_path)
        resolved = resolver.lookup(reference_path)
        schema.update(resolved.contents)
        return _resolve_schema_references(
            schema,
            resolver,
            visited_refs=new_visited_refs,
            normalize=normalize,
        )

    if _SchemaKey.properties in schema:
        for k, val in schema[_SchemaKey.properties].items():
            schema[_SchemaKey.properties][k] = _resolve_schema_references(
                val,
                resolver,
                visited_refs=visited_refs,
                normalize=normalize,
            )

    if _SchemaKey.pattern_properties in schema:
        for k, val in schema[_SchemaKey.pattern_properties].items():
            schema[_SchemaKey.pattern_properties][k] = _resolve_schema_references(
                val,
                resolver,
                visited_refs=visited_refs,
                normalize=normalize,
            )

    if _SchemaKey.items in schema:
        schema[_SchemaKey.items] = _resolve_schema_references(
            schema[_SchemaKey.items],
            resolver,
            visited_refs=visited_refs,
            normalize=normalize,
        )

    if _SchemaKey.any_of in schema:
        if normalize:
            return _normalize_any_of(
                schema[_SchemaKey.any_of],
                resolver,
                visited_refs=visited_refs,
            )

        for i, element in enumerate(schema[_SchemaKey.any_of]):
            schema[_SchemaKey.any_of][i] = _resolve_schema_references(
                element,
                resolver,
                visited_refs=visited_refs,
                normalize=normalize,
            )

    if _SchemaKey.all_of in schema:
        if normalize:
            return _normalize_all_of(
                schema[_SchemaKey.all_of],
                resolver,
                visited_refs=visited_refs,
            )
        for i, element in enumerate(schema[_SchemaKey.all_of]):
            schema[_SchemaKey.all_of][i] = _resolve_schema_references(
                element,
                resolver,
                visited_refs=visited_refs,
                normalize=normalize,
            )

    if _SchemaKey.one_of in schema:
        if normalize:
            return _normalize_one_of(
                schema[_SchemaKey.one_of],
                resolver,
                visited_refs=visited_refs,
            )

        for i, element in enumerate(schema[_SchemaKey.one_of]):
            schema[_SchemaKey.one_of][i] = _resolve_schema_references(
                element,
                resolver,
                visited_refs=visited_refs,
                normalize=normalize,
            )

    if _SchemaKey.not_ in schema:
        schema[_SchemaKey.not_] = _resolve_schema_references(
            schema[_SchemaKey.not_],
            resolver,
            visited_refs=visited_refs,
            normalize=normalize,
        )

    return schema


def _normalize_all_of(
    subschemas: list[dict[str, t.Any]],
    resolver: Resolver,
    *,
    visited_refs: tuple[str, ...] | None = None,
) -> dict[str, t.Any]:
    # If the allOf array has no elements, return an empty schema.
    if len(subschemas) == 0:
        return {}

    # If the allOf array has only one element, just flatten it.
    if len(subschemas) == 1:
        return _resolve_schema_references(
            subschemas[0],
            resolver,
            visited_refs=visited_refs,
            normalize=True,
        )

    # TODO: merge subschemas:
    # - Combining all properties
    # - Taking the most restrictive constraints for each property
    # - Merging required arrays
    # - Handling type intersections carefully
    result: dict[str, t.Any] = {}
    for subschema in subschemas:
        result |= subschema

    return result


def _normalize_one_of(
    subschemas: list[dict[str, t.Any]],
    resolver: Resolver,
    *,
    visited_refs: tuple[str, ...] | None = None,
) -> dict[str, t.Any]:
    # If the oneOf array has no elements, return an empty schema.
    if len(subschemas) == 0:
        return {}

    # If the oneOf array has only one element, just flatten it.
    if len(subschemas) == 1:
        return _resolve_schema_references(
            subschemas[0],
            resolver,
            visited_refs=visited_refs,
            normalize=True,
        )

    # TODO: Resolve this.
    # This is the most complex and often cannot be safely flattened without losing the
    # exclusive constraint. You might need to preserve some form of conditional logic.
    # Safe Resolution Strategies:
    # - Property Merging: Combine properties from multiple schemas, handling conflicts
    #   by taking the most restrictive valid combination.
    # - Constraint Intersection: For numeric constraints, take overlapping ranges. For
    #   string patterns, combine them logically.
    # - Type Resolution: Handle type unions and intersections appropriately.
    # - Validation Preservation: Ensure the resolved schema accepts exactly the same set
    #   of valid documents as the original.
    return subschemas[0]


def _normalize_any_of(
    subschemas: list[dict[str, t.Any]],
    resolver: Resolver,
    *,
    visited_refs: tuple[str, ...] | None = None,
) -> dict[str, t.Any]:
    # If the anyOf array has no elements, return an empty schema.
    if len(subschemas) == 0:
        return {}

    # If the anyOf array has only one element, just flatten it.
    if len(subschemas) == 1:
        return _resolve_schema_references(
            subschemas[0],
            resolver,
            visited_refs=visited_refs,
            normalize=True,
        )

    # TODO: resolve this by creating a union type or identify common properties and
    # make conflicting ones optional. However, complete resolution isn't always
    # possible without losing semantic meaning.
    result: dict[str, t.Any] = {}
    for subschema in subschemas:
        if type_ := subschema.get("type"):
            if isinstance(type_, str):
                type_ = [type_]
            merged_type = list(set(type_ + result.get("type", [])))
        else:
            merged_type = result.get("type", [])

        result |= subschema
        result["type"] = merged_type

    return result
