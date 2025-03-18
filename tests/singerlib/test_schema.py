from __future__ import annotations

import pytest

from singer_sdk._singerlib import Schema, resolve_schema_references

STRING_SCHEMA = Schema(type="string", maxLength=32, default="")
STRING_DICT = {"type": "string", "maxLength": 32, "default": ""}
INTEGER_SCHEMA = Schema(type="integer", maximum=1000000, default=0)
INTEGER_DICT = {"type": "integer", "maximum": 1000000, "default": 0}
ARRAY_SCHEMA = Schema(type="array", items=INTEGER_SCHEMA)
ARRAY_DICT = {"type": "array", "items": INTEGER_DICT}
OBJECT_SCHEMA = Schema(
    type="object",
    properties={
        "a_string": STRING_SCHEMA,
        "an_array": ARRAY_SCHEMA,
    },
    additionalProperties=True,
    required=["a_string"],
)
OBJECT_DICT = {
    "type": "object",
    "properties": {
        "a_string": STRING_DICT,
        "an_array": ARRAY_DICT,
    },
    "additionalProperties": True,
    "required": ["a_string"],
}


@pytest.mark.parametrize(
    "schema,expected",
    [
        pytest.param(
            STRING_SCHEMA,
            STRING_DICT,
            id="string_to_dict",
        ),
        pytest.param(
            INTEGER_SCHEMA,
            INTEGER_DICT,
            id="integer_to_dict",
        ),
        pytest.param(
            ARRAY_SCHEMA,
            ARRAY_DICT,
            id="array_to_dict",
        ),
        pytest.param(
            OBJECT_SCHEMA,
            OBJECT_DICT,
            id="object_to_dict",
        ),
    ],
)
def test_schema_to_dict(schema, expected):
    assert schema.to_dict() == expected


@pytest.mark.parametrize(
    "pydict,expected",
    [
        pytest.param(
            STRING_DICT,
            STRING_SCHEMA,
            id="schema_from_string_dict",
        ),
        pytest.param(
            INTEGER_DICT,
            INTEGER_SCHEMA,
            id="schema_from_integer_dict",
        ),
        pytest.param(
            ARRAY_DICT,
            ARRAY_SCHEMA,
            id="schema_from_array_dict",
        ),
        pytest.param(
            OBJECT_DICT,
            OBJECT_SCHEMA,
            id="schema_from_object_dict",
        ),
    ],
)
def test_schema_from_dict(pydict, expected):
    assert Schema.from_dict(pydict) == expected


@pytest.mark.parametrize(
    "schema,refs,expected",
    [
        pytest.param(
            {
                "type": "object",
                "definitions": {"string_type": {"type": "string"}},
                "properties": {"name": {"$ref": "#/definitions/string_type"}},
            },
            None,
            {
                "type": "object",
                "definitions": {"string_type": {"type": "string"}},
                "properties": {"name": {"type": "string"}},
            },
            id="resolve_schema_references",
        ),
        pytest.param(
            {
                "type": "object",
                "properties": {
                    "name": {"$ref": "references.json#/definitions/string_type"},
                },
            },
            {"references.json": {"definitions": {"string_type": {"type": "string"}}}},
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
            id="resolve_schema_references_with_refs",
        ),
        pytest.param(
            {
                "type": "object",
                "definitions": {"string_type": {"type": "string"}},
                "patternProperties": {".+": {"$ref": "#/definitions/string_type"}},
            },
            None,
            {
                "type": "object",
                "definitions": {"string_type": {"type": "string"}},
                "patternProperties": {".+": {"type": "string"}},
            },
            id="resolve_schema_references_with_pattern_properties",
        ),
        pytest.param(
            {
                "type": "object",
                "properties": {
                    "dogs": {"type": "array", "items": {"$ref": "doggie.json#/dogs"}},
                },
            },
            {
                "doggie.json": {
                    "dogs": {
                        "type": "object",
                        "properties": {
                            "breed": {"type": "string"},
                            "name": {"type": "string"},
                        },
                    },
                },
            },
            {
                "type": "object",
                "properties": {
                    "dogs": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "breed": {"type": "string"},
                                "name": {"type": "string"},
                            },
                        },
                    },
                },
            },
            id="resolve_schema_references_with_items",
        ),
        pytest.param(
            {
                "type": "object",
                "properties": {
                    "thing": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "$ref": "references.json#/definitions/string_type",
                            },
                        },
                    },
                },
            },
            {"references.json": {"definitions": {"string_type": {"type": "string"}}}},
            {
                "type": "object",
                "properties": {
                    "thing": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    },
                },
            },
            id="resolve_schema_nested_references",
        ),
        pytest.param(
            {
                "type": "object",
                "properties": {
                    "name": {"$ref": "references.json#/definitions/string_type"},
                },
            },
            {
                "references.json": {
                    "definitions": {"string_type": {"$ref": "second_reference.json"}},
                },
                "second_reference.json": {"type": "string"},
            },
            {"type": "object", "properties": {"name": {"type": "string"}}},
            id="resolve_schema_indirect_references",
        ),
        pytest.param(
            {
                "type": "object",
                "properties": {
                    "name": {
                        "$ref": "references.json#/definitions/string_type",
                        "still_here": "yep",
                    },
                },
            },
            {"references.json": {"definitions": {"string_type": {"type": "string"}}}},
            {
                "type": "object",
                "properties": {"name": {"type": "string", "still_here": "yep"}},
            },
            id="resolve_schema_preserves_existing_fields",
        ),
        pytest.param(
            {
                "anyOf": [
                    {"$ref": "references.json#/definitions/first_type"},
                    {"$ref": "references.json#/definitions/second_type"},
                ],
            },
            {
                "references.json": {
                    "definitions": {
                        "first_type": {"type": "string"},
                        "second_type": {"type": "integer"},
                    },
                },
            },
            {"anyOf": [{"type": "string"}, {"type": "integer"}]},
            id="resolve_schema_any_of",
        ),
        pytest.param(
            {
                "allOf": [
                    {"$ref": "references.json#/definitions/first_type"},
                    {"$ref": "references.json#/definitions/second_type"},
                ],
            },
            {
                "references.json": {
                    "definitions": {
                        "first_type": {"type": "string"},
                        "second_type": {"type": "integer"},
                    },
                },
            },
            {"allOf": [{"type": "string"}, {"type": "integer"}]},
            id="resolve_schema_all_of",
        ),
    ],
)
def test_resolve_schema_references(schema, refs, expected):
    """Test resolving schema references."""
    assert resolve_schema_references(schema, refs) == expected
