from __future__ import annotations

import typing as t
from dataclasses import dataclass

import pytest
from jsonschema import Draft202012Validator

from singer_sdk.singerlib import Schema, resolve_schema_references

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


@dataclass
class _ResolutionArgs:
    schema: dict[str, t.Any]
    refs: dict[str, dict[str, t.Any]] | None = None

    # TODO: Make this KW_ONLY when Python 3.9 support is dropped
    normalize: bool = False


@pytest.mark.parametrize(
    "args,expected",
    [
        pytest.param(
            _ResolutionArgs(
                {
                    "type": "object",
                    "definitions": {"string_type": {"type": "string"}},
                    "properties": {"name": {"$ref": "#/definitions/string_type"}},
                },
            ),
            {
                "type": "object",
                "definitions": {"string_type": {"type": "string"}},
                "properties": {"name": {"type": "string"}},
            },
            id="resolve_schema_references",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "type": "object",
                    "properties": {
                        "name": {"$ref": "references.json#/definitions/string_type"},
                    },
                },
                {
                    "references.json": {
                        "definitions": {"string_type": {"type": "string"}}
                    }
                },
            ),
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
            id="resolve_schema_references_with_refs",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "type": "object",
                    "definitions": {"string_type": {"type": "string"}},
                    "patternProperties": {".+": {"$ref": "#/definitions/string_type"}},
                },
            ),
            {
                "type": "object",
                "definitions": {"string_type": {"type": "string"}},
                "patternProperties": {".+": {"type": "string"}},
            },
            id="resolve_schema_references_with_pattern_properties",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "type": "object",
                    "properties": {
                        "dogs": {
                            "type": "array",
                            "items": {"$ref": "doggie.json#/dogs"},
                        },
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
            ),
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
            _ResolutionArgs(
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
                {
                    "references.json": {
                        "definitions": {"string_type": {"type": "string"}}
                    }
                },
            ),
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
            _ResolutionArgs(
                {
                    "type": "object",
                    "properties": {
                        "name": {"$ref": "references.json#/definitions/string_type"},
                    },
                },
                {
                    "references.json": {
                        "definitions": {
                            "string_type": {"$ref": "second_reference.json"}
                        },
                    },
                    "second_reference.json": {"type": "string"},
                },
            ),
            {"type": "object", "properties": {"name": {"type": "string"}}},
            id="resolve_schema_indirect_references",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "type": "object",
                    "properties": {
                        "name": {
                            "$ref": "references.json#/definitions/string_type",
                            "still_here": "yep",
                        },
                    },
                },
                {
                    "references.json": {
                        "definitions": {"string_type": {"type": "string"}}
                    }
                },
            ),
            {
                "type": "object",
                "properties": {"name": {"type": "string", "still_here": "yep"}},
            },
            id="resolve_schema_preserves_existing_fields",
        ),
        pytest.param(
            _ResolutionArgs(
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
            ),
            {"anyOf": [{"type": "string"}, {"type": "integer"}]},
            id="resolve_schema_any_of",
        ),
        pytest.param(
            _ResolutionArgs(
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
            ),
            {"allOf": [{"type": "string"}, {"type": "integer"}]},
            id="resolve_schema_all_of",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "oneOf": [
                        {"$ref": "references.json#/definitions/first_type"},
                        {"$ref": "references.json#/definitions/second_type"},
                    ],
                    "title": "A Title",
                },
                {
                    "references.json": {
                        "definitions": {
                            "first_type": {"type": "string", "title": "First Type"},
                            "second_type": {"type": "integer", "title": "Second Type"},
                        }
                    },
                },
            ),
            {
                "oneOf": [
                    {"type": "string", "title": "First Type"},
                    {"type": "integer", "title": "Second Type"},
                ],
                "title": "A Title",
            },
            id="resolve_schema_one_of",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "type": "object",
                    "properties": {
                        "product_price": {
                            "oneOf": [
                                {"$ref": "components#/schemas/LegacyProductPrice"},
                                {"$ref": "components#/schemas/ProductPrice"},
                            ],
                            "title": "Product Price",
                        },
                    },
                },
                {
                    "components": {
                        "schemas": {
                            "LegacyProductPrice": {
                                "type": "object",
                                "properties": {"price": {"type": "number"}},
                                "title": "Legacy Product Price",
                            },
                            "ProductPrice": {
                                "oneOf": [
                                    {
                                        "$ref": "components#/schemas/ProductPriceFixed",
                                    },
                                    {
                                        "$ref": "components#/schemas/ProductPriceFree",
                                    },
                                ],
                            },
                            "ProductPriceFixed": {
                                "type": "object",
                                "properties": {"price": {"type": "number"}},
                                "title": "Product Price Fixed",
                            },
                            "ProductPriceFree": {
                                "type": "object",
                                "properties": {"price": {"type": "number", "const": 0}},
                                "title": "Product Price Free",
                            },
                        }
                    },
                },
            ),
            {
                "type": "object",
                "properties": {
                    "product_price": {
                        "oneOf": [
                            {
                                "type": "object",
                                "properties": {"price": {"type": "number"}},
                                "title": "Legacy Product Price",
                            },
                            {
                                "oneOf": [
                                    {
                                        "type": "object",
                                        "properties": {"price": {"type": "number"}},
                                        "title": "Product Price Fixed",
                                    },
                                    {
                                        "type": "object",
                                        "properties": {
                                            "price": {"type": "number", "const": 0}
                                        },
                                        "title": "Product Price Free",
                                    },
                                ],
                            },
                        ],
                        "title": "Product Price",
                    },
                },
            },
            id="resolve_nested_one_of",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "type": "object",
                    "properties": {
                        "filter": {
                            "$ref": "components#/schemas/Filter",
                        },
                    },
                },
                {
                    "components": {
                        "schemas": {
                            "Filter": {
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "title": "Name",
                                    },
                                    "clauses": {
                                        "type": "array",
                                        "items": {
                                            "$ref": "components#/schemas/Filter",
                                        },
                                        "title": "Clauses",
                                    },
                                },
                            },
                        },
                    },
                },
            ),
            {
                "type": "object",
                "properties": {
                    "filter": {
                        "properties": {
                            "name": {
                                "type": "string",
                                "title": "Name",
                            },
                            "clauses": {
                                "type": "array",
                                "items": {},
                                "title": "Clauses",
                            },
                        },
                    },
                },
            },
            id="resolve_schema_references_with_circular_references",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "not": {"$ref": "components#/schemas/Not"},
                },
                {
                    "components": {
                        "schemas": {
                            "Not": {"type": "string", "enum": ["a", "b"]},
                        },
                    },
                },
            ),
            {
                "not": {"type": "string", "enum": ["a", "b"]},
            },
            id="resolve_schema_with_not_keyword",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "type": "object",
                    "properties": {
                        "min_compute_units": {
                            "$ref": "components#/schemas/ComputeUnit"
                        },
                        "max_compute_units": {
                            "$ref": "components#/schemas/ComputeUnit"
                        },
                    },
                },
                {
                    "components": {
                        "schemas": {
                            "ComputeUnit": {"type": "number"},
                        },
                    },
                },
            ),
            {
                "type": "object",
                "properties": {
                    "min_compute_units": {"type": "number"},
                    "max_compute_units": {"type": "number"},
                },
            },
            id="resolve_schema_multiple_properties_with_same_reference",
        ),
        pytest.param(
            _ResolutionArgs(
                {
                    "properties": {
                        "extensions": {
                            "anyOf": [
                                {"$ref": "components#/schemas/Extension"},
                            ],
                        },
                        "merged": {
                            "allOf": [
                                {"$ref": "components#/schemas/Base"},
                            ],
                        },
                        "compute_units": {
                            "oneOf": [
                                {"$ref": "components#/schemas/ComputeUnit"},
                            ],
                        },
                    },
                },
                {
                    "components": {
                        "schemas": {
                            "Base": {"type": "object", "properties": {}},
                            "ComputeUnit": {"type": "number"},
                            "Extension": {"type": "string"},
                        },
                    },
                },
                normalize=True,
            ),
            {
                "properties": {
                    "extensions": {"type": "string"},
                    "merged": {"type": "object", "properties": {}},
                    "compute_units": {"type": "number"},
                },
            },
            id="resolve_schema_composition_with_single_element",
        ),
        pytest.param(
            _ResolutionArgs({"allOf": []}, normalize=True),
            {},
            id="resolve_schema_all_of_with_no_elements",
        ),
        pytest.param(
            _ResolutionArgs({"oneOf": []}, normalize=True),
            {},
            id="resolve_schema_one_of_with_no_elements",
        ),
        pytest.param(
            _ResolutionArgs({"anyOf": []}, normalize=True),
            {},
            id="resolve_schema_any_of_with_no_elements",
        ),
    ],
)
def test_resolve_schema_references(args: _ResolutionArgs, expected: dict[str, t.Any]):
    """Test resolving schema references."""
    assert (
        resolve_schema_references(
            args.schema,
            args.refs,
            normalize=args.normalize,
        )
        == expected
    )


@pytest.mark.parametrize(
    "schema,data,passes",
    [
        pytest.param(
            {"allOf": [{"type": "string"}, {"maxLength": 5}]},
            "short",
            True,
            id="all_of_passes",
        ),
        pytest.param(
            {"allOf": [{"type": "string"}, {"maxLength": 5}]},
            "too long",
            False,
            id="all_of_fails",
        ),
        pytest.param(
            {
                "anyOf": [
                    {"type": "string", "maxLength": 5},
                    {"type": "number", "minimum": 0},
                ]
            },
            "short",
            True,
            id="any_of_first_passes",
        ),
        pytest.param(
            {
                "anyOf": [
                    {"type": "string", "maxLength": 5},
                    {"type": "number", "minimum": 0},
                ]
            },
            "too long",
            False,
            id="any_of_first_fails",
        ),
        pytest.param(
            {
                "anyOf": [
                    {"type": "string", "maxLength": 5},
                    {"type": "number", "minimum": 0},
                ]
            },
            12,
            True,
            id="any_of_second_passes",
        ),
        pytest.param(
            {
                "anyOf": [
                    {"type": "string", "maxLength": 5},
                    {"type": "number", "minimum": 0},
                ]
            },
            -5,
            False,
            id="any_of_second_fails",  # TODO: Address this false negative
        ),
        pytest.param(
            {
                "oneOf": [
                    {"type": "number", "multipleOf": 5},
                    {"type": "number", "multipleOf": 3},
                ]
            },
            10,
            True,
            id="one_of_first_passes",
        ),
        pytest.param(
            {
                "oneOf": [
                    {"type": "number", "multipleOf": 5},
                    {"type": "number", "multipleOf": 3},
                ]
            },
            9,
            True,
            id="one_of_second_passes",
            marks=pytest.mark.xfail(reason="Not implemented", strict=True),
        ),
        pytest.param(
            {
                "oneOf": [
                    {"type": "number", "multipleOf": 5},
                    {"type": "number", "multipleOf": 3},
                ]
            },
            2,
            False,
            id="one_of_neither_fails",
        ),
        pytest.param(
            {
                "oneOf": [
                    {"type": "number", "multipleOf": 5},
                    {"type": "number", "multipleOf": 3},
                ]
            },
            15,
            False,
            id="one_of_both_fails",
            marks=pytest.mark.xfail(reason="Not implemented", strict=True),
        ),
    ],
)
def test_schema_normalization(schema: dict[str, t.Any], data: t.Any, passes: bool):
    """Test schema decomposition."""
    schema_validator = Draft202012Validator(schema)
    assert schema_validator.is_valid(data) is passes

    resolved = resolve_schema_references(schema, normalize=True)
    resolved_validator = Draft202012Validator(resolved)
    assert resolved_validator.is_valid(data) is passes
