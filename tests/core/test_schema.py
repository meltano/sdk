"""
Testing that Schema can convert schemas lossless from and to dicts.

Schemas are taken from these examples;
https://json-schema.org/learn/miscellaneous-examples.html

NOTE: The following properties are not currently supported;
pattern
unevaluatedProperties
propertyNames
minProperties
maxProperties
prefixItems
contains
minContains
maxContains
minItems
maxItems
uniqueItems
enum
const
contentMediaType
contentEncoding
allOf
oneOf
not

Some of these could be trivially added (if they are SIMPLE_PROPERTIES.
Some might need more thinking if they can contain schemas (though, note that we also
treat 'additionalProperties', 'anyOf' and' patternProperties' as SIMPLE even though they
can contain schemas.
"""

from __future__ import annotations

from singer_sdk._singerlib import Schema


def test_simple_schema():
    simple_schema = {
        "title": "Longitude and Latitude Values",
        "description": "A geographical coordinate.",
        "required": ["latitude", "longitude"],
        "type": "object",
        "properties": {
            "latitude": {"type": "number", "minimum": -90, "maximum": 90},
            "longitude": {"type": "number", "minimum": -180, "maximum": 180},
        },
    }

    schema_plus = Schema.from_dict(simple_schema)
    assert schema_plus.to_dict() == simple_schema
    assert schema_plus.required == ["latitude", "longitude"]
    assert isinstance(schema_plus.properties["latitude"], Schema)
    latitude = schema_plus.properties["latitude"]
    assert latitude.type == "number"


def test_schema_with_items():
    schema = {
        "description": "A representation of a person, company, organization, or place",
        "type": "object",
        "properties": {"fruits": {"type": "array", "items": {"type": "string"}}},
    }
    schema_plus = Schema.from_dict(schema)
    assert schema_plus.to_dict() == schema
    assert isinstance(schema_plus.properties["fruits"], Schema)
    fruits = schema_plus.properties["fruits"]
    assert isinstance(fruits.items, Schema)
    assert fruits.items.type == "string"
