from __future__ import annotations

import pytest

from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._flattening import (
    flatten_key,
    flatten_record,
    flatten_schema,
    get_flattening_options,
)


@pytest.mark.parametrize(
    "flattened_schema, max_level, expected",
    [
        pytest.param(
            {
                "properties": {
                    "key_1": {"type": ["null", "integer"]},
                    "key_2__key_3": {"type": ["null", "string"]},
                    "key_2__key_4": {"type": ["null", "object"]},
                }
            },
            99,
            {
                "key_1": 1,
                "key_2__key_3": "value",
                "key_2__key_4": '{"key_5":1,"key_6":["a","b"]}',
            },
            id="flattened schema limiting the max level",
        ),
        pytest.param(
            {
                "properties": {
                    "key_1": {"type": ["null", "integer"]},
                    "key_2__key_3": {"type": ["null", "string"]},
                    "key_2__key_4__key_5": {"type": ["null", "integer"]},
                    "key_2__key_4__key_6": {"type": ["null", "array"]},
                }
            },
            99,
            {
                "key_1": 1,
                "key_2__key_3": "value",
                "key_2__key_4__key_5": 1,
                "key_2__key_4__key_6": '["a","b"]',
            },
            id="flattened schema not limiting the max level",
        ),
        pytest.param(
            {
                "properties": {
                    "key_1": {"type": ["null", "integer"]},
                    "key_2__key_3": {"type": ["null", "string"]},
                    "key_2__key_4__key_5": {"type": ["null", "integer"]},
                    "key_2__key_4__key_6": {"type": ["null", "array"]},
                }
            },
            1,
            {
                "key_1": 1,
                "key_2__key_3": "value",
                "key_2__key_4": '{"key_5":1,"key_6":["a","b"]}',
            },
            id="max level limiting flattened schema",
        ),
    ],
)
def test_flatten_record(flattened_schema, max_level, expected):
    """Test flatten_record to obey the max_level and flattened_schema parameters."""
    record = {
        "key_1": 1,
        "key_2": {"key_3": "value", "key_4": {"key_5": 1, "key_6": ["a", "b"]}},
    }

    result = flatten_record(
        record, max_level=max_level, flattened_schema=flattened_schema
    )
    assert expected == result


def test_get_flattening_options_missing_max_depth():
    with pytest.raises(
        ConfigValidationError,
        match="Flattening is misconfigured",
    ) as exc:
        get_flattening_options({"flattening_enabled": True})

    assert isinstance(exc.value, ConfigValidationError)
    assert (
        exc.value.errors[0]
        == "flattening_max_depth is required when flattening is enabled"
    )


def test_get_flattening_options_max_key_length():
    options = get_flattening_options(
        {
            "flattening_enabled": True,
            "flattening_max_depth": 5,
            "flattening_max_key_length": 30,
        }
    )
    assert options.max_key_length == 30


def test_flatten_schema_with_typeless_properties():
    """Test that properties without explicit types are preserved during flattening.

    This test demonstrates issue #1886 where properties defined as empty objects
    (e.g., "PropertyName": {}) are dropped from the schema during flattening,
    causing validation failures when records contain these fields.
    """
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "changes": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "OldValue": {},  # Typeless property
                    "NewValue": {},  # Typeless property
                },
            },
        },
    }

    # Flatten with max_level=1 to flatten one level deep
    flattened = flatten_schema(schema, max_level=1)

    # The typeless properties should be preserved in the flattened schema
    # They should be converted to string type to hold JSON-serialized values
    assert "changes__field" in flattened["properties"]
    assert "changes__OldValue" in flattened["properties"], (
        "Typeless property 'OldValue' should be preserved in flattened schema"
    )
    assert "changes__NewValue" in flattened["properties"], (
        "Typeless property 'NewValue' should be preserved in flattened schema"
    )

    # These properties should be typed as string to allow JSON serialization
    assert "string" in flattened["properties"]["changes__OldValue"]["type"]
    assert "string" in flattened["properties"]["changes__NewValue"]["type"]


def test_flatten_record_with_typeless_property_values():
    """Test that records with typeless properties are flattened correctly.

    This test verifies that actual data in typeless properties is properly
    JSON-serialized when flattening records, addressing issue #1886.
    """
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "changes": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "OldValue": {},  # Typeless property
                    "NewValue": {},  # Typeless property
                },
            },
        },
    }

    # First, flatten the schema
    flattened_schema = flatten_schema(schema, max_level=1)

    # Create a record with various types of values in the typeless properties
    record = {
        "id": "123",
        "changes": {
            "field": "status",
            "OldValue": {"nested": "object", "with": ["array", "values"]},
            "NewValue": "simple string",
        },
    }

    # Flatten the record using the flattened schema
    flattened_record = flatten_record(
        record, flattened_schema=flattened_schema, max_level=1
    )

    # Verify the flattened record structure
    assert flattened_record["id"] == "123"
    assert flattened_record["changes__field"] == "status"

    # Typeless properties with complex values should be JSON-serialized
    assert "changes__OldValue" in flattened_record
    assert (
        flattened_record["changes__OldValue"]
        == '{"nested":"object","with":["array","values"]}'
    )

    # Typeless properties with simple values should be preserved
    assert "changes__NewValue" in flattened_record
    assert flattened_record["changes__NewValue"] == "simple string"


def test_flatten_key_with_long_names(subtests: pytest.Subtests):
    """Test that flatten_key abbreviates long key names to stay under 255 chars.

    This test exercises the while loop in flatten_key that reduces key length
    by abbreviating parent keys when the concatenated result exceeds 255 chars.
    """
    # Create a deeply nested structure with long key names that will exceed
    # 255 chars when concatenated with the default "__" separator
    long_key_name = "very_long_key_name_that_contains_many_characters"
    parent_keys = [
        "first_extremely_long_parent_key_name_with_many_words",
        "second_extremely_long_parent_key_name_with_many_words",
        "third_extremely_long_parent_key_name_with_many_words",
        "fourth_extremely_long_parent_key_name_with_many_words",
        "fifth_extremely_long_parent_key_name_with_many_words",
    ]

    with subtests.test("default parameters"):
        # Without abbreviation, this would be well over 255 characters
        # (each parent key is ~53 chars, plus the final key ~47 chars,
        # plus 5 separators = ~317 chars)
        result = flatten_key(long_key_name, parent_keys)

        # The result should be under 255 characters
        assert len(result) < 255

        # The result should still contain all the keys in some form (abbreviated)
        # The abbreviation logic removes lowercase letters from camelized versions
        # or truncates to 3 chars if that results in <= 1 char
        assert result.count("__") == len(parent_keys)  # All separators are present

    with subtests.test("custom separator"):
        # Test with a custom separator to ensure it works with different separators
        result_custom_sep = flatten_key(long_key_name, parent_keys, separator=".")
        assert len(result_custom_sep) < 255
        assert result_custom_sep.count(".") == len(parent_keys)

    with subtests.test("custom max_key_length"):
        result_custom_length = flatten_key(
            long_key_name,
            parent_keys,
            max_key_length=100,
        )
        assert len(result_custom_length) < 100
        assert result_custom_length.count("__") == len(parent_keys)
