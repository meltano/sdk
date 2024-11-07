from __future__ import annotations

import pytest

from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._flattening import flatten_record, get_flattening_options


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
        ConfigValidationError, match="Flattening is misconfigured"
    ) as exc:
        get_flattening_options({"flattening_enabled": True})

    assert (
        exc.value.errors[0]
        == "flattening_max_depth is required when flattening is enabled"
    )
