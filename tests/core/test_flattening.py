from __future__ import annotations

import pytest

from exceptions import InvalidFlatteningRecordsParameter
from singer_sdk.helpers._flattening import flatten_record


@pytest.mark.parametrize(
    "flattened_schema, max_level, expected, expected_exception",
    [
        pytest.param(
            {
                "properties": {
                    "key_1": {"type": ["null", "integer"]},
                    "key_2__key_3": {"type": ["null", "string"]},
                    "key_2__key_4": {"type": ["null", "object"]},
                }
            },
            None,
            {
                "key_1": 1,
                "key_2__key_3": "value",
                "key_2__key_4": '{"key_5": 1, "key_6": ["a", "b"]}',
            },
            None,
            id="flattened schema provided",
        ),
        pytest.param(
            None,
            99,
            {
                "key_1": 1,
                "key_2__key_3": "value",
                "key_2__key_4__key_5": 1,
                "key_2__key_4__key_6": '["a", "b"]',
            },
            None,
            id="flattened schema not provided",
        ),
        pytest.param(
            None, None, None, InvalidFlatteningRecordsParameter, id="no schema or max level provided"
        ),
    ],
)
def test_flatten_record(flattened_schema, max_level, expected, expected_exception):
    """Test flatten_record to obey the max_level and flattened_schema parameters."""
    record = {
        "key_1": 1,
        "key_2": {"key_3": "value", "key_4": {"key_5": 1, "key_6": ["a", "b"]}},
    }
    if expected_exception:
        with pytest.raises(expected_exception):
            flatten_record(
                record, max_level=max_level, flattened_schema=flattened_schema
            )
    else:
        result = flatten_record(
            record, max_level=max_level, flattened_schema=flattened_schema
        )
        assert expected == result
