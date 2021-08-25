"""Stream tests."""

from typing import Any, Dict
import logging

import pytest
import pendulum
from datetime import datetime

from singer_sdk.helpers._typing import conform_record_data_types, to_json_compatible


@pytest.mark.parametrize(
    "row,schema,expected_row",
    [
        (
            {"updatedAt": pendulum.parse("2021-08-25T20:05:28+00:00")},
            {"properties": {"updatedAt": True}},
            {"updatedAt": "2021-08-25T20:05:28+00:00"},
        ),
        (
            {"updatedAt": pendulum.parse("2021-08-25T20:05:28Z")},
            {"properties": {"updatedAt": True}},
            {"updatedAt": "2021-08-25T20:05:28+00:00"},
        ),
        (
            {"updatedAt": pendulum.parse("2021-08-25T20:05:28")},
            {"properties": {"updatedAt": True}},
            {"updatedAt": "2021-08-25T20:05:28+00:00"},
        ),
    ],
)
def test_conform_record_data_types(row: Dict[str, Any], schema: dict, expected_row):
    stream_name = "test-stream"
    # TODO: mock this out
    logger = logging.getLogger()
    actual = conform_record_data_types(stream_name, row, schema, logger)
    print(row["updatedAt"].isoformat())
    assert actual == expected_row


@pytest.mark.parametrize(
    "datetime_val,expected",
    [
        (pendulum.parse("2021-08-25T20:05:28+00:00"), "2021-08-25T20:05:28+00:00"),
        (pendulum.parse("2021-08-25T20:05:28+07:00"), "2021-08-25T20:05:28+07:00"),
        (
            datetime.strptime("2021-08-25T20:05:28", "%Y-%m-%dT%H:%M:%S"),
            "2021-08-25T20:05:28+00:00",
        ),
        (
            datetime.strptime("2021-08-25T20:05:28-03:00", "%Y-%m-%dT%H:%M:%S%z"),
            "2021-08-25T20:05:28-03:00",
        ),
        ("2021-08-25T20:05:28", "2021-08-25T20:05:28"),
        ("2021-08-25T20:05:28Z", "2021-08-25T20:05:28Z"),
    ],
)
def test_to_json_compatible(datetime_val, expected):
    actual = to_json_compatible(datetime_val)

    assert actual == expected
