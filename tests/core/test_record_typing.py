"""Typing tests."""

from __future__ import annotations

import logging
import typing as t
from datetime import datetime

import pytest

from singer_sdk.helpers._compat import datetime_fromisoformat as parse
from singer_sdk.helpers._typing import (
    TypeConformanceLevel,
    conform_record_data_types,
    get_datelike_property_type,
    to_json_compatible,
)


@pytest.mark.parametrize(
    "record,schema,expected_row,ignore_props_message",
    [
        (
            {"updatedAt": parse("2021-08-25T20:05:28+00:00")},
            {"properties": {"updatedAt": True}},
            {"updatedAt": "2021-08-25T20:05:28+00:00"},
            None,
        ),
        (
            {"updatedAt": parse("2021-08-25T20:05:28Z")},
            {"properties": {"updatedAt": True}},
            {"updatedAt": "2021-08-25T20:05:28+00:00"},
            None,
        ),
        (
            {"updatedAt": parse("2021-08-25T20:05:28")},
            {"properties": {"updatedAt": True}},
            {"updatedAt": "2021-08-25T20:05:28+00:00"},
            None,
        ),
        (
            {"present": 1, "absent": "2"},
            {"properties": {"present": {"type": "integer"}}},
            {"present": 1},
            (
                "Properties ('absent',) were present in the 'test-stream' stream but "
                "not found in catalog schema. Ignoring."
            ),
        ),
    ],
    ids=[
        "datetime with offset",
        "datetime with timezone",
        "datetime without timezone",
        "ignored_props_message",
    ],
)
def test_conform_record_data_types(
    record: dict[str, t.Any],
    schema: dict,
    expected_row: dict,
    ignore_props_message: str,
    caplog: pytest.LogCaptureFixture,
):
    stream_name = "test-stream"
    logger = logging.getLogger("test-logger")

    with caplog.at_level(logging.INFO, logger=logger.name):
        actual = conform_record_data_types(
            stream_name,
            record,
            schema,
            TypeConformanceLevel.RECURSIVE,
            logger,
        )
        if ignore_props_message:
            assert ignore_props_message in caplog.text
        else:
            assert not caplog.text

    assert actual == expected_row


@pytest.mark.parametrize(
    "datetime_val,expected",
    [
        (parse("2021-08-25T20:05:28+00:00"), "2021-08-25T20:05:28+00:00"),
        (parse("2021-08-25T20:05:28+07:00"), "2021-08-25T20:05:28+07:00"),
        (
            datetime.strptime(  # noqa: DTZ007
                "2021-08-25T20:05:28",
                "%Y-%m-%dT%H:%M:%S",
            ),
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


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({"type": ["null", "string"]}, None),
        ({"type": "string", "format": "date-time"}, "date-time"),
        ({"type": "string", "format": "date"}, "date"),
        ({"type": "string", "format": "time"}, "time"),
        (
            {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}]},
            "date-time",
        ),
        ({"anyOf": [{"type": "string", "maxLength": 5}]}, None),
        (
            {
                "anyOf": [
                    {
                        "type": "array",
                        "items": {"type": "string", "format": "date-time"},
                    },
                    {"type": "null"},
                ],
            },
            None,
        ),
    ],
)
def test_get_datelike_property_type(schema, expected):
    actual = get_datelike_property_type(schema)
    assert actual == expected
