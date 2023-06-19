"""Test _typing - specifically conform_record_data_types()."""

from __future__ import annotations

import datetime
import logging
import typing as t

from singer_sdk.helpers._typing import (
    TypeConformanceLevel,
    _conform_primitive_property,
    conform_record_data_types,
)
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    PropertiesList,
    Property,
    StringType,
    to_sql_type,
)

if t.TYPE_CHECKING:
    import pytest

logger = logging.getLogger("log")


def test_simple_schema_conforms_types():
    schema = PropertiesList(
        Property("true", BooleanType),
        Property("false", BooleanType),
    ).to_dict()

    record = {
        "true": b"\x01",
        "false": b"\x00",
    }

    expected_output = {
        "true": True,
        "false": False,
    }

    actual_output = conform_record_data_types(
        "test_stream",
        record,
        schema,
        TypeConformanceLevel.RECURSIVE,
        logger,
    )
    assert actual_output == expected_output


def test_primitive_arrays_are_conformed():
    schema = PropertiesList(
        Property("list", ArrayType(BooleanType)),
    ).to_dict()

    record = {
        "list": [b"\x01", b"\x00"],
    }

    expected_output = {"list": [True, False]}

    actual_output = conform_record_data_types(
        "test_stream",
        record,
        schema,
        TypeConformanceLevel.RECURSIVE,
        logger,
    )
    assert actual_output == expected_output


def test_only_root_fields_are_conformed_for_root_level():
    schema = PropertiesList(
        Property("primitive", BooleanType),
        Property("object", PropertiesList(Property("value", BooleanType))),
        Property("list", ArrayType(BooleanType)),
    ).to_dict()

    record = {
        "primitive": b"\x01",
        "object": {"value": b"\x01"},
        "list": [b"\x01", b"\x00"],
    }

    expected_output = {
        "primitive": True,
        "object": {"value": b"\x01"},
        "list": [b"\x01", b"\x00"],
    }

    actual_output = conform_record_data_types(
        "test_stream",
        record,
        schema,
        TypeConformanceLevel.ROOT_ONLY,
        logger,
    )
    assert actual_output == expected_output


def test_no_fields_are_conformed_for_none_level():
    schema = PropertiesList(
        Property("primitive", BooleanType),
        Property("object", PropertiesList(Property("value", BooleanType))),
        Property("list", ArrayType(BooleanType)),
    ).to_dict()

    record = {
        "primitive": b"\x01",
        "object": {"value": b"\x01"},
        "list": [b"\x01", b"\x00"],
    }

    actual_output = conform_record_data_types(
        "test_stream",
        record,
        schema,
        TypeConformanceLevel.NONE,
        logger,
    )
    assert actual_output == record


def test_object_arrays_are_conformed():
    schema = PropertiesList(
        Property("list", ArrayType(PropertiesList(Property("value", BooleanType)))),
    ).to_dict()

    record = {"list": [{"value": b"\x01"}, {"value": b"\x00"}]}

    expected_output = {"list": [{"value": True}, {"value": False}]}

    actual_output = conform_record_data_types(
        "test_stream",
        record,
        schema,
        TypeConformanceLevel.RECURSIVE,
        logger,
    )
    assert actual_output == expected_output


def test_mixed_arrays_are_conformed():
    schema = {
        "type": "object",
        "properties": {
            "list": {
                "type": ["array", "null"],
                "items": {
                    "type": ["object", "boolean"],
                    "properties": {"value": {"type": ["boolean", "null"]}},
                },
            },
        },
    }

    record = {"list": [{"value": b"\x01"}, b"\x00"]}

    expected_output = {"list": [{"value": True}, False]}

    actual_output = conform_record_data_types(
        "test_stream",
        record,
        schema,
        TypeConformanceLevel.RECURSIVE,
        logger,
    )
    assert actual_output == expected_output


def test_nested_objects_are_conformed():
    schema = PropertiesList(
        Property("object", PropertiesList(Property("value", BooleanType))),
    ).to_dict()

    record = {"object": {"value": b"\x01"}}

    expected_output = {"object": {"value": True}}

    actual_output = conform_record_data_types(
        "test_stream",
        record,
        schema,
        TypeConformanceLevel.RECURSIVE,
        logger,
    )
    assert actual_output == expected_output


def test_simple_schema_removes_types(caplog: pytest.LogCaptureFixture):
    schema = PropertiesList(
        Property("keep", StringType),
    ).to_dict()

    record = {"keep": "hello", "remove": "goodbye"}

    expected_output = {"keep": "hello"}

    with caplog.at_level(logging.WARNING):
        actual_output = conform_record_data_types(
            "test_stream",
            record,
            schema,
            TypeConformanceLevel.RECURSIVE,
            logger,
        )
        assert actual_output == expected_output
        assert caplog.records[0].message == (
            "Properties ('remove',) were present in the 'test_stream' stream but not "
            "found in catalog schema. Ignoring."
        )


def test_nested_objects_remove_types(caplog: pytest.LogCaptureFixture):
    schema = PropertiesList(
        Property("object", PropertiesList(Property("keep", StringType))),
    ).to_dict()

    record = {"object": {"keep": "hello", "remove": "goodbye"}}

    expected_output = {"object": {"keep": "hello"}}

    with caplog.at_level(logging.WARNING):
        actual_output = conform_record_data_types(
            "test_stream",
            record,
            schema,
            TypeConformanceLevel.RECURSIVE,
            logger,
        )
        assert actual_output == expected_output
        assert caplog.records[0].message == (
            "Properties ('object.remove',) were present in the 'test_stream' stream "
            "but not found in catalog schema. Ignoring."
        )


def test_object_arrays_remove_types(caplog: pytest.LogCaptureFixture):
    schema = PropertiesList(
        Property("list", ArrayType(PropertiesList(Property("keep", StringType)))),
    ).to_dict()

    record = {"list": [{"keep": "hello", "remove": "goodbye"}]}

    expected_output = {"list": [{"keep": "hello"}]}

    with caplog.at_level(logging.WARNING):
        actual_output = conform_record_data_types(
            "test_stream",
            record,
            schema,
            TypeConformanceLevel.RECURSIVE,
            logger,
        )
        assert actual_output == expected_output
        assert caplog.records[0].message == (
            "Properties ('list.remove',) were present in the 'test_stream' stream but "
            "not found in catalog schema. Ignoring."
        )


def test_conform_primitives():
    assert (
        _conform_primitive_property(
            datetime.datetime(2020, 5, 17, tzinfo=datetime.timezone.utc),
            {"type": "string"},
        )
        == "2020-05-17T00:00:00+00:00"
    )
    assert (
        _conform_primitive_property(datetime.date(2020, 5, 17), {"type": "string"})
        == "2020-05-17T00:00:00+00:00"
    )
    assert (
        _conform_primitive_property(datetime.timedelta(365), {"type": "string"})
        == "1971-01-01T00:00:00+00:00"
    )
    assert (
        _conform_primitive_property(datetime.time(12, 0, 0), {"type": "string"})
        == "12:00:00"
    )

    assert _conform_primitive_property(b"\x00", {"type": "string"}) == "00"
    assert _conform_primitive_property(b"\xBC", {"type": "string"}) == "bc"

    assert _conform_primitive_property(b"\x00", {"type": "boolean"}) is False
    assert _conform_primitive_property(b"\xBC", {"type": "boolean"}) is True

    assert _conform_primitive_property(None, {"type": "boolean"}) is None
    assert _conform_primitive_property(0, {"type": "boolean"}) is False
    assert _conform_primitive_property(1, {"type": "boolean"}) is True



import sqlalchemy
import pytest
@pytest.mark.parametrize(
    "jsonschema_type,expected",
    [
        ({'type': ['string', 'null']}, sqlalchemy.types.VARCHAR),
        ({'type': ['integer', 'null']}, sqlalchemy.types.INTEGER),
        ({'type': ['number', 'null']}, sqlalchemy.types.DECIMAL),
        ({'type': ['boolean', 'null']}, sqlalchemy.types.BOOLEAN),
        ({'type': "object", "properties": {}}, sqlalchemy.types.VARCHAR),
        ({'type': "array"}, sqlalchemy.types.VARCHAR),
        ({ "format": "date", "type": [ "string", "null" ] }, sqlalchemy.types.DATE),
        ({ "format": "time", "type": [ "string", "null" ] }, sqlalchemy.types.TIME),
        ({ "format": "date-time", "type": [ "string", "null" ] }, sqlalchemy.types.DATETIME),
        ({"anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}]}, sqlalchemy.types.DATETIME),
        ({ "anyOf": [ {"type": "integer"}, {"type": "null"}, ], }, sqlalchemy.types.INTEGER),
    ]
)
def test_to_sql_type(jsonschema_type, expected):
    assert isinstance(to_sql_type(jsonschema_type), expected)
