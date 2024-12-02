"""Test _typing - specifically conform_record_data_types()."""

from __future__ import annotations

import datetime
import logging

import pytest
import sqlalchemy as sa

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
    append_type,
    to_sql_type,
)

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


def test_conform_object_additional_properties():
    schema = PropertiesList(
        Property(
            "object",
            PropertiesList(additional_properties=True),
        ),
    ).to_dict()

    record = {"object": {"extra": "value"}}
    expected_output = {"object": {"extra": "value"}}

    actual_output = conform_record_data_types(
        "test_stream",
        record,
        schema,
        TypeConformanceLevel.RECURSIVE,
        logger,
    )
    assert actual_output == expected_output


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
        == "2020-05-17"
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
    assert _conform_primitive_property(b"\xbc", {"type": "string"}) == "bc"

    assert _conform_primitive_property(b"\x00", {"type": "boolean"}) is False
    assert _conform_primitive_property(b"\xbc", {"type": "boolean"}) is True

    assert _conform_primitive_property(None, {"type": "boolean"}) is None
    assert _conform_primitive_property(0, {"type": "boolean"}) is False
    assert (
        _conform_primitive_property(
            0, {"anyOf": [{"type": "boolean"}, {"type": "integer"}]}
        )
        == 0
    )
    assert _conform_primitive_property(0, {"type": ["boolean", "integer"]}) == 0
    assert _conform_primitive_property(1, {"type": "boolean"}) is True
    assert _conform_primitive_property(1, {"type": ["boolean", "null"]}) is True
    assert _conform_primitive_property(1, {"type": ["boolean"]}) is True


@pytest.mark.filterwarnings("ignore:Use `JSONSchemaToSQL` instead.:DeprecationWarning")
@pytest.mark.parametrize(
    "jsonschema_type,expected",
    [
        ({"type": ["string", "null"]}, sa.types.VARCHAR),
        ({"type": ["integer", "null"]}, sa.types.INTEGER),
        ({"type": ["number", "null"]}, sa.types.DECIMAL),
        ({"type": ["boolean", "null"]}, sa.types.BOOLEAN),
        ({"type": "object", "properties": {}}, sa.types.VARCHAR),
        ({"type": "array"}, sa.types.VARCHAR),
        ({"format": "date", "type": ["string", "null"]}, sa.types.DATE),
        ({"format": "time", "type": ["string", "null"]}, sa.types.TIME),
        (
            {"format": "date-time", "type": ["string", "null"]},
            sa.types.DATETIME,
        ),
        (
            {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}]},
            sa.types.DATETIME,
        ),
        ({"anyOf": [{"type": "integer"}, {"type": "null"}]}, sa.types.INTEGER),
        (
            {"type": ["array", "object", "boolean", "null"]},
            sa.types.VARCHAR,
        ),
    ],
)
def test_to_sql_type(jsonschema_type, expected):
    assert isinstance(to_sql_type(jsonschema_type), expected)


@pytest.mark.parametrize(
    "type_dict,expected",
    [
        pytest.param({"type": "string"}, {"type": ["string", "null"]}, id="string"),
        pytest.param({"type": "integer"}, {"type": ["integer", "null"]}, id="integer"),
        pytest.param({"type": "number"}, {"type": ["number", "null"]}, id="number"),
        pytest.param({"type": "boolean"}, {"type": ["boolean", "null"]}, id="boolean"),
        pytest.param(
            {"type": "object", "properties": {}},
            {"type": ["object", "null"], "properties": {}},
            id="object",
        ),
        pytest.param({"type": "array"}, {"type": ["array", "null"]}, id="array"),
        pytest.param(
            {"anyOf": [{"type": "integer"}, {"type": "number"}]},
            {"anyOf": [{"type": "integer"}, {"type": "number"}, "null"]},
            id="anyOf",
        ),
        pytest.param(
            {"oneOf": [{"type": "integer"}, {"type": "number"}]},
            {"oneOf": [{"type": "integer"}, {"type": "number"}, {"type": "null"}]},
            id="oneOf",
        ),
    ],
)
def test_append_null(type_dict: dict, expected: dict):
    result = append_type(type_dict, "null")
    assert result == expected


def test_iterate_properties_list():
    primitive_property = Property("primitive", BooleanType)
    object_property = Property("object", PropertiesList(Property("value", BooleanType)))
    list_property = Property("list", ArrayType(BooleanType))

    properties_list = PropertiesList(primitive_property, object_property, list_property)

    assert primitive_property in properties_list
    assert object_property in properties_list
    assert list_property in properties_list
