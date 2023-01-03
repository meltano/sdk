"""Test _typing - specifically conform_record_data_types()."""
import datetime
import logging
import unittest

from singer_sdk.helpers._typing import (
    _conform_primitive_property,
    conform_record_data_types, ConformanceLevel,
)
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    PropertiesList,
    Property,
    StringType,
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

    actual_output = conform_record_data_types("test_stream", record, schema, ConformanceLevel.RECURSIVE, logger)
    assert actual_output == expected_output


def test_primitive_arrays_are_conformed():
    schema = PropertiesList(
        Property("list", ArrayType(BooleanType)),
    ).to_dict()

    record = {
        "list": [b"\x01", b"\x00"],
    }

    expected_output = {"list": [True, False]}

    actual_output = conform_record_data_types("test_stream", record, schema, ConformanceLevel.RECURSIVE, logger)
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
        "list": [b"\x01", b"\x00"]
    }

    expected_output = {
        "primitive": True,
        "object": {"value": b"\x01"},
        "list": [b"\x01", b"\x00"]
    }

    actual_output = conform_record_data_types("test_stream", record, schema, ConformanceLevel.ROOT_ONLY, logger)
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
        "list": [b"\x01", b"\x00"]
    }

    actual_output = conform_record_data_types("test_stream", record, schema, ConformanceLevel.NONE, logger)
    assert actual_output == record


def test_object_arrays_are_conformed():
    schema = PropertiesList(
        Property("list", ArrayType(PropertiesList(Property("value", BooleanType)))),
    ).to_dict()

    record = {"list": [{"value": b"\x01"}, {"value": b"\x00"}]}

    expected_output = {"list": [{"value": True}, {"value": False}]}

    actual_output = conform_record_data_types("test_stream", record, schema, ConformanceLevel.RECURSIVE, logger)
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
            }
        },
    }

    record = {"list": [{"value": b"\x01"}, b"\x00"]}

    expected_output = {"list": [{"value": True}, False]}

    actual_output = conform_record_data_types("test_stream", record, schema, ConformanceLevel.RECURSIVE, logger)
    assert actual_output == expected_output


def test_nested_objects_are_conformed():
    schema = PropertiesList(
        Property("object", PropertiesList(Property("value", BooleanType))),
    ).to_dict()

    record = {"object": {"value": b"\x01"}}

    expected_output = {"object": {"value": True}}

    actual_output = conform_record_data_types("test_stream", record, schema, ConformanceLevel.RECURSIVE, logger)
    assert actual_output == expected_output


class TestSimpleEval(unittest.TestCase):
    def test_simple_schema_removes_types(self):
        schema = PropertiesList(
            Property("keep", StringType),
        ).to_dict()

        record = {"keep": "hello", "remove": "goodbye"}

        expected_output = {"keep": "hello"}

        with self.assertLogs("log", level="WARN") as logs:
            actual_output = conform_record_data_types(
                "test_stream", record, schema, ConformanceLevel.RECURSIVE, logger
            )
            assert actual_output == expected_output
            self.assertEqual(
                logs.output,
                [
                    "WARNING:log:Properties ('remove',) were present in the 'test_stream' stream but not found in catalog "
                    "schema. Ignoring."
                ],
            )

    def test_nested_objects_remove_types(self):
        schema = PropertiesList(
            Property("object", PropertiesList(Property("keep", StringType))),
        ).to_dict()

        record = {"object": {"keep": "hello", "remove": "goodbye"}}

        expected_output = {"object": {"keep": "hello"}}

        with self.assertLogs("log", level="WARN") as logs:
            actual_output = conform_record_data_types(
                "test_stream", record, schema, ConformanceLevel.RECURSIVE, logger
            )
            assert actual_output == expected_output
            self.assertEqual(
                logs.output,
                [
                    "WARNING:log:Properties ('object.remove',) were present in the 'test_stream' stream but not found in "
                    "catalog schema. Ignoring."
                ],
            )

    def test_object_arrays_remove_types(self):
        schema = PropertiesList(
            Property("list", ArrayType(PropertiesList(Property("keep", StringType)))),
        ).to_dict()

        record = {"list": [{"keep": "hello", "remove": "goodbye"}]}

        expected_output = {"list": [{"keep": "hello"}]}

        with self.assertLogs("log", level="WARN") as logs:
            actual_output = conform_record_data_types(
                "test_stream", record, schema, ConformanceLevel.RECURSIVE, logger
            )
            assert actual_output == expected_output
            self.assertEqual(
                logs.output,
                [
                    "WARNING:log:Properties ('list.remove',) were present in the 'test_stream' stream but not found in "
                    "catalog schema. Ignoring."
                ],
            )


def test_conform_primitives():
    assert (
            _conform_primitive_property(datetime.datetime(2020, 5, 17), {"type": "string"})
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

    assert _conform_primitive_property(b"\x00", {"type": "boolean"}) == False
    assert _conform_primitive_property(b"\xBC", {"type": "boolean"}) == True

    assert _conform_primitive_property(None, {"type": "boolean"}) is None
    assert _conform_primitive_property(0, {"type": "boolean"}) == False
    assert _conform_primitive_property(1, {"type": "boolean"}) == True
