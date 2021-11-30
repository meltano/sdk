"""Test the generic tests from `singer_sdk.testing`."""

from typing import Any, Dict, Iterable, List, Optional

import pytest

from singer_sdk.streams.core import REPLICATION_FULL_TABLE, Stream
from singer_sdk.tap_base import Tap
from singer_sdk.testing import AttributeTests, StreamTests, TapTestRunner, TapTests
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)


class SimpleTestStream(Stream):
    """Test stream class."""

    name = "test"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("array_value", ArrayType(StringType)),
        Property("bool_value", BooleanType),
        Property("datetime_iso8601_value", DateTimeType),
        Property("float_value", NumberType),
        Property("int_value", IntegerType),
        Property("object_value", ObjectType()),
        Property("str_value_unique", StringType, required=True),
        Property("str_value_nonunique", DateTimeType, required=True),
        Property("missing_value", StringType),
    ).to_dict()
    replication_key = "updatedAt"

    def __init__(self, tap: Tap):
        """Create a new stream."""
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Generate records."""
        yield {
            "id": 1,
            "array_value": ["a", "b", "c"],
            "bool_value": True,
            "datetime_iso8601_value": "2021-01-01T00:00:00Z",
            "float_value": 9.8,
            "int_value": 5,
            "object_value": {"a": "b", "c": 1},
            "str_value_unique": "Meltano is beautiful",
            "str_value_nonunique": "Melty",
            "missing_value": None,
        }
        yield {
            "id": 2,
            "array_value": [],
            "bool_value": False,
            "datetime_iso8601_value": "2021-02-01T00:00:00Z",
            "float_value": 100000.1,
            "int_value": 5,
            "object_value": {100.1: "foo", 1: 2},
            "str_value_unique": "Meltano is delicious",
            "str_value_nonunique": "Melty",
        }
        yield {
            "id": 3,
            "array_value": ["melty"],
            "bool_value": True,
            "datetime_iso8601_value": "2021-03-01T00:00:00Z",
            "float_value": 0.1,
            "int_value": 0,
            "object_value": {"a": ["things"]},
            "str_value_unique": "Meltano is healthy",
            "str_value_nonunique": "Melty",
        }


class SimpleTestTap(Tap):
    """Test tap class."""

    name = "test-tap"
    settings_jsonschema = {}

    def discover_streams(self) -> List[Stream]:
        """List all streams."""
        return [SimpleTestStream(self)]


@pytest.fixture
def test_runner():
    catalog_dict = {
        "streams": [
            {
                "key_properties": ["id"],
                "tap_stream_id": SimpleTestStream.name,
                "stream": SimpleTestStream.name,
                "schema": SimpleTestStream.schema,
                "replication_method": REPLICATION_FULL_TABLE,
                "replication_key": None,
            }
        ]
    }
    runner = TapTestRunner(
        tap_class=SimpleTestTap,
        tap_config={},
        tap_kwargs={"catalog": catalog_dict},
    )
    runner.run_sync()

    yield runner


@pytest.fixture
def attribute_test_kwargs(test_runner):
    return {"stream_records": test_runner.records["test"], "stream_name": "test"}


def test_attribute_unique_test(attribute_test_kwargs):
    test_class = AttributeTests.unique.value
    t1 = test_class(attribute_name="str_value_unique", **attribute_test_kwargs)
    t2 = test_class(attribute_name="str_value_nonunique", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(AssertionError):
        t2.run_test()


def test_attribute_is_number_test(attribute_test_kwargs):
    test_class = AttributeTests.is_number.value
    t1 = test_class(attribute_name="float_value", **attribute_test_kwargs)
    t2 = test_class(attribute_name="str_value_nonunique", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(AssertionError):
        t2.run_test()


def test_attribute_is_boolean_test(attribute_test_kwargs):
    test_class = AttributeTests.is_boolean.value
    t1 = test_class(attribute_name="bool_value", **attribute_test_kwargs)
    t2 = test_class(attribute_name="str_value_nonunique", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(AssertionError):
        t2.run_test()


def test_attribute_is_datetime_test(attribute_test_kwargs):
    test_class = AttributeTests.is_datetime.value
    t1 = test_class(attribute_name="datetime_iso8601_value", **attribute_test_kwargs)
    t2 = test_class(attribute_name="str_value_nonunique", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(AssertionError):
        t2.run_test()


def test_attribute_is_integer_test(attribute_test_kwargs):
    test_class = AttributeTests.is_integer.value
    t1 = test_class(attribute_name="int_value", **attribute_test_kwargs)
    t2 = test_class(attribute_name="str_value_nonunique", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(AssertionError):
        t2.run_test()


def test_attribute_is_object_test(attribute_test_kwargs):
    test_class = AttributeTests.is_object.value
    t1 = test_class(attribute_name="object_value", **attribute_test_kwargs)
    t2 = test_class(attribute_name="int_value", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(AssertionError):
        t2.run_test()
