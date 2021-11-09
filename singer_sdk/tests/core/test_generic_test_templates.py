"""Test the generic tests from `singer_sdk.testing`."""

import pytest

from singer_sdk.testing import TapTestRunner, AttributeTests, TapValidationError

import pendulum
import pytest
import requests
from typing import Optional, List, Iterable, Dict, Any

from singer_sdk.helpers.jsonpath import _compile_jsonpath
from singer_sdk.streams.core import (
    REPLICATION_FULL_TABLE,
    REPLICATION_INCREMENTAL,
    Stream,
)
from singer_sdk.tap_base import Tap
from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    BooleanType,
    PropertiesList,
    Property,
    StringType,
)


class SimpleTestStream(Stream):
    """Test stream class."""

    name = "test"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
        Property("updatedAt", DateTimeType, required=True),
        Property("same_value", StringType),
        Property("is_country", BooleanType),
    ).to_dict()
    replication_key = "updatedAt"

    def __init__(self, tap: Tap):
        """Create a new stream."""
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Generate records."""
        yield {
            "id": 1,
            "value": "Egypt",
            "updatedAt": "2021-01-01T00:00:00Z",
            "is_country": True,
            "same_value": "foo",
        }
        yield {
            "id": 2,
            "value": "Germany",
            "updatedAt": "2021-02-01T00:00:00Z",
            "is_country": None,
            "same_value": "foo",
        }
        yield {
            "id": 3,
            "value": "India",
            "updatedAt": "2021-03-01T00:00:00Z",
            "is_country": False,
            "same_value": "foo",
        }


class SimpleTestTap(Tap):
    """Test tap class."""

    name = "test-tap"
    settings_jsonschema = PropertiesList(Property("start_date", DateTimeType)).to_dict()

    def discover_streams(self) -> List[Stream]:
        """List all streams."""
        return [SimpleTestStream(self)]


@pytest.fixture
def tap_test_runner():
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
        tap_config={"start_date": "2021-01-01"},
        tap_creation_args={"catalog": catalog_dict},
    )
    runner.run_sync()

    yield runner


@pytest.fixture
def attribute_test_kwargs(tap_test_runner):
    return {"test_runner": tap_test_runner, "stream_name": "test"}


def test_attribute_unique_test(attribute_test_kwargs):
    test_class = AttributeTests.unique.value
    t1 = test_class(attribute_name="id", **attribute_test_kwargs)
    t2 = test_class(attribute_name="same_value", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(TapValidationError):
        t2.run_test()


def test_attribute_is_number_expectation(attribute_test_kwargs):
    test_class = AttributeTests.is_number.value
    t1 = test_class(attribute_name="id", **attribute_test_kwargs)
    t2 = test_class(attribute_name="value", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(TapValidationError):
        t2.run_test()

def test_attribute_is_boolean_expectation(attribute_test_kwargs):
    test_class = AttributeTests.is_boolean.value
    t1 = test_class(attribute_name="is_country", **attribute_test_kwargs)
    t2 = test_class(attribute_name="id", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(TapValidationError):
        t2.run_test()


def test_attribute_is_datetime_expectation(attribute_test_kwargs):
    test_class = AttributeTests.is_datetime.value
    t1 = test_class(attribute_name="updatedAt", **attribute_test_kwargs)
    t2 = test_class(attribute_name="same_value", **attribute_test_kwargs)

    t1.run_test()
    with pytest.raises(TapValidationError):
        t2.run_test()
