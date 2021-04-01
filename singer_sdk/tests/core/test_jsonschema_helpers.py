"""Test sample sync."""

from typing import List

from singer_sdk.tap_base import Tap
from singer_sdk.streams.core import Stream
from singer_sdk.typing import (
    ArrayType,
    ObjectType,
    StringType,
    IntegerType,
    PropertiesList,
    Property,
)


class ConfigTestTap(Tap):
    """Test tap class."""

    config_jsonschema = PropertiesList(
        Property("host", StringType, required=True),
        Property("username", StringType, required=True),
        Property("password", StringType, required=True),
        Property("batch_size", IntegerType, default=-1),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        return []


def test_nested_complex_objects():
    test1a = Property(
        "Datasets",
        ArrayType(StringType),
    )
    test1b = test1a.to_dict()
    test2a = Property(
        "Datasets",
        ArrayType(
            ObjectType(
                Property("DatasetId", StringType),
                Property("DatasetName", StringType),
            )
        ),
    )
    test2b = test2a.to_dict()
    assert test1a and test1b and test2a and test2b


def test_default_value():
    prop = Property("test_property", StringType, default="test_property_value")
    assert prop.to_dict() == {
        "test_property": {"type": ["string", "null"], "default": "test_property_value"}
    }


def test_tap_config_default_injection():

    config_dict = {"host": "gitlab.com", "username": "foo", "password": "bar"}

    tap = ConfigTestTap(config=config_dict, parse_env_config=False, catalog={})

    expected_tap_config = {
        "host": "gitlab.com",
        "username": "foo",
        "password": "bar",
        "batch_size": -1,
    }

    assert dict(tap.config) == expected_tap_config

    config_dict = {
        "host": "gitlab.com",
        "username": "foo",
        "password": "bar",
        "batch_size": 500,
    }

    tap = ConfigTestTap(config=config_dict, parse_env_config=False, catalog={})

    assert dict(tap.config) == config_dict
