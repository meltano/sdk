"""Test sample sync."""

from typing import List

import pytest

from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    DateType,
    DurationType,
    EmailType,
    HostnameType,
    IPv4Type,
    IPv6Type,
    IntegerType,
    JSONPointerType,
    JSONTypeHelper,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    RegexType,
    RelativeJSONPointerType,
    StringType,
    TimeType,
    URIReferenceType,
    URITemplateType,
    URIType,
    UUIDType,
)


class ConfigTestTap(Tap):
    """Test tap class."""

    name = "config-test"
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


def test_property_description():
    text = "A test property"
    prop = Property("test_property", StringType, description=text)
    assert prop.to_dict() == {
        "test_property": {
            "type": ["string", "null"],
            "description": text,
        }
    }


@pytest.mark.parametrize(
    "json_type,expected_json_schema",
    [
        (
            StringType,
            {
                "type": ["string"],
            },
        ),
        (
            DateTimeType,
            {
                "type": ["string"],
                "format": "date-time",
            },
        ),
        (
            TimeType,
            {
                "type": ["string"],
                "format": "time",
            },
        ),
        (
            DateType,
            {
                "type": ["string"],
                "format": "date",
            },
        ),
        (
            DurationType,
            {
                "type": ["string"],
                "format": "duration",
            },
        ),
        (
            EmailType,
            {
                "type": ["string"],
                "format": "email",
            },
        ),
        (
            HostnameType,
            {
                "type": ["string"],
                "format": "hostname",
            },
        ),
        (
            IPv4Type,
            {
                "type": ["string"],
                "format": "ipv4",
            },
        ),
        (
            IPv6Type,
            {
                "type": ["string"],
                "format": "ipv6",
            },
        ),
        (
            UUIDType,
            {
                "type": ["string"],
                "format": "uuid",
            },
        ),
        (
            URIType,
            {
                "type": ["string"],
                "format": "uri",
            },
        ),
        (
            URIReferenceType,
            {
                "type": ["string"],
                "format": "uri-reference",
            },
        ),
        (
            URITemplateType,
            {
                "type": ["string"],
                "format": "uri-template",
            },
        ),
        (
            JSONPointerType,
            {
                "type": ["string"],
                "format": "json-pointer",
            },
        ),
        (
            RelativeJSONPointerType,
            {
                "type": ["string"],
                "format": "relative-json-pointer",
            },
        ),
        (
            RegexType,
            {
                "type": ["string"],
                "format": "regex",
            },
        ),
        (
            BooleanType,
            {
                "type": ["boolean"],
            },
        ),
        (
            IntegerType,
            {
                "type": ["integer"],
            },
        ),
        (
            NumberType,
            {
                "type": ["number"],
            },
        ),
    ],
)
def test_inbuilt_type(json_type: JSONTypeHelper, expected_json_schema: dict):
    assert json_type.type_dict == expected_json_schema
