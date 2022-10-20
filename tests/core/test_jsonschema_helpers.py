"""Test sample sync."""

import re
from typing import List

import pytest

from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    CustomType,
    DateTimeType,
    DateType,
    DurationType,
    EmailType,
    HostnameType,
    IntegerType,
    IPv4Type,
    IPv6Type,
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
        Property("password", StringType, required=True, secret=True),
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


@pytest.mark.parametrize(
    "property_obj,expected_jsonschema",
    [
        (
            Property("my_prop1", StringType, required=True),
            {"my_prop1": {"type": ["string"]}},
        ),
        (
            Property("my_prop2", StringType, required=False),
            {"my_prop2": {"type": ["string", "null"]}},
        ),
        (
            Property("my_prop3", StringType, secret=True),
            {
                "my_prop3": {
                    "type": ["string", "null"],
                    "secret": True,
                    "writeOnly": True,
                }
            },
        ),
        (
            Property("my_prop4", StringType, description="This is a property."),
            {
                "my_prop4": {
                    "description": "This is a property.",
                    "type": ["string", "null"],
                }
            },
        ),
        (
            Property("my_prop5", StringType, default="some_val"),
            {
                "my_prop5": {
                    "default": "some_val",
                    "type": ["string", "null"],
                }
            },
        ),
    ],
)
def test_property_creation(property_obj: Property, expected_jsonschema: dict) -> None:
    assert property_obj.to_dict() == expected_jsonschema
    # assert property_obj.type_dict == expected_jsonschema["type"]


def test_wrapped_type_dict():
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Type dict for <class 'singer_sdk.typing.ArrayType'> is not defined. "
            + "Try instantiating it with a nested type such as ArrayType(StringType)."
        ),
    ):
        Property("bad_array_prop", ArrayType).to_dict()

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Type dict for <class 'singer_sdk.typing.ObjectType'> is not defined. "
            + "Try instantiating it with a nested type such as ObjectType(StringType)."
        ),
    ):
        Property("bad_object_prop", ObjectType).to_dict()

    assert Property("good_array_prop", ArrayType(StringType)).to_dict() == {
        "good_array_prop": {
            "type": ["array", "null"],
            "items": {"type": ["string"]},
        }
    }


def test_array_type():
    wrapped_type = StringType
    expected_json_schema = {
        "type": "array",
        "items": wrapped_type.type_dict,
    }

    assert ArrayType(wrapped_type).type_dict == expected_json_schema


@pytest.mark.parametrize(
    "properties,addtional_properties",
    [
        (
            [
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
            ],
            None,
        ),
        (
            [
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
            ],
            StringType,
        ),
        (
            [
                Property("id", StringType),
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
            ],
            None,
        ),
        (
            [
                Property("id", StringType),
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
            ],
            StringType,
        ),
        (
            [
                Property("id", StringType),
                Property("email", StringType, True),
                Property("username", StringType, True),
                Property("phone_number", StringType),
            ],
            None,
        ),
        (
            [
                Property("id", StringType),
                Property("email", StringType, True),
                Property("username", StringType, True),
                Property("phone_number", StringType),
            ],
            StringType,
        ),
        (
            [
                Property("id", StringType),
                Property("email", StringType, True),
                Property("email", StringType, True),
                Property("username", StringType, True),
                Property("phone_number", StringType),
            ],
            None,
        ),
        (
            [
                Property("id", StringType),
                Property("email", StringType, True),
                Property("email", StringType, True),
                Property("username", StringType, True),
                Property("phone_number", StringType),
            ],
            StringType,
        ),
    ],
    ids=[
        "no requried, no duplicates, no additional properties",
        "no requried, no duplicates, additional properties",
        "no requried, duplicates, no additional properties",
        "no requried, duplicates, additional properties",
        "requried, no duplicates, no additional properties",
        "requried, no duplicates, additional properties",
        "requried, duplicates, no additional properties",
        "requried, duplicates, additional properties",
    ],
)
def test_object_type(properties: List[Property], addtional_properties: JSONTypeHelper):
    merged_property_schemas = {
        name: schema for p in properties for name, schema in p.to_dict().items()
    }

    required = [p.name for p in properties if not p.optional]
    required_schema = {"required": required} if required else {}
    addtional_properties_schema = (
        {"additionalProperties": addtional_properties.type_dict}
        if addtional_properties
        else {}
    )

    expected_json_schema = {
        "type": "object",
        "properties": merged_property_schemas,
        **required_schema,
        **addtional_properties_schema,
    }

    object_type = ObjectType(*properties, additional_properties=addtional_properties)
    assert object_type.type_dict == expected_json_schema


def test_custom_type():
    json_schema = {
        "type": ["string"],
        "pattern": "^meltano$",
    }

    assert CustomType(json_schema).type_dict == json_schema
