"""Test sample sync."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Callable

import pytest
from pytest_snapshot.plugin import Snapshot

from singer_sdk.helpers._typing import (
    JSONSCHEMA_ANNOTATION_SECRET,
    JSONSCHEMA_ANNOTATION_WRITEONLY,
    is_array_type,
    is_boolean_type,
    is_date_or_datetime_type,
    is_datetime_type,
    is_integer_type,
    is_object_type,
    is_secret_type,
    is_string_array_type,
    is_string_type,
)
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

TYPE_FN_CHECKS: set[Callable] = {
    is_array_type,
    is_boolean_type,
    is_date_or_datetime_type,
    is_datetime_type,
    is_integer_type,
    is_secret_type,
    is_string_array_type,
    is_string_type,
}


class ConfigTestTap(Tap):
    """Test tap class."""

    name = "config-test"
    config_jsonschema = PropertiesList(
        Property("host", StringType, required=True),
        Property("username", StringType, required=True),
        Property("password", StringType, required=True, secret=True),
        Property("batch_size", IntegerType, default=-1),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
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
    "property_obj,expected_jsonschema,type_fn_checks_true",
    [
        (
            Property("my_prop1", StringType, required=True),
            {"my_prop1": {"type": ["string"]}},
            {is_string_type},
        ),
        (
            Property("my_prop2", StringType, required=False),
            {"my_prop2": {"type": ["string", "null"]}},
            {is_string_type},
        ),
        (
            Property("my_prop3", StringType, secret=True),
            {
                "my_prop3": {
                    "type": ["string", "null"],
                    JSONSCHEMA_ANNOTATION_SECRET: True,
                    JSONSCHEMA_ANNOTATION_WRITEONLY: True,
                }
            },
            {is_secret_type, is_string_type},
        ),
        (
            Property("my_prop4", StringType, description="This is a property."),
            {
                "my_prop4": {
                    "description": "This is a property.",
                    "type": ["string", "null"],
                }
            },
            {is_string_type},
        ),
        (
            Property("my_prop5", StringType, default="some_val"),
            {
                "my_prop5": {
                    "default": "some_val",
                    "type": ["string", "null"],
                }
            },
            {is_string_type},
        ),
        (
            Property("my_prop6", ArrayType(StringType)),
            {
                "my_prop6": {
                    "type": ["array", "null"],
                    "items": {"type": ["string"]},
                }
            },
            {is_array_type, is_string_array_type},
        ),
        (
            Property(
                "my_prop7",
                ObjectType(
                    Property("not_a_secret", StringType),
                    Property("is_a_secret", StringType, secret=True),
                ),
            ),
            {
                "my_prop7": {
                    "type": ["object", "null"],
                    "properties": {
                        "not_a_secret": {"type": ["string", "null"]},
                        "is_a_secret": {
                            "type": ["string", "null"],
                            "secret": True,
                            "writeOnly": True,
                        },
                    },
                }
            },
            {is_object_type, is_secret_type},
        ),
        (
            Property("my_prop8", IntegerType),
            {
                "my_prop8": {
                    "type": ["integer", "null"],
                }
            },
            {is_integer_type},
        ),
        (
            Property(
                "my_prop9",
                IntegerType,
                allowed_values=[1, 2, 3, 4, 5, 6, 7, 8, 9],
                examples=[1, 2, 3],
            ),
            {
                "my_prop9": {
                    "type": ["integer", "null"],
                    "enum": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                    "examples": [1, 2, 3],
                }
            },
            {is_integer_type},
        ),
    ],
)
def test_property_creation(
    property_obj: Property,
    expected_jsonschema: dict,
    type_fn_checks_true: set[Callable],
) -> None:
    property_dict = property_obj.to_dict()
    assert property_dict == expected_jsonschema
    for check_fn in TYPE_FN_CHECKS:
        property_name = list(property_dict.keys())[0]
        property_node = property_dict[property_name]
        if check_fn in type_fn_checks_true:
            assert (
                check_fn(property_node) is True
            ), f"{check_fn.__name__} was not True for {repr(property_dict)}"
        else:
            assert (
                check_fn(property_node) is False
            ), f"{check_fn.__name__} was not False for {repr(property_dict)}"


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


@pytest.mark.snapshot
@pytest.mark.parametrize(
    "schema_obj,snapshot_name",
    [
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
            ),
            "base.json",
            id="no required, no duplicates, no additional properties",
        ),
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
                additional_properties=StringType,
            ),
            "additional_properties.json",
            id="no required, no duplicates, additional properties",
        ),
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
            ),
            "duplicates.json",
            id="no required, duplicates, no additional properties",
        ),
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
                additional_properties=StringType,
            ),
            "duplicates_additional_properties.json",
            id="no required, duplicates, additional properties",
        ),
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("email", StringType, required=True),
                Property("username", StringType, required=True),
                Property("phone_number", StringType),
            ),
            "required.json",
            id="required, no duplicates, no additional properties",
        ),
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("email", StringType, required=True),
                Property("username", StringType, required=True),
                Property("phone_number", StringType),
                additional_properties=StringType,
            ),
            "required_additional_properties.json",
            id="required, no duplicates, additional properties",
        ),
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("email", StringType, required=True),
                Property("email", StringType, required=True),
                Property("username", StringType, required=True),
                Property("phone_number", StringType),
            ),
            "required_duplicates.json",
            id="required, duplicates, no additional properties",
        ),
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("email", StringType, required=True),
                Property("email", StringType, required=True),
                Property("username", StringType, required=True),
                Property("phone_number", StringType),
                additional_properties=StringType,
            ),
            "required_duplicates_additional_properties.json",
            id="required, duplicates, additional properties",
        ),
        pytest.param(
            ObjectType(
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
                pattern_properties={
                    "^attr_[a-z]+$": StringType,
                },
            ),
            "pattern_properties.json",
            id="pattern properties",
        ),
    ],
)
def test_object_type(
    schema_obj: ObjectType,
    snapshot_dir: Path,
    snapshot_name: str,
    snapshot: Snapshot,
):
    snapshot.snapshot_dir = snapshot_dir.joinpath("jsonschema")
    snapshot.assert_match(schema_obj.to_json(indent=2), snapshot_name)


def test_custom_type():
    json_schema = {
        "type": ["string"],
        "pattern": "^meltano$",
    }

    assert CustomType(json_schema).type_dict == json_schema
