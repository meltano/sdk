"""Test sample sync."""

from __future__ import annotations

import re
import typing as t
from logging import WARNING
from textwrap import dedent

import pytest

from singer_sdk.helpers._typing import (
    JSONSCHEMA_ANNOTATION_SECRET,
    JSONSCHEMA_ANNOTATION_WRITEONLY,
    is_array_type,
    is_boolean_type,
    is_date_or_datetime_type,
    is_datetime_type,
    is_integer_type,
    is_null_type,
    is_number_type,
    is_object_type,
    is_secret_type,
    is_string_array_type,
    is_string_type,
)
from singer_sdk.tap_base import Tap
from singer_sdk.typing import (
    DEFAULT_JSONSCHEMA_VALIDATOR,
    AllOf,
    AnyType,
    ArrayType,
    BooleanType,
    CustomType,
    DateTimeType,
    DateType,
    DiscriminatedUnion,
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

if t.TYPE_CHECKING:
    from pathlib import Path

    from pytest_snapshot.plugin import Snapshot

    from singer_sdk.streams.core import Stream

TYPE_FN_CHECKS: set[t.Callable] = {
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


def test_to_json():
    schema = PropertiesList(
        Property(
            "test_property",
            StringType,
            description="A test property",
            required=True,
        ),
        Property(
            "test_property_2",
            StringType,
            description="A test property",
        ),
        Property(
            "test_property_3",
            AllOf(
                ObjectType(Property("test_property_4", StringType)),
                ObjectType(Property("test_property_5", StringType)),
            ),
        ),
        additional_properties=False,
    )
    assert schema.to_json(indent=4) == dedent(
        """\
        {
            "type": "object",
            "properties": {
                "test_property": {
                    "type": [
                        "string"
                    ],
                    "description": "A test property"
                },
                "test_property_2": {
                    "type": [
                        "string",
                        "null"
                    ],
                    "description": "A test property"
                },
                "test_property_3": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "test_property_4": {
                                    "type": [
                                        "string",
                                        "null"
                                    ]
                                }
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "test_property_5": {
                                    "type": [
                                        "string",
                                        "null"
                                    ]
                                }
                            }
                        }
                    ]
                }
            },
            "required": [
                "test_property"
            ],
            "additionalProperties": false,
            "$schema": "https://json-schema.org/draft/2020-12/schema"
        }""",
    )


def test_any_type(caplog: pytest.LogCaptureFixture):
    schema = PropertiesList(
        Property("any_type", AnyType, description="Can be anything"),
    )
    with caplog.at_level(WARNING):
        assert schema.to_dict() == {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "any_type": {
                    "description": "Can be anything",
                },
            },
        }
        assert caplog.records[0].levelname == "WARNING"
        assert caplog.records[0].message == (
            "Could not append type because the JSON schema for the dictionary `{}` "
            "appears to be invalid."
        )


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
            ),
        ),
    )
    test2b = test2a.to_dict()
    assert test1a
    assert test1b
    assert test2a
    assert test2b


def test_default_value():
    prop = Property("test_property", StringType, default="test_property_value")
    assert prop.to_dict() == {
        "test_property": {"type": ["string", "null"], "default": "test_property_value"},
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
        },
    }


def test_property_title():
    title = "My Test Property"
    prop = Property("test_property", StringType, title=title)
    assert prop.to_dict() == {
        "test_property": {
            "type": ["string", "null"],
            "title": title,
        },
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
                },
            },
            {is_secret_type, is_string_type},
        ),
        (
            Property("my_prop4", StringType, description="This is a property."),
            {
                "my_prop4": {
                    "description": "This is a property.",
                    "type": ["string", "null"],
                },
            },
            {is_string_type},
        ),
        (
            Property("my_prop5", StringType, default="some_val"),
            {
                "my_prop5": {
                    "default": "some_val",
                    "type": ["string", "null"],
                },
            },
            {is_string_type},
        ),
        (
            Property("my_prop6", ArrayType(StringType)),
            {
                "my_prop6": {
                    "type": ["array", "null"],
                    "items": {"type": ["string"]},
                },
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
                },
            },
            {is_object_type, is_secret_type},
        ),
        (
            Property("my_prop8", IntegerType),
            {
                "my_prop8": {
                    "type": ["integer", "null"],
                },
            },
            {is_integer_type},
        ),
        (
            Property(
                "my_prop9",
                IntegerType,
                allowed_values=[1, 2, 3, 4, 5, 6, 7, 8, 9],
                examples=[1, 2, 3],
                deprecated=True,
            ),
            {
                "my_prop9": {
                    "type": ["integer", "null"],
                    "enum": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                    "examples": [1, 2, 3],
                    "deprecated": True,
                },
            },
            {is_integer_type},
        ),
        (
            Property(
                "my_prop10",
                ArrayType(
                    StringType(
                        allowed_values=["create", "delete", "insert", "update"],
                        examples=["insert", "update"],
                    ),
                ),
            ),
            {
                "my_prop10": {
                    "type": ["array", "null"],
                    "items": {
                        "type": ["string"],
                        "enum": ["create", "delete", "insert", "update"],
                        "examples": ["insert", "update"],
                    },
                },
            },
            {is_array_type, is_string_array_type},
        ),
        (
            Property(
                "my_prop11",
                ArrayType(
                    StringType,
                    allowed_values=[
                        ["create", "delete"],
                        ["insert", "update"],
                    ],
                ),
            ),
            {
                "my_prop11": {
                    "type": ["array", "null"],
                    "items": {
                        "type": ["string"],
                    },
                    "enum": [["create", "delete"], ["insert", "update"]],
                },
            },
            {is_array_type, is_string_array_type},
        ),
        (
            Property(
                "my_prop12",
                StringType(min_length=5, max_length=10, pattern="^a.*b$"),
            ),
            {
                "my_prop12": {
                    "type": ["string", "null"],
                    "minLength": 5,
                    "maxLength": 10,
                    "pattern": "^a.*b$",
                },
            },
            {is_string_type},
        ),
        (
            Property(
                "my_prop13",
                IntegerType(minimum=5, maximum=10),
            ),
            {
                "my_prop13": {
                    "type": ["integer", "null"],
                    "minimum": 5,
                    "maximum": 10,
                },
            },
            {is_integer_type},
        ),
        (
            Property(
                "my_prop14",
                IntegerType(exclusive_minimum=5, exclusive_maximum=10, multiple_of=2),
            ),
            {
                "my_prop14": {
                    "type": ["integer", "null"],
                    "exclusiveMinimum": 5,
                    "exclusiveMaximum": 10,
                    "multipleOf": 2,
                },
            },
            {is_integer_type},
        ),
    ],
)
def test_property_creation(
    property_obj: Property,
    expected_jsonschema: dict,
    type_fn_checks_true: set[t.Callable],
) -> None:
    property_dict = property_obj.to_dict()
    assert property_dict == expected_jsonschema
    for check_fn in TYPE_FN_CHECKS:
        property_name = next(iter(property_dict.keys()))
        property_node = property_dict[property_name]
        if check_fn in type_fn_checks_true:
            assert check_fn(property_node) is True, (
                f"{check_fn.__name__} was not True for {property_dict!r}"
            )
        else:
            assert check_fn(property_node) is False, (
                f"{check_fn.__name__} was not False for {property_dict!r}"
            )


def test_wrapped_type_dict():
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Type dict for <class 'singer_sdk.typing.ArrayType'> is not defined. "
            "Try instantiating it with a nested type such as ArrayType(StringType).",
        ),
    ):
        Property("bad_array_prop", ArrayType).to_dict()

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Type dict for <class 'singer_sdk.typing.ObjectType'> is not defined. "
            "Try instantiating it with a nested type such as ObjectType(StringType).",
        ),
    ):
        Property("bad_object_prop", ObjectType).to_dict()

    assert Property("good_array_prop", ArrayType(StringType)).to_dict() == {
        "good_array_prop": {
            "type": ["array", "null"],
            "items": {"type": ["string"]},
        },
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
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
                additional_properties=False,
            ),
            "no_additional_properties.json",
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
                Property("id", StringType),
                Property("email", StringType),
                Property("username", StringType),
                Property("phone_number", StringType),
                additional_properties=False,
            ),
            "duplicates_no_additional_properties.json",
            id="no required, duplicates, no additional properties allowed",
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
                Property("username", StringType, required=True),
                Property("phone_number", StringType),
                additional_properties=False,
            ),
            "required_no_additional_properties.json",
            id="required, no duplicates, no additional properties allowed",
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
                Property("email", StringType, required=True),
                Property("email", StringType, required=True),
                Property("username", StringType, required=True),
                Property("phone_number", StringType),
                additional_properties=False,
            ),
            "required_duplicates_no_additional_properties.json",
            id="required, duplicates, no additional properties allowed",
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


@pytest.mark.parametrize(
    "property_schemas,type_check_functions,results",
    [
        (
            [
                {
                    "anyOf": [
                        {"type": "array"},
                        {"type": "null"},
                    ],
                },
                {"type": "array"},
                {"type": ["array", "null"]},
            ],
            [is_array_type],
            [True],
        ),
        (
            [
                {
                    "anyOf": [
                        {"type": "boolean"},
                        {"type": "null"},
                    ],
                },
                {"type": "boolean"},
                {"type": ["boolean", "null"]},
            ],
            [is_boolean_type],
            [True],
        ),
        (
            [
                {
                    "anyOf": [
                        {"type": "integer"},
                        {"type": "null"},
                    ],
                },
                {"type": "integer"},
                {"type": ["integer", "null"]},
            ],
            [is_integer_type],
            [True],
        ),
        (
            [
                {
                    "anyOf": [
                        {"type": "string", "format": "date-time"},
                        {"type": "null"},
                    ],
                },
                {"type": "string"},
                {"type": ["string", "null"]},
            ],
            [is_string_type],
            [True],
        ),
        (
            [
                {
                    "anyOf": [
                        {"type": "string", "format": "date-time"},
                        {"type": "null"},
                    ],
                },
                {"type": "null"},
                {"type": ["string", "null"]},
            ],
            [is_null_type],
            [True],
        ),
        (
            [
                {
                    "anyOf": [
                        {"type": "string", "format": "date-time"},
                        {"type": "number"},
                    ],
                },
                {"type": "number"},
                {"type": ["number", "null"]},
            ],
            [is_number_type],
            [True],
        ),
        (
            [
                {
                    "anyOf": [
                        {"enum": ["developer", "team", "enterprise"]},
                        {"type": "string"},
                    ],
                },
            ],
            [is_string_type],
            [True],
        ),
    ],
)
def test_type_check_variations(property_schemas, type_check_functions, results):
    for property_schema in property_schemas:
        for type_check_function, result in zip(type_check_functions, results):
            assert type_check_function(property_schema) == result


def test_discriminated_union():
    th = DiscriminatedUnion(
        "flow",
        oauth=ObjectType(
            Property("client_id", StringType, required=True, secret=True),
            Property("client_secret", StringType, required=True, secret=True),
            additional_properties=False,
        ),
        password=ObjectType(
            Property("username", StringType, required=True),
            Property("password", StringType, required=True, secret=True),
            additional_properties=False,
        ),
    )

    validator = DEFAULT_JSONSCHEMA_VALIDATOR(th.to_dict())

    assert validator.is_valid(
        {
            "flow": "oauth",
            "client_id": "123",
            "client_secret": "456",
        },
    )
    assert validator.is_valid(
        {
            "flow": "password",
            "password": "123",
            "username": "456",
        },
    )
    assert not validator.is_valid(
        {
            "flow": "oauth",
            "client_id": "123",
        },
    )
    assert not validator.is_valid(
        {
            "flow": "password",
            "client_id": "123",
        },
    )


def test_schema_dependencies():
    th = ObjectType(
        # username/password
        Property("username", StringType, requires_properties=["password"]),
        Property("password", StringType, secret=True),
        # OAuth
        Property(
            "client_id",
            StringType,
            requires_properties=["client_secret", "refresh_token"],
        ),
        Property("client_secret", StringType, secret=True),
        Property("refresh_token", StringType, secret=True),
    )

    validator = DEFAULT_JSONSCHEMA_VALIDATOR(th.to_dict())

    assert validator.is_valid(
        {
            "username": "foo",
            "password": "bar",
        },
    )

    assert validator.is_valid(
        {
            "client_id": "foo",
            "client_secret": "bar",
            "refresh_token": "baz",
        },
    )

    assert not validator.is_valid(
        {
            "username": "foo",
        },
    )

    assert not validator.is_valid(
        {
            "client_id": "foo",
            "client_secret": "bar",
        },
    )


def test_is_datetime_type():
    assert is_datetime_type({"type": "string", "format": "date-time"})
    assert not is_datetime_type({"type": "string"})

    assert is_datetime_type({"anyOf": [{"type": "string", "format": "date-time"}]})
    assert not is_datetime_type({"anyOf": [{"type": "string"}]})

    assert is_datetime_type({"allOf": [{"type": "string", "format": "date-time"}]})
    assert not is_datetime_type({"allOf": [{"type": "string"}]})


def test_is_date_or_datetime_type():
    assert is_date_or_datetime_type({"type": "string", "format": "date"})
    assert is_date_or_datetime_type({"type": "string", "format": "date-time"})
    assert not is_date_or_datetime_type({"type": "string"})

    assert is_date_or_datetime_type(
        {"anyOf": [{"type": "string", "format": "date-time"}]},
    )
    assert is_date_or_datetime_type({"anyOf": [{"type": "string", "format": "date"}]})
    assert not is_date_or_datetime_type({"anyOf": [{"type": "string"}]})

    assert is_date_or_datetime_type(
        {"allOf": [{"type": "string", "format": "date-time"}]},
    )
    assert is_date_or_datetime_type({"allOf": [{"type": "string", "format": "date"}]})
    assert not is_date_or_datetime_type({"allOf": [{"type": "string"}]})


def test_is_string_array_type():
    assert is_string_array_type(
        {
            "type": "array",
            "items": {"type": "string"},
        },
    )
    assert not is_string_array_type(
        {
            "type": "array",
            "items": {"type": "integer"},
        },
    )

    assert is_string_array_type(
        {
            "anyOf": [
                {"type": "array", "items": {"type": "string"}},
                {"type": "null"},
            ],
        },
    )
    assert not is_string_array_type(
        {
            "anyOf": [
                {"type": "array", "items": {"type": "integer"}},
                {"type": "null"},
            ],
        },
    )

    assert is_string_array_type(
        {
            "allOf": [
                {"type": "array", "items": {"type": "string"}},
            ],
        },
    )
    assert not is_string_array_type(
        {
            "allOf": [
                {"type": "array", "items": {"type": "integer"}},
            ],
        },
    )


def test_is_array_type():
    assert is_array_type(
        {
            "type": "array",
            "items": {"type": "string"},
        },
    )
    assert not is_array_type(
        {
            "type": "string",
        },
    )

    assert is_array_type(
        {
            "anyOf": [
                {"type": "array"},
                {"type": "null"},
            ],
        },
    )
    assert not is_array_type(
        {
            "anyOf": [
                {"type": "string"},
                {"type": "null"},
            ],
        },
    )

    assert is_array_type(
        {
            "allOf": [
                {"type": "array"},
            ],
        },
    )
    assert not is_array_type(
        {
            "allOf": [
                {"type": "string"},
            ],
        },
    )
