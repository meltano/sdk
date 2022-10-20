"""Test sample sync."""

from __future__ import annotations

import pytest

from singer_sdk.helpers._meltano import _to_meltano_kind, meltano_yaml_str
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    DateType,
    DurationType,
    EmailType,
    HostnameType,
    IntegerType,
    IPv4Type,
    IPv6Type,
    JSONPointerType,
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


@pytest.mark.parametrize(
    "plugin_name,capabilities,config_jsonschema,expected_yaml",
    [
        (
            "tap-from-source",
            ["about", "stream-maps"],
            PropertiesList(
                Property(
                    "username",
                    StringType,
                    required=True,
                    description="The username to connect with.",
                ),
                Property(
                    "password",
                    StringType,
                    required=True,
                    secret=True,
                    description="The user's password.",
                ),
                Property("start_date", DateType),
            ).to_dict(),
            """name: tap-from-source
namespace: tap_from_source

## The following could not be auto-detected:
# maintenance_status:   #
# repo:                 #
# variant:              #
# label:                #
# description:          #
# pip_url:              #
# domain_url:           #
# logo_url:             #
# keywords: []          #

capabilities:
 - about
 - stream-maps
settings_group_validation:
 - - username
   - password
settings:
- name: username
  label: Username
  kind: string
  description: The username to connect with.
- name: password
  label: Password
  kind: password
  description: The user's password.
- name: start_date
  label: Start Date
  kind: date_iso8601
  description: null
""",
        )
    ],
)
def test_meltano_yml_creation(
    plugin_name: str,
    capabilities: list[str],
    config_jsonschema: dict,
    expected_yaml: str,
):
    assert expected_yaml == meltano_yaml_str(
        plugin_name, capabilities, config_jsonschema
    )


@pytest.mark.parametrize(
    "type_dict,expected_kindstr",
    [
        # Handled Types:
        (StringType.type_dict, "string"),
        (DateTimeType.type_dict, "date_iso8601"),
        (DateType.type_dict, "date_iso8601"),
        (BooleanType.type_dict, "boolean"),
        (IntegerType.type_dict, "integer"),
        (
            DurationType.type_dict,
            "string",
        ),
        # Treat as strings:
        (TimeType.type_dict, "string"),
        (EmailType.type_dict, "string"),
        (HostnameType.type_dict, "string"),
        (IPv4Type.type_dict, "string"),
        (IPv6Type.type_dict, "string"),
        (UUIDType.type_dict, "string"),
        (URIType.type_dict, "string"),
        (URIReferenceType.type_dict, "string"),
        (URITemplateType.type_dict, "string"),
        (JSONPointerType.type_dict, "string"),
        (RelativeJSONPointerType.type_dict, "string"),
        (RegexType.type_dict, "string"),
        # No handling and no compatible default:
        (NumberType.type_dict, None),
    ],
)
def test_meltano_type_to_kind(type_dict: dict, expected_kindstr: str | None) -> None:
    assert _to_meltano_kind(type_dict) == expected_kindstr
