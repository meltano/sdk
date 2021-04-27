"""Stream class for {{ cookiecutter.tap_id }}."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from {{ cookiecutter.library_name }}.client import {{ cookiecutter.source_name }}Stream

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

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


{% if cookiecutter.stream_type == "GraphQL" %}
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.
class UsersStream({{ cookiecutter.source_name }}Stream):
    name = "users"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        Property("age", IntegerType),
        Property("email", StringType),
        Property(
            "address",
            ObjectType(
                Property("street", StringType),
                Property("city", StringType),
                Property("state", StringType),
                Property("zip", StringType),
            )
        ),
    ).to_dict()
    primary_keys = ["id"]
    replication_key = None
    graphql_query = """
        users {
            name
            id
            age
            email
            address {
                street
                city
                state
                zip
            }
        }
        """


class GroupsStream({{ cookiecutter.source_name }}Stream):
    name = "groups"
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        Property("modified", DateTimeType),
    ).to_dict()
    primary_keys = ["id"]
    replication_key = "modified"
    graphql_query = """
        groups {
            name
            id
            modified
        }
        """


{% elif cookiecutter.stream_type in ("Other", "REST") %}
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.
class UsersStream({{ cookiecutter.source_name }}Stream):
    name = "users"
{% if cookiecutter.stream_type == "REST" %}
    path = "/users"
{% endif %}
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        Property("age", IntegerType),
        Property("email", StringType),
        Property("street", StringType),
        Property("city", StringType),
        Property("state", StringType),
        Property("zip", StringType),
    ).to_dict()


class GroupsStream({{ cookiecutter.source_name }}Stream):
    name = "groups"
{% if cookiecutter.stream_type == "REST" %}
    path = "/groups"
{% endif %}
    primary_keys = ["id"]
    replication_key = "modified"
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        Property("modified", DateTimeType),
    ).to_dict()
{% endif %}
