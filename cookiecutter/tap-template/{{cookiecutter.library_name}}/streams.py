"""Stream class for {{ cookiecutter.tap_id }}."""

from pathlib import Path
from typing import Any, Dict

from tap_base.streams import {{ cookiecutter.stream_type }}StreamBase

from {{ cookiecutter.library_name }}.globals import PLUGIN_NAME

SCHEMAS_DIR = Path("./schemas")

{% if cookiecutter.stream_type == "GraphQL" %}

# TODO: - Implement a generic stream type for auth and post-processing (if any):
class Tap{{ cookiecutter.source_name }}Stream({{ cookiecutter.stream_type }}StreamBase):
    """{{ cookiecutter.source_name }} stream class."""

    tap_name = PLUGIN_NAME
    site_url_base = "https://api.mysample.com/"

    # # TODO (optional): If auth is required, implement `get_auth_header()``:
    # def get_auth_header(self) -> Dict[str, Any]:
    #     """Return an authorization header for REST request."""
    #     return {}

    # # TODO (optional): If post-processing is required, implement `post_process()`
    # def post_process(self, row: dict) -> dict:
    #     """Transform raw data from HTTP GET into the expected property values."""
    #     return row


# TODO: - Override `StreamA` and `StreamB` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.
class StreamA(Tap{{ cookiecutter.source_name }}Stream):
    name = "users"
    schema_filepath =  SCHEMAS_DIR / "users.json"
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

class StreamB(Tap{{ cookiecutter.source_name }}Stream):
    name = "groups"
    schema_filepath =  SCHEMAS_DIR / "groups.json"
    graphql_query = """
        groups {
            name
            id
            modified
        }
        """


{% elif cookiecutter.stream_type == "REST" %}

# TODO: - Implement a generic stream type for auth and post-processing (if any):
class Tap{{ cookiecutter.source_name }}Stream({{ cookiecutter.stream_type }}StreamBase):
    """{{ cookiecutter.source_name }} stream class."""

    tap_name = PLUGIN_NAME
    site_url_base = "https://api.mysample.com/"

    # # TODO (optional): If auth is required, implement `get_auth_header()``:
    # def get_auth_header(self) -> Dict[str, Any]:
    #     """Return an authorization header for REST request."""
    #     return {}

    # # TODO (optional): If post-processing is required, implement `post_process()`
    # def post_process(self, row: dict) -> dict:
    #     """Transform raw data from HTTP GET into the expected property values."""
    #     return row


# TODO: - Override `StreamA` and `StreamB` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.
class StreamA(Tap{{ cookiecutter.source_name }}Stream):
    stream_name = "users"
    url_suffix = "/users"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "users.json"


class StreamB(Tap{{ cookiecutter.source_name }}Stream):
    stream_name = "groups"
    url_suffix = "/groups"
    primary_keys = ["id"]
    replication_key = "modified"
    schema_filepath = SCHEMAS_DIR / "groups.json"


{% endif %}
