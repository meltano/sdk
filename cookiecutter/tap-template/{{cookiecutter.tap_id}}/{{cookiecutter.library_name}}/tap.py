"""{{ cookiecutter.source_name }} tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from {{ cookiecutter.library_name }}.streams import (
    {{ cookiecutter.source_name }}Stream,
{%- if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}
    UsersStream,
    GroupsStream,
{%- endif %}
)

{%- if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    UsersStream,
    GroupsStream,
]
{%- endif %}


class Tap{{ cookiecutter.source_name }}(Tap):
    """{{ cookiecutter.source_name }} tap class."""
    name = "{{ cookiecutter.tap_id }}"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property("auth_token", th.StringType, required=True),
        th.Property("project_ids", th.ArrayType(th.StringType), required=True),
        th.Property("start_date", th.DateTimeType),
        th.Property("api_url", th.StringType, default="https://api.mysample.com"),
    ).to_dict()
{%- if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
{%- endif %}
