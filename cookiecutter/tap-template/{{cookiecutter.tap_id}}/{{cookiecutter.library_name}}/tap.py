"""{{ cookiecutter.source_name }} tap class."""

from typing import List

from singer_sdk import {{ 'SQL' if cookiecutter.stream_type == 'SQL' else '' }}Tap, {{ 'SQL' if cookiecutter.stream_type == 'SQL' else '' }}Stream
from singer_sdk import typing as th  # JSON schema typing helpers

{%- if cookiecutter.stream_type == "SQL" %}
from {{ cookiecutter.library_name }}.client import {{ cookiecutter.source_name }}Stream
{%- else %}
# TODO: Import your custom stream types here:
from {{ cookiecutter.library_name }}.streams import (
    {{ cookiecutter.source_name }}Stream,
{%- if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}
    UsersStream,
    GroupsStream,
{%- endif %}
)
{%- endif %}

{%- if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    {{ cookiecutter.source_name }}Stream,
    UsersStream,
    GroupsStream,
]
{%- endif %}


class Tap{{ cookiecutter.source_name }}({{ 'SQL' if cookiecutter.stream_type == 'SQL' else '' }}Tap):
    """{{ cookiecutter.source_name }} tap class."""

    name = "{{ cookiecutter.tap_id }}"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType),
            required=True,
            description="Project IDs to replicate"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.mysample.com",
            description="The url for the API service"
        ),
    ).to_dict()
{%- if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
{%- endif %}


if __name__ == "__main__":
    Tap{{ cookiecutter.source_name }}.cli()
