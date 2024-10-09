"""{{ cookiecutter.source_name }} tap class."""

from __future__ import annotations

from singer_sdk import {{ 'SQL' if cookiecutter.stream_type == 'SQL' else '' }}Tap
from singer_sdk import typing as th  # JSON schema typing helpers

{%- if cookiecutter.stream_type == "SQL" %}

from {{ cookiecutter.library_name }}.client import {{ cookiecutter.source_name }}Stream
{%- else %}

# TODO: Import your custom stream types here:
from {{ cookiecutter.library_name }} import streams
{%- endif %}


class Tap{{ cookiecutter.source_name }}({{ 'SQL' if cookiecutter.stream_type == 'SQL' else '' }}Tap):
    """{{ cookiecutter.source_name }} tap class."""

    name = "{{ cookiecutter.tap_id }}"

    {%- if cookiecutter.stream_type == "SQL" %}
    default_stream_class = {{ cookiecutter.source_name }}Stream
    {%- endif %}

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType),
            required=True,
            title="Project IDs",
            description="Project IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            title="API URL",
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
        {%- if cookiecutter.stream_type in ("GraphQL", "REST") %}
        th.Property(
            "user_agent",
            th.StringType,
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap_name>/<tap_version>'"
            ),
        ),
        {%- endif %}
    ).to_dict()
{%- if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}

    def discover_streams(self) -> list[streams.{{ cookiecutter.source_name }}Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]
{%- endif %}


if __name__ == "__main__":
    Tap{{ cookiecutter.source_name }}.cli()
