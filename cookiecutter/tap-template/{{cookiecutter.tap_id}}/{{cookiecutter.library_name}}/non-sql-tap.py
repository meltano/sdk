"""{{ cookiecutter.source_name }} tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from {{ cookiecutter.library_name }} import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class Tap{{ cookiecutter.source_name }}(Tap):
    """Singer tap for {{ cookiecutter.source_name }}."""

    name = "{{ cookiecutter.tap_id }}"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType(nullable=False), nullable=False),
            required=True,
            title="Project IDs",
            description="Project IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            title="API URL",
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
        {%- if cookiecutter.stream_type in ("GraphQL", "REST") %}
        th.Property(
            "user_agent",
            th.StringType(nullable=True),
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap_name>/<tap_version>'"
            ),
        ),
        {%- endif %}
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.{{ cookiecutter.source_name }}Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    Tap{{ cookiecutter.source_name }}.cli()
