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

if TYPE_CHECKING:
    from collections.abc import Iterable


class Tap{{ cookiecutter.source_name }}(Tap):
    """Singer tap for {{ cookiecutter.source_name }}."""

    name = "{{ cookiecutter.tap_id }}"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
    {%- if cookiecutter.auth_method in ("OAuth2", "JWT") %}
        th.Property(
            "client_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Client ID",
            description="OAuth client ID for the {{ cookiecutter.source_name }} OAuth app",
        ),
        th.Property(
            "client_secret",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Client Secret",
            description="OAuth client secret for the {{ cookiecutter.source_name }} OAuth app",
        ),
        th.Property(
            "refresh_token",
            th.StringType(),
            secret=True,  # Flag config as protected.
            title="Refresh Token",
            description="OAuth refresh token for the {{ cookiecutter.source_name }} OAuth app",
        ),
    {%- elif cookiecutter.auth_method == "Basic Auth" %}
        th.Property(
            "username",
            th.StringType(nullable=False),
            required=True,
            title="Username",
            description="The {{ cookiecutter.source_name }} username",
        ),
        th.Property(
            "password",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Password",
            description="The {{ cookiecutter.source_name }} password",
        ),
    {%- else %}
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against {{ cookiecutter.source_name }}",
        ),
    {%- endif %}
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
{%- endif %}


if __name__ == "__main__":
    Tap{{ cookiecutter.source_name }}.cli()
