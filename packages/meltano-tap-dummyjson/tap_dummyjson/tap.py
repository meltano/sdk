from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from . import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk import Stream


class TapDummyJSON(Tap):
    """DummyJSON tap class."""

    name = "tap-dummyjson"
    package_name: str = "meltano-tap-dummyjson"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            title="DummyJSON Username",
            description="Username for the API service",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="DummyJSON Password",
            description="Password for the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            title="Start Date",
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            title="API URL",
            default="https://dummyjson.com",
            description="The base url for the API service",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.Products(self),
        ]


if __name__ == "__main__":
    TapDummyJSON.cli()
