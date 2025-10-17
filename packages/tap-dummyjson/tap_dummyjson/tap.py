from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from . import streams


class TapDummyJSON(Tap):
    """DummyJSON tap class."""

    name = "tap-dummyjson"

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

    def discover_streams(self):
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.Products(self),
        ]


if __name__ == "__main__":
    TapDummyJSON.cli()
