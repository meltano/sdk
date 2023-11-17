"""Tap, target and stream test fixtures."""

from __future__ import annotations

import typing as t

import pendulum
import pytest

from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)


class SimpleTestStream(Stream):
    """Test stream class."""

    name = "test"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
        Property("updatedAt", DateTimeType, required=True),
    ).to_dict()
    replication_key = "updatedAt"

    def __init__(self, tap: Tap):
        """Create a new stream."""
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(
        self,
        context: dict | None,  # noqa: ARG002
    ) -> t.Iterable[dict[str, t.Any]]:
        """Generate records."""
        yield {"id": 1, "value": "Egypt"}
        yield {"id": 2, "value": "Germany"}
        yield {"id": 3, "value": "India"}


class UnixTimestampIncrementalStream(SimpleTestStream):
    name = "unix_ts"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
        Property("updatedAt", IntegerType, required=True),
    ).to_dict()
    replication_key = "updatedAt"


class UnixTimestampIncrementalStream2(UnixTimestampIncrementalStream):
    name = "unix_ts_override"

    def compare_start_date(self, value: str, start_date_value: str) -> str:
        """Compare a value to a start date value."""

        start_timestamp = pendulum.parse(start_date_value).format("X")
        return max(value, start_timestamp, key=float)


class SimpleTestTap(Tap):
    """Test tap class."""

    name = "test-tap"
    config_jsonschema = PropertiesList(
        Property("username", StringType, required=True),
        Property("password", StringType, required=True),
        Property("start_date", DateTimeType),
        additional_properties=False,
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """List all streams."""
        return [
            SimpleTestStream(self),
            UnixTimestampIncrementalStream(self),
            UnixTimestampIncrementalStream2(self),
        ]


@pytest.fixture
def tap_class():
    """Return the tap class."""
    return SimpleTestTap


@pytest.fixture
def tap() -> SimpleTestTap:
    """Tap instance."""
    return SimpleTestTap(
        config={
            "username": "utest",
            "password": "ptest",
            "start_date": "2021-01-01",
        },
        parse_env_config=False,
    )
