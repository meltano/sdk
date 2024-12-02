"""Tap, target and stream test fixtures."""

from __future__ import annotations

import typing as t
from contextlib import contextmanager

import pytest
from typing_extensions import override

from singer_sdk import Stream, Tap
from singer_sdk.helpers._compat import datetime_fromisoformat
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    IntegerType,
    ObjectType,
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

    @override
    def get_records(
        self,
        context: dict | None,
    ) -> t.Iterable[dict[str, t.Any]]:
        """Generate records."""
        yield {"id": 1, "value": "Egypt"}
        yield {"id": 2, "value": "Germany"}
        yield {"id": 3, "value": "India"}

    @contextmanager
    def with_replication_method(self, method: str | None) -> t.Iterator[None]:
        """Context manager to temporarily override the replication method."""
        original_method = self.forced_replication_method
        self.forced_replication_method = method
        yield
        self.forced_replication_method = original_method


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

    @override
    def compare_start_date(self, value: str, start_date_value: str) -> str:
        """Compare a value to a start date value."""

        start_timestamp = datetime_fromisoformat(start_date_value).timestamp()
        return max(value, start_timestamp, key=float)


class SimpleTestTap(Tap):
    """Test tap class."""

    name = "test-tap"
    config_jsonschema = PropertiesList(
        Property("username", StringType, required=True),
        Property("password", StringType, required=True),
        Property("start_date", DateTimeType),
        Property(
            "nested",
            ObjectType(
                Property("key", StringType, required=True),
            ),
            required=False,
        ),
        Property(
            "array",
            ArrayType(
                ObjectType(
                    Property("key", StringType, required=True),
                ),
            ),
            required=False,
        ),
        additional_properties=False,
    ).to_dict()

    @override
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
