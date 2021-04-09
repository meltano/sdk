"""Stream tests."""

from typing import Any, Dict, Iterable, List, Optional

import pytest
import pendulum

from singer_sdk.typing import (
    IntegerType,
    PropertiesList,
    Property,
    StringType,
    DateTimeType,
)
from singer_sdk.streams.core import (
    REPLICATION_FULL_TABLE,
    REPLICATION_INCREMENTAL,
    Stream,
)
from singer_sdk.tap_base import Tap


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
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        yield {"id": 1, "value": "Egypt"}
        yield {"id": 2, "value": "Germany"}
        yield {"id": 3, "value": "India"}


class SimpleTestTap(Tap):
    """Test tap class."""

    settings_jsonschema = PropertiesList(Property("start_date", DateTimeType)).to_dict()

    def discover_streams(self) -> List[Stream]:
        return [SimpleTestStream(self)]


@pytest.fixture
def tap() -> SimpleTestTap:
    """Tap instance."""

    catalog_dict = {
        "streams": [
            {
                "key_properties": ["id"],
                "tap_stream_id": SimpleTestStream.name,
                "stream": SimpleTestStream.name,
                "schema": SimpleTestStream.schema,
                "replication_method": REPLICATION_FULL_TABLE,
                "replication_key": None,
            }
        ]
    }
    return SimpleTestTap(
        config={"start_date": "2021-01-01"},
        parse_env_config=False,
        catalog=catalog_dict,
    )


@pytest.fixture
def stream(tap: SimpleTestTap) -> SimpleTestStream:
    """Stream instance"""
    return tap.load_streams()[0]


def test_stream_apply_catalog(tap: SimpleTestTap, stream: SimpleTestStream):
    """Applying a catalog to a stream should overwrite fields."""

    assert stream.primary_keys is None
    assert stream.replication_key == "updatedAt"
    assert stream.replication_method == REPLICATION_INCREMENTAL
    assert stream.forced_replication_method is None

    stream.apply_catalog(catalog_dict=tap.input_catalog)

    assert stream.primary_keys == ["id"]
    assert stream.replication_key is None
    assert stream.replication_method == REPLICATION_FULL_TABLE
    assert stream.forced_replication_method == REPLICATION_FULL_TABLE


def test_stream_starting_timestamp(tap: SimpleTestTap, stream: SimpleTestStream):
    """Validate state and start_time setting handling."""
    timestamp_value = "2021-02-01"

    assert stream.get_starting_timestamp(None) == pendulum.parse(
        stream.config.get("start_date")
    )
    tap.load_state(
        {
            "bookmarks": {
                stream.name: {
                    "replication_key": stream.replication_key,
                    "replication_key_value": timestamp_value,
                }
            }
        }
    )
    assert stream.replication_key == "updatedAt"
    assert stream.replication_method == REPLICATION_INCREMENTAL
    assert stream.is_timestamp_replication_key
    assert stream.get_starting_timestamp(None) == pendulum.parse(
        timestamp_value
    ), f"Incorrect starting timestamp. Tap state was {dict(tap.state)}"
