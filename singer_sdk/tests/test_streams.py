from __future__ import annotations
from os import name

from typing import Any, Dict, Iterable, List

import pytest

from singer_sdk.helpers.typing import IntegerType, PropertiesList, StringType
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL, Stream
from singer_sdk.tap_base import Tap


class SimpleTestStream(Stream):
    """Test stream class."""

    name = "test"
    schema = PropertiesList(
        IntegerType("id", required=True),
        StringType("value", required=True),
        StringType("updatedAt", required=True),
    ).to_dict()
    replication_key = ["updatedAt"]

    def __init__(self, tap: Tap):
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, partition: dict | None) -> Iterable[Dict[str, Any]]:
        yield {"id": 1, "value": "Egypt"}
        yield {"id": 2, "value": "Germany"}
        yield {"id": 3, "value": "India"}


class SimpleTapTest(Tap):
    """Test tap class."""

    state = None

    def discover_streams(self) -> List[Stream]:
        return [SimpleTestStream(self)]


@pytest.fixture
def tap() -> SimpleTapTest:
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
    return SimpleTapTest(
        config={},
        parse_env_config=False,
        catalog=catalog_dict,
    )


@pytest.fixture
def stream(tap: SimpleTapTest) -> SimpleTestStream:
    """Stream instance"""
    return tap.load_streams()[0]


def test_stream_apply_catalog(tap: SimpleTapTest, stream: SimpleTestStream):
    """Applying a catalog to a stream should overwrite fields."""

    assert stream.primary_keys is None
    assert stream.replication_key == ["updatedAt"]
    assert stream.replication_method == REPLICATION_INCREMENTAL
    assert stream.forced_replication_method is None

    stream.apply_catalog(catalog_dict=tap.input_catalog)

    assert stream.primary_keys == ["id"]
    assert stream.replication_key is None
    assert stream.replication_method == REPLICATION_FULL_TABLE
    assert stream.forced_replication_method == REPLICATION_FULL_TABLE
