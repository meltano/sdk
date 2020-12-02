"""Sample tap test for tap-parquet."""

from typing import List, Union, Any

from singer import Catalog, Schema
from singer.catalog import CatalogEntry

from tap_base.connection_base import DiscoverableConnectionBase

import pyarrow as pa
import pyarrow.parquet as pq


class SampleTapParquetConnection(DiscoverableConnectionBase):
    """Parquet Tap Connection Class."""

    _conn: Any

    def open_connection(self) -> Any:
        """Connect to parquet database."""
        self._conn = "placeholder"
        return self._conn

    def get_available_stream_ids(self) -> List[str]:
        return ["placeholder"]

    def discover_stream(self, tap_stream_id) -> CatalogEntry:
        """Return a list of all streams (tables)."""
        _schema = Schema(
            properties=[
                Schema(description="f0", type=["string", "None"]),
                Schema(description="f1", type=["string", "None"]),
                Schema(description="f2", type=["string", "None"]),
            ]
        )
        return CatalogEntry(
            tap_stream_id=tap_stream_id,
            stream=tap_stream_id,
            key_properties=[],
            schema=_schema,
            replication_key=None,
            is_view=None,
            database=None,
            table=None,
            row_count=None,
            stream_alias=None,
            metadata=None,
            replication_method=None,
        )
