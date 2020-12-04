"""Sample tap test for tap-parquet."""

from typing import List, Any

from singer import Schema, metadata
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

    def discover_available_stream_ids(self) -> List[str]:
        # TODO: automatically infer this from the parquet schema
        return ["placeholder"]

    def discover_stream(self, tap_stream_id) -> CatalogEntry:
        """Return a list of all streams (tables)."""
        # TODO: automatically infer this from the parquet schema
        schema = Schema(
            properties={
                "f0": Schema(type=["string", "None"]),
                "f1": Schema(type=["string", "None"]),
                "f2": Schema(type=["string", "None"]),
            }
        )
        key_properties = ["f0"]
        replication_keys = ["f0"]
        replication_method = "INCREMENTAL"
        return CatalogEntry(
            tap_stream_id=tap_stream_id,
            stream=tap_stream_id,
            schema=schema,
            metadata=metadata.get_standard_metadata(
                schema=schema.to_dict(),
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=replication_keys,
                schema_name=None,
            ),
            key_properties=key_properties,
            replication_key=replication_keys[0],
            replication_method=replication_method,
            is_view=None,
            database=None,
            table=None,
            row_count=None,
            stream_alias=None,
        )
