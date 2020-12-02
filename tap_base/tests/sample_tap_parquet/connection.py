"""Sample tap test for tap-parquet."""

from typing import List, Union, Any

from tap_base import GenericConnectionBase

import pyarrow as pa
import pyarrow.parquet as pq


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records."""


class SampleTapParquetConnection(GenericConnectionBase):
    """Parquet Tap Connection Class."""

    _conn: Any

    def open_connection(self) -> Any:
        """Connect to parquet database."""
        self._conn = "placeholder"
        return self._conn

    def get_available_stream_ids() -> List[str]:
        return ["placeholder"]
