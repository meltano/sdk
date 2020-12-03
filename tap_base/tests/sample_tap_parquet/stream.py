"""Sample tap stream test for tap-parquet."""

from typing import Any, Dict, Iterable
from tap_base import TapStreamBase
from tap_base.tests.sample_tap_parquet.connection import SampleTapParquetConnection

import pyarrow as pa
import pyarrow.parquet as pq


class SampleTapParquetStream(TapStreamBase):
    """Sample tap test for parquet."""

    def get_row_generator(self) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects."""
        filepath = self._conn.get_config("filepath")
        if not filepath:
            raise ValueError("Parquet 'filepath' config cannot be blank.")
        try:
            parquet_file = pq.ParquetFile(filepath)
        except Exception as ex:
            raise IOError(f"Could not read from parquet filepath '{filepath}': {ex}")
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i)
            for batch in table.to_batches():
                for row in zip(*batch.columns):
                    yield {
                        table.column_names[i]: val for i, val in enumerate(row, start=0)
                    }
