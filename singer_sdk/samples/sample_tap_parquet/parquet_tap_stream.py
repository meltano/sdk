"""Sample tap stream test for tap-parquet."""

from typing import Iterable, Optional

import pyarrow.parquet as pq

from singer_sdk.streams.core import Stream

PLUGIN_NAME = "sample-tap-parquet"


class SampleTapParquetStream(Stream):
    """Sample tap test for parquet."""

    def get_records(self, partition: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        filepath = self.config.get("filepath")
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
                        table.column_names[i]: val.as_py()
                        for i, val in enumerate(row, start=0)
                    }
