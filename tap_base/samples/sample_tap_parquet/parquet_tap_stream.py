"""Sample tap stream test for tap-parquet."""

from typing import Iterable

import pyarrow.parquet as pq

from tap_base.streams.core import TapStreamBase

PLUGIN_NAME = "sample-tap-parquet"


class SampleTapParquetStream(TapStreamBase):
    """Sample tap test for parquet."""

    tap_name = PLUGIN_NAME

    def get_record_generator(self) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        filepath = self.get_config("filepath")
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
