"""Sample Parquet target stream class, which handles writing streams."""

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq

from tap_base.target_stream_base import TargetStreamBase


class SampleParquetTargetStream(TargetStreamBase):

    DEFAULT_BATCH_SIZE_ROWS = 100000
    DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
    DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel

    DATETIME_ERROR_TREATMENT = "MAX"

    @staticmethod
    def _get_parquet_type(singer_type: Union[str, Dict]) -> Any:
        if singer_type in ["decimal", "float", "double"]:
            return pa.decimal128
        if singer_type in ["date-time"]:
            return pa.datetime
        if singer_type in ["date"]:
            return pa.date64
        return pa.string

    def _get_parquet_schema(self) -> List[Tuple[str, Any]]:
        col_list: List[Tuple[str, Any]] = []
        for property in self.schema["properties"]:
            col_list.append(
                (property["name"], self._get_parquet_type(property["type"]))
            )
        return col_list

    def flush_records(
        self, records_to_load: Iterable[Dict], expected_row_count: Optional[int]
    ):
        num_written = 0
        for record in records_to_load:
            schema = pa.schema([("some_int", pa.int32()), ("some_string", pa.string())])
            writer = pq.ParquetWriter(self.get_config("filepath"), schema)
            table = pa.Table.from_batches([sample_batch])
            writer.write_table(table)
            for i in range(5):
                table = pa.Table.from_batches([_make_sample_batch()])
                writer.write_table(table)
            writer.close()
            num_written += 1
        if num_written != expected_row_count:
            self.logger.warning(
                f"Number of rows loaded ({num_written}) "
                f"did not match expected count ({expected_row_count})."
            )
