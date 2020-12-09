"""Sample Snowflake target stream class, which handles writing streams."""


from logging import FileHandler
from typing import BinaryIO, Callable, Dict, Iterable, Optional
import os
import gzip
import json

from tempfile import mkstemp
from decimal import Decimal

from tap_base.tests.sample_tap_snowflake.connection import SampleSnowflakeConnection

from tap_base.target_stream_base import TargetStreamBase


class RecordValidationException(Exception):
    """Exception to raise when record validation failed."""

    pass


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed."""

    pass


class SampleSnowflakeTargetStream(TargetStreamBase):

    DEFAULT_BATCH_SIZE_ROWS = 100000
    DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
    DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel

    DATETIME_ERROR_TREATMENT = "MAX"

    _conn: SampleSnowflakeConnection

    def validate_record(self, record: Dict) -> Dict:
        """Calls the base class validator, then runs snowflake-specific validation."""
        super().validate_record(record)
        if self._conn.get_config("validate_records"):
            try:
                self._validators[self.stream_name].validate(_float_to_decimal(record))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    raise InvalidValidationOperationException(
                        "Data validation failed and cannot load to destination. "
                        f"RECORD: {record}\n"
                        "multipleOf validations that allows long precisions are not "
                        "supported (i.e. with 15 digits "
                        "or more) Try removing 'multipleOf' methods from JSON schema."
                    )
                raise RecordValidationException(
                    f"Record does not pass schema validation. RECORD: {record}"
                )

    def flush_records(
        self, records_to_load: Iterable[Dict], expected_row_count: Optional[int]
    ) -> dict:
        temp_dir = self.get_config("temp_dir")
        no_compression = self.get_config("no_compression")
        retain_s3_files = self.get_config("retain_s3_files")

        if temp_dir:
            os.makedirs(temp_dir, exist_ok=True)
        # Using gzip or plain file object
        if no_compression:
            csv_fd, csv_file = mkstemp(suffix=".csv", prefix="records_", dir=temp_dir)
            with open(csv_fd, "wb") as outfile:
                self.write_record_to_file(
                    outfile, records_to_load, self._record_to_csv_line
                )
        else:
            csv_fd, csv_file = mkstemp(
                suffix=".csv.gz", prefix="records_", dir=temp_dir
            )
            with gzip.open(csv_file, "wb") as outfile:
                self.write_record_to_file(
                    outfile, records_to_load, self._record_to_csv_line
                )
        size_bytes = os.path.getsize(csv_file)
        s3_key = self._put_to_stage(
            csv_file, stream, expected_row_count, temp_dir=temp_dir
        )
        self._load_csv(self.conn, s3_key, expected_row_count, size_bytes)
        os.close(csv_fd)
        os.remove(csv_file)
        if not retain_s3_files:
            self._delete_from_stage(s3_key)
        return self._cached_state

    def _record_to_csv_line(self, record: Dict) -> str:
        if self.flattener:
            flattened = self.flattener.flatten_record(
                record, self.flatten_schema, max_level=self.data_flattening_max_level
            )
        return ",".join(
            [
                json.dumps(flattened[name], ensure_ascii=False)
                if name in flattened and (flattened[name] == 0 or flattened[name])
                else ""
                for name in self._flatten_record
            ]
        )

    def write_record_to_file(
        self, outfile: BinaryIO, records: Iterable[Dict], tranformer_fn: Callable
    ):
        raise NotImplementedError()

    def _put_to_stage(
        self,
        conn: SampleSnowflakeConnection,
        csv_file: str,
        stream_name: str,
        expected_row_count: Optional[int],
        temp_dir: str,
    ) -> str:
        raise NotImplementedError()

    def _delete_from_stage(self, conn: SampleSnowflakeConnection, s3_key: str):
        raise NotImplementedError()

    def _load_csv(
        self, conn: SampleSnowflakeConnection, s3_key, expected_row_count, size_bytes
    ):
        raise NotImplementedError()
