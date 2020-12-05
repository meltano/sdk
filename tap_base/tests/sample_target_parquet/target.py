"""Sample target test for target-parquet."""

import copy
import json
import sys

from jsonschema import Draft4Validator, FormatChecker
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pyarrow as pa
import pyarrow.parquet as pq
import singer

# Reuse the tap connection rather than create a new target connection:
from tap_base.tests.sample_tap_parquet.connection import SampleParquetConnection

from tap_base.target_base import TargetBase


PLUGIN_NAME = "sample-target-parquet"
PLUGIN_VERSION_FILE = "./VERSION"
PLUGIN_CAPABILITIES = [
    "sync",
    "catalog",
    "discover",
    "state",
]
ACCEPTED_CONFIG = ["filepath"]
REQUIRED_CONFIG_SETS = [["filepath"]]


class SampleTargetParquet(TargetBase):
    """Sample target for Parquet."""

    _conn: SampleParquetConnection
    _schemas: Dict[str, Dict]
    _validators: Dict[str, Draft4Validator]
    _key_properties: Dict[str, List[str]] = {}
    _records_to_load: Dict[str, Dict[str, Any]] = {}
    _row_count: Dict[str, int] = {}
    _stream_to_sync: Dict[str, Any] = {}
    _total_row_count: Dict[str, int] = {}

    _state = None
    _flushed_state = None

    def __init__(self, config: dict, state: dict = None) -> None:
        """Initialize the target."""
        vers = Path(PLUGIN_VERSION_FILE).read_text()
        super().__init__(
            plugin_name=PLUGIN_NAME,
            version=vers,
            capabilities=PLUGIN_CAPABILITIES,
            accepted_options=ACCEPTED_CONFIG,
            option_set_requirements=REQUIRED_CONFIG_SETS,
            connection_class=SampleParquetConnection,
            config=config,
        )

    def validate_record(self, stream, record_dict: dict) -> dict:
        adjust_timestamps_in_record(record_dict, self._schemas[stream])
        if self.get_config("validate_records"):
            try:
                self._validators[stream].validate(float_to_decimal(record_dict))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    raise InvalidValidationOperationException(
                        "Data validation failed and cannot load to destination. "
                        f"RECORD: {record_dict}\n"
                        "multipleOf validations that allows long precisions are not "
                        "supported (i.e. with 15 digits "
                        "or more) Try removing 'multipleOf' methods from JSON schema."
                    )
                raise RecordValidationException(
                    f"Record does not pass schema validation. RECORD: {record_dict}"
                )
        return dict

    def process_record_message(self, message_dict: dict) -> None:
        if "stream" not in message_dict:
            raise Exception(f"Line is missing required key 'stream': {message_dict}")
        stream = message_dict["stream"]
        if stream not in self._schemas:
            raise Exception(
                f"A record for stream '{stream}' was encountered before a "
                "corresponding schema."
            )
        # Get schema for this record's stream
        record = message_dict["record"]
        # Validate record
        record = self.validate_record(stream, record)
        primary_key_string = stream_to_sync[stream].record_primary_key_string(record)
        if not primary_key_string:
            primary_key_string = "RID-{}".format(self._total_row_count[stream])
        if stream not in self._records_to_load:
            self._records_to_load[stream] = {}
        # increment row count only when a new PK is encountered in the current batch
        if primary_key_string not in self._records_to_load[stream]:
            self._row_count[stream] += 1
            self._total_row_count[stream] += 1
        # append record
        if self.get_config("add_metadata_columns") or self.get_config("hard_delete"):
            self._records_to_load[stream][
                primary_key_string
            ] = self.add_metadata_values_to_record(
                message_dict, self._stream_to_sync[stream]
            )
        else:
            self._records_to_load[stream][primary_key_string] = record
        if self._row_count[stream] >= batch_size_rows:
            # flush all streams, delete records if needed, reset counts and then emit current state
            if self.get_config("flush_all_streams"):
                filter_streams = None
            else:
                filter_streams = [stream]
            # Flush and return a new state dict with new positions only for the flushed streams
            flushed_state = self.flush_streams(
                self._records_to_load,
                self._row_count,
                self._stream_to_sync,
                self._config,
                self._state,
                self._flushed_state,
                filter_streams=filter_streams,
            )
            # emit last encountered state
            self.emit_state(copy.deepcopy(flushed_state))

    def emit_state(self, state):
        if state is not None:
            line = json.dumps(state)
            self.logger.info(f"Emitting state {line}")
            sys.stdout.write(f"{line}\n")
            sys.stdout.flush()

    def process_schema_message(self, message_dict: dict) -> None:
        if "stream" not in message_dict:
            raise Exception(f"Line is missing required key 'stream': {message_dict}")

        stream = message_dict["stream"]
        new_schema = float_to_decimal(message_dict["schema"])

        # Update and flush only if the the schema is new or different than
        # the previously used version of the schema
        if stream not in self._schemas or self._schemas[stream] != new_schema:
            self._schemas[stream] = new_schema
            self._validators[stream] = Draft4Validator(
                self._schemas[stream], format_checker=FormatChecker()
            )

            # flush records from previous stream SCHEMA
            # if same stream has been encountered again, it means the schema might have been altered
            # so previous records need to be flushed
            if self._row_count.get(stream, 0) > 0:
                flushed_state = self.flush_streams(
                    self._records_to_load,
                    self._row_count,
                    self._stream_to_sync,
                    self._config,
                    self._state,
                    self._flushed_state,
                )

                # emit latest encountered state
                self.emit_state(flushed_state)

            # key_properties key must be available in the SCHEMA message.
            if "key_properties" not in message_dict:
                raise Exception("key_properties field is required")

            # Log based and Incremental replications on tables with no Primary Key
            # cause duplicates when merging UPDATE events.
            # Stop loading data by default if no Primary Key.
            #
            # If you want to load tables with no Primary Key:
            #  1) Set ` 'primary_key_required': false ` in the target-snowflake config.json
            #  or
            #  2) Use fastsync [postgres-to-snowflake, mysql-to-snowflake, etc.]
            if (
                self.get_config("primary_key_required", True)
                and len(message_dict["key_properties"]) == 0
            ):
                self.logger.critical(
                    "Primary key is set to mandatory but not defined in the [{}] stream".format(
                        stream
                    )
                )
                raise Exception("key_properties field is required")

            self._key_properties[stream] = message_dict["key_properties"]

            if self.get_config("add_metadata_columns") or self.get_config(
                "hard_delete"
            ):
                stream_to_sync[stream] = DbSync(
                    config, add_metadata_columns_to_schema(message_dict), table_cache
                )
            else:
                stream_to_sync[stream] = DbSync(config, message_dict, table_cache)

            stream_to_sync[stream].create_schema_if_not_exists()
            stream_to_sync[stream].sync_table()

            self._row_count[stream] = 0
            self._total_row_count[stream] = 0

    # pylint: disable=too-many-arguments
    def flush_streams(
        stream_to_sync, filter_streams=None,
    ):
        """
        Flushes all buckets and resets records count to 0 as well as empties records to load list
        :param streams: dictionary with records to load per stream
        :param row_count: dictionary with row count per stream
        :param stream_to_sync: Snowflake db sync instance per stream
        :param config: dictionary containing the configuration
        :param state: dictionary containing the original state from tap
        :param flushed_state: dictionary containing updated states only when streams got flushed
        :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
        :return: State dict with flushed positions
        """
        parallelism = self.get_config("parallelism", DEFAULT_PARALLELISM)
        max_parallelism = self.get_config("max_parallelism", DEFAULT_MAX_PARALLELISM)

        # Parallelism 0 means auto parallelism:
        #
        # Auto parallelism trying to flush streams efficiently with auto defined number
        # of threads where the number of threads is the number of streams that need to
        # be loaded but it's not greater than the value of max_parallelism
        if parallelism == 0:
            n_streams_to_flush = len(streams.keys())
            if n_streams_to_flush > max_parallelism:
                parallelism = max_parallelism
            else:
                parallelism = n_streams_to_flush

        # Select the required streams to flush
        if filter_streams:
            streams_to_flush = filter_streams
        else:
            streams_to_flush = streams.keys()

        # Single-host, thread-based parallelism
        with parallel_backend("threading", n_jobs=parallelism):
            Parallel()(
                delayed(load_stream_batch)(
                    stream=stream,
                    records_to_load=streams[stream],
                    row_count=row_count,
                    db_sync=stream_to_sync[stream],
                    no_compression=self.get_config("no_compression"),
                    delete_rows=self.get_config("hard_delete"),
                    temp_dir=self.get_config("temp_dir"),
                )
                for stream in streams_to_flush
            )

        # reset flushed stream records to empty to avoid flushing same records
        for stream in streams_to_flush:
            streams[stream] = {}

            # Update flushed streams
            if filter_streams:
                # update flushed_state position if we have state information for the stream
                if state is not None and stream in state.get("bookmarks", {}):
                    # Create bookmark key if not exists
                    if "bookmarks" not in flushed_state:
                        flushed_state["bookmarks"] = {}
                    # Copy the stream bookmark from the latest state
                    flushed_state["bookmarks"][stream] = copy.deepcopy(
                        state["bookmarks"][stream]
                    )

            # If we flush every bucket use the latest state
            else:
                flushed_state = copy.deepcopy(state)

        # Return with state message with flushed positions
        return flushed_state

    def process_activate_version_message(self, message_dict: dict) -> None:
        self.logger.debug("ACTIVATE_VERSION message")

    def process_state_message(self, message_dict: dict) -> None:
        self.logger.debug("Setting state to {}".format(message_dict["value"]))
        state = message_dict["value"]

        # Initially set flushed state
        if not flushed_state:
            flushed_state = copy.deepcopy(state)

    def process_lines(self, lines: Iterable[str], table_cache=None) -> None:
        schema = pa.schema([("some_int", pa.int32()), ("some_string", pa.string())])
        writer = pq.ParquetWriter(self.get_config("filepath"), schema)
        for line in lines:
            try:
                o = json.loads(line)
            except json.decoder.JSONDecodeError:
                self.logger.error("Unable to parse:\n{}".format(line))
                raise

            if "type" not in o:
                raise Exception("Line is missing required key 'type': {}".format(line))

            t = o["type"]

            if t == "RECORD":
                self.process_record_message(line)
            elif t == "SCHEMA":
                self.process_schema_message(line)
            elif t == "ACTIVATE_VERSION":
                self.process_activate_version_message(line)
            elif t == "STATE":
                self.process_state_message(line)
            else:
                raise Exception(f"Unknown message type {o['type']} in message {o}")

            table = pa.Table.from_batches([sample_batch])
            writer.write_table(table)
            for i in range(5):
                table = pa.Table.from_batches([_make_sample_batch()])
                writer.write_table(table)
            writer.close()
