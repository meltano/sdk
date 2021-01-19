"""TargetBase abstract class."""

import abc
import copy
import json

import click
from singer_sdk.helpers import classproperty
from singer_sdk import helpers

from typing import Any, Dict, Iterable, Optional, Type

from singer_sdk.plugin_base import PluginBase
from singer_sdk.target_sink_base import TargetSinkBase


class TargetBase(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for targets."""

    # Constructor

    default_sink_class: Type[TargetSinkBase]
    _sinks: Dict[str, TargetSinkBase] = {}

    def __init__(self, config: Optional[Dict[str, Any]] = None,) -> None:
        """Initialize the tap."""
        self.logger.info(f"Initializing '{self.name}' target...")
        self._state = {}
        self._flushed_state = {}
        self._schemas = {}
        super().__init__(config=config)

    def get_sink(self, stream_name: str) -> TargetSinkBase:
        if stream_name in self._sinks:
            return self._sinks[stream_name]
        raise RuntimeError(
            "Attempted to retrieve stream before initialization. "
            "Please check that the upstream tap has sent the proper SCHEMA message."
        )

    def sink_exists(self, stream_name: str) -> bool:
        return stream_name in self._sinks

    def init_sink(self, stream_name: str, schema: dict) -> TargetSinkBase:
        self.logger.info(f"Initializing '{self.name}' target sink...")
        self._sinks[stream_name] = self.default_sink_class(
            target=self, stream_name=stream_name, schema=schema
        )
        return self._sinks[stream_name]

    def process_lines(self, lines: Iterable[str], table_cache=None) -> None:
        self.logger.info(f"Target '{self.name}' is listening for input from tap.")
        line_counter = 0
        record_counter = 0
        state_counter = 0
        for line in lines:
            line_counter += 1
            try:
                line_dict = json.loads(line)
            except json.decoder.JSONDecodeError:
                self.logger.error("Unable to parse:\n{}".format(line))
                raise
            if "type" not in line_dict:
                raise Exception("Line is missing required key 'type': {}".format(line))
            record_type = line_dict["type"]
            if record_type == "SCHEMA":
                self.process_schema_message(line_dict)
            elif record_type == "RECORD":
                self.process_record_message(line_dict)
                record_counter += 1
            elif record_type == "ACTIVATE_VERSION":
                self.process_activate_version_message(line_dict)
            elif record_type == "STATE":
                self.process_state_message(line_dict)
                state_counter += 1
            else:
                raise Exception(f"Unknown message type {record_type} in message {o}")
        self.logger.info(
            f"Target '{self.name}' completed after {line_counter} lines of input "
            f"({record_counter} records, {state_counter} state messages)."
        )

    def process_record_message(self, message_dict: dict) -> None:
        if "stream" not in message_dict:
            raise Exception(f"Line is missing required key 'stream': {message_dict}")
        stream_name = message_dict["stream"]
        if not self.sink_exists(stream_name):
            raise Exception(
                f"A record for stream '{stream_name}' was encountered before a "
                "corresponding schema."
            )
        stream = self.get_sink(stream_name)
        record = message_dict["record"]
        stream.process_record(record, message_dict)
        if (
            stream._num_records_cached
            >= self.default_sink_class.DEFAULT_BATCH_SIZE_ROWS
        ):
            # flush all streams, delete records if needed, reset counts and then emit current state
            if self.get_config("flush_all_streams"):
                streams_to_flush = self._sinks
            else:
                streams_to_flush = [stream]
            for stream in streams_to_flush:
                stream.flush_all()

    def process_schema_message(self, message_dict: dict) -> None:
        if "stream" not in message_dict:
            raise Exception(f"Line is missing required key 'stream': {message_dict}")
        if "schema" not in message_dict:
            raise Exception(f"Line is missing required key 'schema': {message_dict}")

        stream_name = message_dict["stream"]
        new_schema = helpers._float_to_decimal(message_dict["schema"])
        new_schema = helpers._float_to_decimal(message_dict["schema"])

        # Update and flush only if the the schema is new or different than
        # the previously used version of the schema
        if stream_name not in self._schemas:
            self.init_sink(stream_name, new_schema)
        else:
            sink = self.get_sink(stream_name)
            prev_schema = sink.schema
            if prev_schema != new_schema:
                # flush records from previous stream SCHEMA
                # if same stream has been encountered again, it means the schema might have been altered
                # so previous records need to be flushed
                sink.flush_records()
                if self._row_count.get(stream_name, 0) > 0:
                    self.flushed_state = self.flush_sinks(self._stream_to_sync)

                    # emit latest encountered state
                    self.emit_state(self.flushed_state)

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
                        "Primary key is set to mandatory but not defined in "
                        f"the [{stream_name}] stream"
                    )
                    raise Exception("key_properties field is required")

                self._key_properties[stream_name] = message_dict["key_properties"]

                if self.get_config("add_metadata_columns") or self.get_config(
                    "hard_delete"
                ):
                    stream_to_sync[stream_name] = DbSync(
                        config,
                        add_metadata_columns_to_schema(message_dict),
                        table_cache,
                    )
                else:
                    stream_to_sync[stream_name] = DbSync(
                        config, message_dict, table_cache
                    )

                stream_to_sync[stream_name].create_schema_if_not_exists()
                stream_to_sync[stream_name].sync_table()

                self._row_count[stream_name] = 0
                self._total_row_count[stream_name] = 0

    # pylint: disable=too-many-arguments
    def flush_sinks(stream_to_sync, filter_streams=None):
        """
        Flushes all buckets and resets records count to 0 as well as empties records to load list

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
        self.logger.debug(f"Setting state to {message_dict['value']}")
        state = message_dict["value"]
        # Initially set flushed state
        sink = self.get_sink(message_dict["stream"])
        sink.flush_all()
        if not self._flushed_state:
            self._flushed_state = copy.deepcopy(state)

    def handle_cli_args(self, args, cwd, environ) -> None:
        """Take necessary action in response to a CLI command."""
        pass

    @classproperty
    def cli(cls):
        """Execute standard CLI handler for taps."""

        @click.option("--version", is_flag=True)
        @click.option("--about", is_flag=True)
        @click.option("--format")
        @click.option("--config")
        @click.command()
        def cli(
            version: bool = False,
            about: bool = False,
            discover: bool = False,
            config: str = None,
            state: str = None,
            catalog: str = None,
            format: str = None,
        ):
            """Handle command line execution."""
            if version:
                cls.print_version()
                return
            if about:
                cls.print_about(format)
                return
            target = cls(config=config, state=state)
            target.process_lines()

        return cli
