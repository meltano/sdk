"""Target abstract class."""

import abc
import copy
import json
import sys

import click

from joblib import Parallel, parallel_backend, delayed
from typing import Any, Dict, Iterable, Optional, Type, List

import singer

from singer_sdk.exceptions import RecordsWitoutSchemaException
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import final
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import Sink

_MAX_PARALLELISM = 8


class Target(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for targets."""

    _DRAIN_AFTER_STATE_MESSAGES: bool = True

    # Constructor

    default_sink_class: Type[Sink]

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        parse_env_config: bool = False,
    ) -> None:
        """Initialize the target."""
        super().__init__(config=config, parse_env_config=parse_env_config)

        self._latest_state: Dict[str, dict] = {}
        self._drained_state: Dict[str, dict] = {}
        self._sinks_active: Dict[str, Sink] = {}
        self._sinks_to_clear: List[Sink] = []

    @classproperty
    def capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        return ["target"]

    @property
    def max_parallelism(self) -> int:
        """Return max number of sinks that can be drained in parallel."""
        return _MAX_PARALLELISM

    def get_sink(
        self,
        stream_name: str,
        *,
        record: Optional[dict] = None,
        schema: Optional[dict] = None,
        key_properties: Optional[List[str]] = None,
    ) -> Sink:
        """Return a sink for the given stream name.

        If schema is provided, a new sink will be created. If the sink already existed,
        the old sink becomes archived and held until the next drain_all() operation.

        :raises: RecordsWitoutSchemaException if sink does not exist and schema is not
                 sent.
        """
        if schema:
            if self.sink_exists(stream_name):
                self._sinks_to_clear.append(self._sinks_active.pop(stream_name))

            return self.add_sink(stream_name, schema, key_properties)

        self._assert_sink_exists(stream_name)
        return self._sinks_active[stream_name]

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        """Return a sink for the given stream name.

        :raises: ValueError if no Sink class is defined.
        """
        if self.default_sink_class:
            return self.default_sink_class

        raise ValueError(
            f"No sink class defined for '{stream_name}' "
            "and no default sink class available."
        )

    def sink_exists(self, stream_name: str) -> bool:
        """Return True if a sink has been initialized."""
        return stream_name in self._sinks_active

    @final
    def listen(self) -> None:
        """Read from STDIN until all messages are processed."""
        self._process_lines(sys.stdin)

    @final
    def add_sink(
        self, stream_name: str, schema: dict, key_properties: Optional[List[str]] = None
    ) -> Sink:
        """Create a sink and register it."""
        self.logger.info(f"Initializing '{self.name}' target sink...")
        result = self.default_sink_class(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )
        self._sinks_active[stream_name] = result
        return result

    @staticmethod
    def _assert_line_requires(line_dict: dict, requires: List[str]) -> None:
        for required in requires:
            if required not in line_dict:
                raise Exception(
                    f"Line is missing required '{required}' key: {line_dict}"
                )

    def _assert_sink_exists(self, stream_name) -> None:
        """Raise a RecordsWitoutSchemaException exception if stream doesn't exist."""
        if not self.sink_exists(stream_name):
            raise RecordsWitoutSchemaException(
                f"A record for stream '{stream_name}' was encountered before a "
                "corresponding schema."
            )

    # Message handling

    def _process_lines(self, lines: Iterable[str], table_cache=None) -> None:
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

            self._assert_line_requires(line_dict, requires=["type"])

            record_type = line_dict["type"]
            if record_type == "SCHEMA":
                self._process_schema_message(line_dict)
                continue

            if record_type == "RECORD":
                self._process_record_message(line_dict)
                record_counter += 1
                continue

            if record_type == "ACTIVATE_VERSION":
                self._process_activate_version_message(line_dict)
                continue

            if record_type == "STATE":
                self._process_state_message(line_dict)
                state_counter += 1
                continue

            raise Exception(f"Unknown message type '{record_type}' in message.")

        self.drain_all()
        self.logger.info(
            f"Target '{self.name}' completed after {line_counter} lines of input "
            f"({record_counter} records, {state_counter} state messages)."
        )

    def _process_record_message(self, message_dict: dict) -> None:
        """Process a RECORD message."""
        self._assert_line_requires(message_dict, requires=["stream", "record"])

        stream_name = message_dict["stream"]
        record = message_dict["record"]
        sink = self.get_sink(stream_name, record=record)
        sink._validate_record(record)
        if sink.include_sdc_metadata_properties:
            sink._add_metadata_values_to_record(record, message_dict)
        record = sink.preprocess_record(record)
        sink.load_record(record)
        if sink.is_full:
            self.logger.info(
                f"Target sink for '{sink.stream_name}' is full. Draining..."
            )
            self.drain_one(sink)

    def _process_schema_message(self, message_dict: dict) -> None:
        """Process a SCHEMA messages."""
        self._assert_line_requires(message_dict, requires=["stream", "schema"])

        stream_name = message_dict["stream"]
        # new_schema = helpers._float_to_decimal(new_schema)
        _ = self.get_sink(
            stream_name,
            schema=message_dict["schema"],
            key_properties=message_dict.get("key_properties", None),
        )

    def _process_state_message(self, message_dict: dict) -> None:
        """Process a state message. drain sinks if needed."""
        self._assert_line_requires(message_dict, requires=["value"])
        state = message_dict["value"]
        self._latest_state = state
        if self._DRAIN_AFTER_STATE_MESSAGES:
            self.drain_all()

    def _process_activate_version_message(self, message_dict: dict) -> None:
        """Handle the optional ACTIVATE_VERSION message extension."""
        self.logger.warning(
            "ACTIVATE_VERSION message received but not supported. Ingnoring."
        )

    # Sink drain methods

    @final
    def drain_all(self) -> None:
        """Drains all sinks, starting with those cleared due to changed schema."""
        state = copy.deepcopy(self._latest_state)
        self._drain_all(self._sinks_to_clear, 1)
        self._sinks_to_clear = []
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        self._write_state_message(state)

    @final
    def drain_one(self, sink: Sink) -> None:
        """Drain a specific sink."""
        if sink.current_size == 0:
            return

        draining = sink.start_drain()
        sink.drain(draining)
        sink.mark_drained()

    def _drain_all(self, sink_list: List[Sink], parallelism: int) -> None:
        def _drain_sink(sink: Sink):
            self.drain_one(sink)

        with parallel_backend("threading", n_jobs=parallelism):
            Parallel()(delayed(_drain_sink)(sink=sink) for sink in sink_list)

    def _write_state_message(self, state: dict):
        """Emit the stream's latest state."""
        self.logger.info(f"Emitting completed target state {state}")
        singer.write_message(singer.StateMessage(state))

    # CLI handler

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
            config: str = None,
            format: str = None,
        ):
            """Handle command line execution."""
            if version:
                cls.print_version()
                return

            if about:
                cls.print_about(format)
                return

            target = cls(config=config)
            target.listen()

        return cli
