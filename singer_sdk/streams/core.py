"""Stream abstract class."""

import abc
import copy
import datetime
import json
import logging
from singer_sdk.mapper import SameRecordTransform, StreamMap
import requests
from types import MappingProxyType
from os import PathLike
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Any,
    List,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import pendulum
import singer
from singer import (
    metadata,
    RecordMessage,
    SchemaMessage,
)
from singer.catalog import Catalog
from singer.schema import Schema

from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.helpers._catalog import pop_deselected_record_properties

from singer_sdk.helpers._typing import (
    conform_record_data_types,
    is_datetime_type,
)
from singer_sdk.helpers._state import (
    get_writeable_state_dict,
    get_state_partitions_list,
    increment_state,
    finalize_state_progress_markers,
    reset_state_progress_markers,
    write_replication_key_signpost,
    log_sort_error,
)
from singer_sdk.exceptions import MaxRecordsLimitException, InvalidStreamSortException
from singer_sdk.helpers._compat import final
from singer_sdk.helpers._util import utc_now
from singer_sdk.helpers import _catalog


# Replication methods
REPLICATION_FULL_TABLE = "FULL_TABLE"
REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"

FactoryType = TypeVar("FactoryType", bound="Stream")

METRICS_LOG_LEVEL_SETTING = "metrics_log_level"


class Stream(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    STATE_MSG_FREQUENCY = 10000  # Number of records between state messages
    _MAX_RECORDS_LIMIT: Optional[int] = None

    # Used for nested stream relationships
    parent_stream_type: Optional[Type["Stream"]] = None
    ignore_parent_replication_key: bool = False

    def __init__(
        self,
        tap: TapBaseClass,
        schema: Optional[Union[str, PathLike, Dict[str, Any], Schema]] = None,
        name: Optional[str] = None,
    ):
        """Init tap stream."""
        if name:
            self.name: str = name
        if not self.name:
            raise ValueError("Missing argument or class variable 'name'.")

        self.logger: logging.Logger = tap.logger
        self.tap_name: str = tap.name
        self._config: dict = dict(tap.config)
        self._tap = tap
        self._tap_state = tap.state
        self._tap_input_catalog: Optional[dict] = None
        self._stream_maps: Optional[List[StreamMap]] = None
        self.forced_replication_method: Optional[str] = None
        self._replication_key: Optional[str] = None
        self._primary_keys: Optional[List[str]] = None
        self._state_partitioning_keys: Optional[List[str]] = None
        self._schema_filepath: Optional[Path] = None
        self._metadata: Optional[List[dict]] = None
        self._schema: dict
        self.child_streams: List[Stream] = []
        if schema:
            if isinstance(schema, (PathLike, str)):
                if not Path(schema).is_file():
                    raise FileExistsError(
                        f"Could not find schema file '{self.schema_filepath}'."
                    )

                self._schema_filepath = Path(schema)
            elif isinstance(schema, dict):
                self._schema = schema
            elif isinstance(schema, Schema):
                self._schema = schema.to_dict()
            else:
                raise ValueError(
                    f"Unexpected type {type(schema).__name__} for arg 'schema'."
                )

        if self.schema_filepath:
            self._schema = json.loads(Path(self.schema_filepath).read_text())

        if not self.schema:
            raise ValueError(
                f"Could not initialize schema for stream '{self.name}'. "
                "A valid schema object or filepath was not provided."
            )

    @property
    def stream_maps(self) -> List[StreamMap]:
        """Return a list of one or more map transformations for this stream.

        The 0th item is the primary stream map. List should not be empty.
        """
        if self._stream_maps:
            return self._stream_maps

        if self._tap.mapper:
            self._stream_maps = self._tap.mapper.stream_maps[self.name]
            self.logger.info(
                f"Tap has custom mapper. Using {len(self.stream_maps)} provided map(s)."
            )
        else:
            self.logger.info(
                f"No custom mapper provided for '{self.name}'. "
                "Using SameRecordTransform as default."
            )
            self._stream_maps = [
                SameRecordTransform(
                    stream_alias=self.name,
                    raw_schema=self.schema,
                    key_properties=self.primary_keys,
                )
            ]
        return self.stream_maps

    @property
    def is_timestamp_replication_key(self) -> bool:
        """Return True if the stream uses a timestamp-based replication key.

        Developers can override to `True` in order to force this value, although this
        should not be required in most use cases since the type can generally be
        accurately detected from the JSON Schema.
        """
        if not self.replication_key:
            return False
        type_dict = self.schema.get("properties", {}).get(self.replication_key)
        return is_datetime_type(type_dict)

    def get_starting_replication_key_value(
        self, context: Optional[dict]
    ) -> Optional[Any]:
        """Return starting replication key.

        Will return the value of the stream's replication key when `--state` is passed.
        If no prior state exists, will return `None`.

        Developers should use this method to seed incremental processing for
        non-datetime replication keys. For datetime and date replication keys, use
        `get_starting_timestamp()`
        """
        if self.is_timestamp_replication_key:
            return self.get_starting_timestamp(context)

        return self._starting_replication_key_value(context)

    def get_starting_timestamp(
        self, context: Optional[dict]
    ) -> Optional[datetime.datetime]:
        """Return `start_date` config, or state if using timestamp replication.

        Will return the value of the stream's replication key when `--state` is passed.
        If no state exists, will return `start_date` if set, or `None` if neither
        the stream state nor `start_date` is set.

        Developers should use this method to seed incremental processing for date
        and datetime replication keys. For non-datetime replication keys, use
        `get_starting_replication_key_value()`
        """
        if self.is_timestamp_replication_key:
            replication_key_value = self._starting_replication_key_value(context)
            if replication_key_value:
                return cast(datetime.datetime, pendulum.parse(replication_key_value))

        if "start_date" in self.config:
            return cast(datetime.datetime, pendulum.parse(self.config["start_date"]))

        return None

    def _starting_replication_key_value(self, context: Optional[dict]) -> Optional[Any]:
        """Return starting replication key."""
        if self.replication_key:
            state = self.get_context_state(context)
            replication_key_value = state.get("replication_key_value")
            if replication_key_value and self.replication_key == state.get(
                "replication_key"
            ):
                return replication_key_value

        return None

    @final
    @property
    def selected(self) -> bool:
        """Return true if the stream is selected."""
        return _catalog.is_property_selected(
            stream_name=self.name,
            metadata=self.metadata,
            breadcrumb=(),
            logger=self.logger,
        )

    @final
    @property
    def has_selected_descendents(self) -> bool:
        """Return True if any child streams are selected, recursively."""
        for child in self.child_streams or []:
            if child.selected or child.has_selected_descendents:
                return True

        return False

    @final
    @property
    def descendent_streams(self) -> List["Stream"]:
        """Return a list of all children recursively."""
        result: List[Stream] = list(self.child_streams) or []
        for child in self.child_streams:
            result += child.descendent_streams or []
        return result

    def _write_replication_key_signpost(
        self,
        context: Optional[dict],
        value: Union[datetime.datetime, str, int, float],
    ):
        """Write the signpost value, if available."""
        if not value:
            return

        state = self.get_context_state(context)
        write_replication_key_signpost(state, value)

    def get_replication_key_signpost(
        self, context: Optional[dict]
    ) -> Optional[Union[datetime.datetime, Any]]:
        """Return the max allowable bookmark value for this stream's replication key.

        For timestamp-based replication keys, this defaults to `utc_now()`. For
        non-timestamp replication keys, default to `None`. For consistency in subsequent
        calls, the value will be frozen (cached) at its initially called state, per
        partition argument if applicable.

        Developers may optionally override this method in advanced use cases such
        as unsorted incremental streams or complex hierarchical stream scenarios.
        For more info: https://sdk.meltano.com/en/latest/implementation/state.html
        """
        if self.is_timestamp_replication_key:
            return utc_now()

        return None

    @property
    def schema_filepath(self) -> Optional[Path]:
        """Return a path to a schema file for the stream or None if n/a."""
        return self._schema_filepath

    @property
    def schema(self) -> dict:
        """Return the schema dict for the stream."""
        return self._schema

    @property
    def primary_keys(self) -> Optional[List[str]]:
        """Return primary key(s) for the stream."""
        if not self._primary_keys:
            return None
        return self._primary_keys

    @primary_keys.setter
    def primary_keys(self, new_value: List[str]):
        """Set primary key(s) for the stream."""
        self._primary_keys = new_value

    @property
    def state_partitioning_keys(self) -> Optional[List[str]]:
        """Return partition keys for the stream state bookmarks.

        If not set, a default partitioning will be inherited from the stream's context.
        If an empty list is set (`[]`), state will be held in one bookmark per stream.
        """
        return self._state_partitioning_keys

    @state_partitioning_keys.setter
    def state_partitioning_keys(self, new_value: Optional[List[str]]) -> None:
        """Set partition keys for the stream state bookmarks.

        If not set, a default partitioning will be inherited from the stream's context.
        If an empty list is set (`[]`), state will be held in one bookmark per stream.
        """
        self._state_partitioning_keys = new_value

    @property
    def replication_key(self) -> Optional[str]:
        """Return replication key for the stream."""
        if not self._replication_key:
            return None
        return self._replication_key

    @replication_key.setter
    def replication_key(self, new_value: str) -> None:
        """Set replication key for the stream."""
        self._replication_key = new_value

    @property
    def is_sorted(self) -> bool:
        """Return `True` if stream is sorted. Defaults to `False`.

        When `True`, incremental streams will attempt to resume if unexpectedly
        interrupted.

        This setting enables additional checks which may trigger
        `InvalidStreamSortException` if records are found which are unsorted.
        """
        return False

    @property
    def metadata(self) -> List[dict]:
        """Return metadata object (dict) as specified in the Singer spec.

        Metadata from an input catalog will override standard metadata.
        """
        if self._metadata is not None:
            return self._metadata

        if self._tap_input_catalog:
            catalog = singer.Catalog.from_dict(self._tap_input_catalog)
            catalog_entry = catalog.get_stream(self.tap_stream_id)
            if catalog_entry:
                self._metadata = cast(List[dict], catalog_entry.metadata)
                return self._metadata

        self._metadata = cast(
            List[dict],
            metadata.get_standard_metadata(
                schema=self.schema,
                replication_method=self.forced_replication_method,
                key_properties=self.primary_keys or None,
                valid_replication_keys=(
                    [self.replication_key] if self.replication_key else None
                ),
                schema_name=None,
            ),
        )

        # If there's no input catalog, select all streams
        if not self._tap_input_catalog:
            for entry in self._metadata:
                if entry["breadcrumb"] == ():
                    entry["metadata"]["selected"] = True

        return self._metadata

    @property
    def _singer_catalog_entry(self) -> singer.CatalogEntry:
        """Return catalog entry as specified by the Singer catalog spec."""
        return singer.CatalogEntry(
            tap_stream_id=self.tap_stream_id,
            stream=self.name,
            schema=Schema.from_dict(self.schema),
            metadata=self.metadata,
            key_properties=self.primary_keys or None,
            replication_key=self.replication_key,
            replication_method=self.replication_method,
            is_view=None,
            database=None,
            table=None,
            row_count=None,
            stream_alias=None,
        )

    @property
    def _singer_catalog(self) -> singer.Catalog:
        return singer.Catalog([self._singer_catalog_entry])

    @property
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return MappingProxyType(self._config)

    @property
    def tap_stream_id(self) -> str:
        """Return a unique stream ID.

        Default implementations will return `self.name` but this behavior may be
        overridden if required by the developer.
        """
        return self.name

    @property
    def replication_method(self) -> str:
        """Return the replication method to be used."""
        if self.forced_replication_method:
            return str(self.forced_replication_method)
        if self.replication_key:
            return REPLICATION_INCREMENTAL
        return REPLICATION_FULL_TABLE

    # State properties:

    @property
    def tap_state(self) -> dict:
        """Return a writeable state dict for the entire tap.

        Note: This dictionary is shared (and writable) across all streams.

        This method is internal to the SDK and should not need to be overridden.
        Developers may access this property but this is not recommended except in
        advanced use cases. Instead, developers should access the latest stream
        replication key values using `Stream.get_starting_timestamp()` for timestamp
        keys, or `Stream.get_starting_replication_key_value()` for non-timestamp keys.
        """
        return self._tap_state

    def get_context_state(self, context: Optional[dict]) -> dict:
        """Return a writable state dict for the given context.

        Gives a partitioned context state if applicable; else returns stream state.
        A blank state will be created in none exists.

        This method is internal to the SDK and should not need to be overridden.
        Developers may access this property but this is not recommended except in
        advanced use cases. Instead, developers should access the latest stream
        replication key values using `Stream.get_starting_timestamp()` for timestamp
        keys, or `Stream.get_starting_replication_key_value()` for non-timestamp keys.

        Partition level may be overridden by Stream.state_partitioning_keys if set.
        """
        state_partition_context = self._get_state_partition_context(context)
        if state_partition_context:
            return get_writeable_state_dict(
                self.tap_state,
                self.name,
                state_partition_context=state_partition_context,
            )
        return self.stream_state

    @property
    def stream_state(self) -> dict:
        """Return a writeable state dict for this stream.

        This method is internal to the SDK and should not need to be overridden.
        Developers may access this property but this is not recommended except in
        advanced use cases. Instead, developers should access the latest stream
        replication key values using `Stream.get_starting_timestamp()` for timestamp
        keys, or `Stream.get_starting_replication_key_value()` for non-timestamp keys.

        A blank state entry will be created if one doesn't already exist.
        """
        return get_writeable_state_dict(self.tap_state, self.name)

    # Partitions

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.

        Developers may override this property to provide a default partitions list.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.
        """
        result: List[dict] = []
        for partition_state in (
            get_state_partitions_list(self.tap_state, self.name) or []
        ):
            result.append(partition_state["context"])
        return result or None

    # Private bookmarking methods

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, context: Optional[dict] = None
    ):
        """Update state of stream or partition with data from the provided record.

        Raises InvalidStreamSortException is self.is_sorted = True and unsorted data is
        detected.
        """
        state_dict = self.get_context_state(context)
        if latest_record:
            if self.replication_method in [
                REPLICATION_INCREMENTAL,
                REPLICATION_LOG_BASED,
            ]:
                if not self.replication_key:
                    raise ValueError(
                        f"Could not detect replication key for '{self.name}' stream"
                        f"(replication method={self.replication_method})"
                    )
                treat_as_sorted = self.is_sorted
                if not treat_as_sorted and self.state_partitioning_keys is not None:
                    # Streams with custom state partitioning are not resumable.
                    treat_as_sorted = False
                increment_state(
                    state_dict,
                    replication_key=self.replication_key,
                    latest_record=latest_record,
                    is_sorted=treat_as_sorted,
                )

    # Private message authoring methods:

    def _write_state_message(self):
        """Write out a STATE message with the latest state."""
        singer.write_message(singer.StateMessage(value=self.tap_state))

    def _write_schema_message(self):
        """Write out a SCHEMA message with the stream schema."""
        bookmark_keys = [self.replication_key] if self.replication_key else None
        for stream_map in self.stream_maps:
            schema_message = SchemaMessage(
                stream_map.stream_alias,
                stream_map.transformed_schema,
                self.primary_keys,
                bookmark_keys,
            )
            singer.write_message(schema_message)

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message."""
        pop_deselected_record_properties(
            record, self.schema, self.metadata, self.name, self.logger
        )
        record = conform_record_data_types(
            stream_name=self.name,
            row=record,
            schema=self.schema,
            logger=self.logger,
        )
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            # Emit record if not filtered
            if mapped_record is not None:
                record_message = RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )
                singer.write_message(record_message)

    @property
    def _metric_logging_function(self) -> Optional[Callable]:
        if METRICS_LOG_LEVEL_SETTING not in self.config:
            return self.logger.info

        if self.config[METRICS_LOG_LEVEL_SETTING].upper() == "INFO":
            return self.logger.info

        if self.config[METRICS_LOG_LEVEL_SETTING].upper() == "DEBUG":
            return self.logger.debug

        if self.config[METRICS_LOG_LEVEL_SETTING].upper() == "NONE":
            return None

        assert False, (
            "Unexpected logging level for metrics: "
            + self.config[METRICS_LOG_LEVEL_SETTING]
        )

    def _write_metric_log(self, metric: dict, extra_tags: Optional[dict]) -> None:
        """Emit a metric log. Optionally with appended tag info."""
        if not self._metric_logging_function:
            return None

        if extra_tags:
            metric["tags"].update(extra_tags)
        self._metric_logging_function(f"INFO METRIC: {str(metric)}")

    def _write_record_count_log(self, record_count, context: Optional[dict]) -> None:
        """Emit a metric log. Optionally with appended tag info."""
        extra_tags = {} if not context else {"context": context}
        counter_metric: Dict[str, Any] = {
            "type": "counter",
            "metric": "record_count",
            "value": record_count,
            "tags": {"stream": self.name},
        }
        self._write_metric_log(counter_metric, extra_tags=extra_tags)

    def _write_request_duration_log(
        self,
        endpoint: str,
        response: requests.Response,
        context: Optional[dict],
        extra_tags: Optional[dict],
    ) -> None:
        request_duration_metric: Dict[str, Any] = {
            "type": "timer",
            "metric": "http_request_duration",
            "value": response.elapsed.total_seconds(),
            "tags": {
                "endpoint": endpoint,
                "http_status_code": response.status_code,
                "status": "succeeded" if response.status_code < 400 else "failed",
            },
        }
        extra_tags = extra_tags or {}
        if context:
            extra_tags["context"] = context
        self._write_metric_log(metric=request_duration_metric, extra_tags=extra_tags)

    def _check_max_record_limit(self, record_count) -> None:
        if (
            self._MAX_RECORDS_LIMIT is not None
            and record_count >= self._MAX_RECORDS_LIMIT
        ):
            raise MaxRecordsLimitException(
                "Stream prematurely aborted due to the stream's max record "
                f"limit ({self._MAX_RECORDS_LIMIT}) being reached."
            )

    # Handle interim stream state

    def reset_state_progress_markers(self, state: Optional[dict] = None) -> None:
        """Reset progress markers. If all=True, all state contexts will be set.

        This method is internal to the SDK and should not need to be overridden.
        """
        if state is None or state == {}:
            context: Optional[dict]
            for context in self.partitions or [{}]:
                context = context or None
                state = self.get_context_state(context)
                reset_state_progress_markers(state)
            return

        reset_state_progress_markers(state)

    def finalize_state_progress_markers(self, state: Optional[dict] = None) -> None:
        """Reset progress markers. If all=True, all state contexts will be finalized.

        This method is internal to the SDK and should not need to be overridden.

        If all=True and the stream has children, child streams will also be finalized.
        """
        if state is None or state == {}:
            for child_stream in self.child_streams or []:
                child_stream.finalize_state_progress_markers()

            context: Optional[dict]
            for context in self.partitions or [{}]:
                context = context or None
                state = self.get_context_state(context)
                finalize_state_progress_markers(state)
            return

        finalize_state_progress_markers(state)

    # Private sync methods:

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        """Sync records, emitting RECORD and STATE messages."""
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    self._sync_children(child_context)
                self._check_max_record_limit(record_count)
                if self.selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()

    # Public methods ("final", not recommended to be overridden)

    @final
    def sync(self, context: Optional[dict] = None):
        """Sync this stream.

        This method is internal to the SDK and should not need to be overridden.
        """
        msg = f"Beginning {self.replication_method.lower()} sync of '{self.name}'"
        if context:
            msg += f" with context: {context}"
        self.logger.info(f"{msg}...")

        # Use a replication signpost, if available
        signpost = self.get_replication_key_signpost(context)
        if signpost:
            self._write_replication_key_signpost(context, signpost)

        # Send a SCHEMA message to the downstream target:
        self._write_schema_message()
        # Sync the records themselves:
        self._sync_records(context)

    def _sync_children(self, child_context) -> None:
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                child_stream.sync(context=child_context)

    # Overridable Methods

    def apply_catalog(self, catalog_dict: dict) -> None:
        """Apply a catalog dict, updating any settings overridden within the catalog.

        Developers may override this method in order to introduce advanced catalog
        parsing, or to explicitly fail on advanced catalog customizations which
        are not supported by the tap.
        """
        self._tap_input_catalog = catalog_dict

        catalog = Catalog.from_dict(catalog_dict)
        catalog_entry: singer.CatalogEntry = catalog.get_stream(self.name)
        if catalog_entry:
            self.primary_keys = catalog_entry.key_properties
            self.replication_key = catalog_entry.replication_key
            if catalog_entry.replication_method:
                self.forced_replication_method = catalog_entry.replication_method

    def _get_state_partition_context(self, context: Optional[dict]) -> Optional[Dict]:
        """Override state handling if Stream.state_partitioning_keys is specified."""
        if context is None:
            return None

        if self.state_partitioning_keys is None:
            return context

        return {k: v for k, v in context.items() if k in self.state_partitioning_keys}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.

        Developers may override this behavior to send specific information to child
        streams for context.
        """
        if context is None:
            for child_stream in self.child_streams:
                if child_stream.state_partitioning_keys is None:
                    parent_type = type(self).__name__
                    child_type = type(child_stream).__name__
                    raise NotImplementedError(
                        "No child context behavior was defined between parent stream "
                        f"'{self.name}' and child stream '{child_stream.name}'."
                        "The parent stream must define "
                        f"`{parent_type}.get_child_context()` and/or the child stream "
                        f"must define `{child_type}.state_partitioning_keys`."
                    )

        return context or record

    # Abstract Methods

    @abc.abstractmethod
    def get_records(
        self, context: Optional[dict]
    ) -> Iterable[Union[dict, Tuple[dict, dict]]]:
        """Abstract row generator function. Must be overridden by the child class.

        Each row emitted should be a dictionary of property names to their values.
        Returns either a record dict or a tuple: (record_dict, child_context)

        A method which should retrieve data from the source and return records
        incrementally using the python `yield` operator.

        Only custom stream types need to define this method. REST and GraphQL streams
        should instead use the class-specific methods for REST or GraphQL, respectively.

        This method takes an optional `context` argument, which can be safely ignored
        unless the stream is a child stream or requires partitioning.
        More info: https://sdk.meltano.com/en/latest/partitioning.html

        Parent streams can optionally return a tuple, in which
        case the second item in the tuple being a `child_context` dictionary for the
        stream's `context`.
        More info: https://sdk.meltano.com/en/latest/parent_streams.html
        """
        pass

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.
        """
        return row
