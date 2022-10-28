"""Stream abstract class."""

from __future__ import annotations

import abc
import copy
import datetime
import gzip
import itertools
import json
import logging
from os import PathLike
from pathlib import Path
from types import MappingProxyType
from typing import Any, Generator, Iterable, Iterator, Mapping, TypeVar, cast
from uuid import uuid4

import pendulum

import singer_sdk._singerlib as singer
from singer_sdk import metrics
from singer_sdk.exceptions import InvalidStreamSortException, MaxRecordsLimitException
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    SDKBatchMessage,
)
from singer_sdk.helpers._catalog import pop_deselected_record_properties
from singer_sdk.helpers._compat import final
from singer_sdk.helpers._flattening import get_flattening_options
from singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    get_starting_replication_value,
    get_state_partitions_list,
    get_writeable_state_dict,
    increment_state,
    log_sort_error,
    reset_state_progress_markers,
    write_replication_key_signpost,
    write_starting_replication_value,
)
from singer_sdk.helpers._typing import conform_record_data_types, is_datetime_type
from singer_sdk.helpers._util import utc_now
from singer_sdk.mapper import RemoveRecordTransform, SameRecordTransform, StreamMap
from singer_sdk.plugin_base import PluginBase as TapBaseClass

# Replication methods
REPLICATION_FULL_TABLE = "FULL_TABLE"
REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"

FactoryType = TypeVar("FactoryType", bound="Stream")
_T = TypeVar("_T")


def lazy_chunked_generator(
    iterable: Iterable[_T],
    chunk_size: int,
) -> Generator[Iterator[_T], None, None]:
    """Yield a generator for each chunk of the given iterable.

    Args:
        iterable: The iterable to chunk.
        chunk_size: The size of each chunk.

    Yields:
        A generator for each chunk of the given iterable.
    """
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, chunk_size))
        if not chunk:
            break
        yield iter(chunk)


class Stream(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    STATE_MSG_FREQUENCY = 10000  # Number of records between state messages
    _MAX_RECORDS_LIMIT: int | None = None

    # Used for nested stream relationships
    parent_stream_type: type[Stream] | None = None
    ignore_parent_replication_key: bool = False

    # Internal API cost aggregator
    _sync_costs: dict[str, int] = {}

    # Batch attributes
    batch_size: int = 1000
    """Max number of records to write to each batch file."""

    def __init__(
        self,
        tap: TapBaseClass,
        schema: str | PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
    ) -> None:
        """Init tap stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.

        Raises:
            ValueError: TODO
            FileNotFoundError: TODO
        """
        if name:
            self.name: str = name
        if not self.name:
            raise ValueError("Missing argument or class variable 'name'.")

        self.logger: logging.Logger = tap.logger
        self.metrics_logger = tap.metrics_logger
        self.tap_name: str = tap.name
        self._config: dict = dict(tap.config)
        self._tap = tap
        self._tap_state = tap.state
        self._tap_input_catalog: singer.Catalog | None = None
        self._stream_maps: list[StreamMap] | None = None
        self.forced_replication_method: str | None = None
        self._replication_key: str | None = None
        self._primary_keys: list[str] | None = None
        self._state_partitioning_keys: list[str] | None = None
        self._schema_filepath: Path | None = None
        self._metadata: singer.MetadataMapping | None = None
        self._mask: singer.SelectionMask | None = None
        self._schema: dict
        self.child_streams: list[Stream] = []
        if schema:
            if isinstance(schema, (PathLike, str)):
                if not Path(schema).is_file():
                    raise FileNotFoundError(
                        f"Could not find schema file '{self.schema_filepath}'."
                    )

                self._schema_filepath = Path(schema)
            elif isinstance(schema, dict):
                self._schema = schema
            elif isinstance(schema, singer.Schema):
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
    def stream_maps(self) -> list[StreamMap]:
        """Get stream transformation maps.

        The 0th item is the primary stream map. List should not be empty.

        Returns:
            A list of one or more map transformations for this stream.
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
                "Using SameRecordTransform."
            )
            self._stream_maps = [
                SameRecordTransform(
                    stream_alias=self.name,
                    raw_schema=self.schema,
                    key_properties=self.primary_keys,
                    flattening_options=get_flattening_options(self.config),
                )
            ]
        return self._stream_maps

    @property
    def is_timestamp_replication_key(self) -> bool:
        """Check is replication key is a timestamp.

        Developers can override to `True` in order to force this value, although this
        should not be required in most use cases since the type can generally be
        accurately detected from the JSON Schema.

        Returns:
            True if the stream uses a timestamp-based replication key.
        """
        if not self.replication_key:
            return False
        type_dict = self.schema.get("properties", {}).get(self.replication_key)
        return is_datetime_type(type_dict)

    def get_starting_replication_key_value(self, context: dict | None) -> Any | None:
        """Get starting replication key.

        Will return the value of the stream's replication key when `--state` is passed.
        If no prior state exists, will return `None`.

        Developers should use this method to seed incremental processing for
        non-datetime replication keys. For datetime and date replication keys, use
        :meth:`~singer_sdk.Stream.get_starting_timestamp()`

        Args:
            context: Stream partition or context dictionary.

        Returns:
            Starting replication value.
        """
        state = self.get_context_state(context)

        return get_starting_replication_value(state)

    def get_starting_timestamp(self, context: dict | None) -> datetime.datetime | None:
        """Get starting replication timestamp.

        Will return the value of the stream's replication key when `--state` is passed.
        If no state exists, will return `start_date` if set, or `None` if neither
        the stream state nor `start_date` is set.

        Developers should use this method to seed incremental processing for date
        and datetime replication keys. For non-datetime replication keys, use
        :meth:`~singer_sdk.Stream.get_starting_replication_key_value()`

        Args:
            context: Stream partition or context dictionary.

        Returns:
            `start_date` from config, or state value if using timestamp replication.

        Raises:
            ValueError: If the replication value is not a valid timestamp.
        """
        value = self.get_starting_replication_key_value(context)

        if value is None:
            return None

        if not self.is_timestamp_replication_key:
            raise ValueError(
                f"The replication key {self.replication_key} is not of timestamp type"
            )

        return cast(datetime.datetime, pendulum.parse(value))

    @final
    @property
    def selected(self) -> bool:
        """Check if stream is selected.

        Returns:
            True if the stream is selected.
        """
        return self.mask.get((), True)

    @final
    @property
    def has_selected_descendents(self) -> bool:
        """Check descendents.

        Returns:
            True if any child streams are selected, recursively.
        """
        for child in self.child_streams or []:
            if child.selected or child.has_selected_descendents:
                return True

        return False

    @final
    @property
    def descendent_streams(self) -> list[Stream]:
        """Get child streams.

        Returns:
            A list of all children, recursively.
        """
        result: list[Stream] = list(self.child_streams) or []
        for child in self.child_streams:
            result += child.descendent_streams or []
        return result

    def _write_replication_key_signpost(
        self,
        context: dict | None,
        value: datetime.datetime | str | int | float,
    ) -> None:
        """Write the signpost value, if available.

        Args:
            context: Stream partition or context dictionary.
            value: TODO

        Returns:
            TODO
        """
        if not value:
            return

        state = self.get_context_state(context)
        write_replication_key_signpost(state, value)

    def compare_start_date(self, value: str, start_date_value: str) -> str:
        """Compare a bookmark value to a start date and return the most recent value.

        If the replication key is a datetime-formatted string, this method will parse
        the value and compare it to the start date. Otherwise, the bookmark value is
        returned.

        If the tap uses a non-datetime replication key (e.g. an UNIX timestamp), the
        developer is encouraged to override this method to provide custom logic for
        comparing the bookmark value to the start date.

        Args:
            value: The replication key value.
            start_date_value: The start date value from the config.

        Returns:
            The most recent value between the bookmark and start date.
        """
        if self.is_timestamp_replication_key:
            return max(value, start_date_value, key=pendulum.parse)
        else:
            return value

    def _write_starting_replication_value(self, context: dict | None) -> None:
        """Write the starting replication value, if available.

        Args:
            context: Stream partition or context dictionary.
        """
        value = None
        state = self.get_context_state(context)

        if self.replication_key:
            replication_key_value = state.get("replication_key_value")
            if replication_key_value and self.replication_key == state.get(
                "replication_key"
            ):
                value = replication_key_value

            # Use start_date if it is more recent than the replication_key state
            start_date_value: str | None = self.config.get("start_date")
            if start_date_value:
                if not value:
                    value = start_date_value
                else:
                    value = self.compare_start_date(value, start_date_value)

        write_starting_replication_value(state, value)

    def get_replication_key_signpost(
        self, context: dict | None
    ) -> datetime.datetime | Any | None:
        """Get the replication signpost.

        For timestamp-based replication keys, this defaults to `utc_now()`. For
        non-timestamp replication keys, default to `None`. For consistency in subsequent
        calls, the value will be frozen (cached) at its initially called state, per
        partition argument if applicable.

        Developers may optionally override this method in advanced use cases such
        as unsorted incremental streams or complex hierarchical stream scenarios.
        For more info: :doc:`/implementation/state`

        Args:
            context: Stream partition or context dictionary.

        Returns:
            Max allowable bookmark value for this stream's replication key.
        """
        if self.is_timestamp_replication_key:
            return utc_now()

        return None

    @property
    def schema_filepath(self) -> Path | None:
        """Get path to schema file.

        Returns:
            Path to a schema file for the stream or `None` if n/a.
        """
        return self._schema_filepath

    @property
    def schema(self) -> dict:
        """Get schema.

        Returns:
            JSON Schema dictionary for this stream.
        """
        return self._schema

    @property
    def primary_keys(self) -> list[str] | None:
        """Get primary keys.

        Returns:
            A list of primary key(s) for the stream.
        """
        if not self._primary_keys:
            return []
        return self._primary_keys

    @primary_keys.setter
    def primary_keys(self, new_value: list[str]) -> None:
        """Set primary key(s) for the stream.

        Args:
            new_value: TODO
        """
        self._primary_keys = new_value

    @property
    def state_partitioning_keys(self) -> list[str] | None:
        """Get state partition keys.

        If not set, a default partitioning will be inherited from the stream's context.
        If an empty list is set (`[]`), state will be held in one bookmark per stream.

        Returns:
            Partition keys for the stream state bookmarks.
        """
        return self._state_partitioning_keys

    @state_partitioning_keys.setter
    def state_partitioning_keys(self, new_value: list[str] | None) -> None:
        """Set partition keys for the stream state bookmarks.

        If not set, a default partitioning will be inherited from the stream's context.
        If an empty list is set (`[]`), state will be held in one bookmark per stream.

        Args:
            new_value: the new list of keys
        """
        self._state_partitioning_keys = new_value

    @property
    def replication_key(self) -> str | None:
        """Get replication key.

        Returns:
            Replication key for the stream.
        """
        if not self._replication_key:
            return None
        return self._replication_key

    @replication_key.setter
    def replication_key(self, new_value: str) -> None:
        """Set replication key for the stream.

        Args:
            new_value: TODO
        """
        self._replication_key = new_value

    @property
    def is_sorted(self) -> bool:
        """Expect stream to be sorted.

        When `True`, incremental streams will attempt to resume if unexpectedly
        interrupted.

        Returns:
            `True` if stream is sorted. Defaults to `False`.
        """
        return False

    @property
    def check_sorted(self) -> bool:
        """Check if stream is sorted.

        This setting enables additional checks which may trigger
        `InvalidStreamSortException` if records are found which are unsorted.

        Returns:
            `True` if sorting is checked. Defaults to `True`.
        """
        return True

    @property
    def metadata(self) -> singer.MetadataMapping:
        """Get stream metadata.

        Metadata attributes (`inclusion`, `selected`, etc.) are part of the Singer spec.

        Metadata from an input catalog will override standard metadata.

        Returns:
            A mapping from property breadcrumbs to metadata objects.
        """
        if self._metadata is not None:
            return self._metadata

        if self._tap_input_catalog:
            catalog_entry = self._tap_input_catalog.get_stream(self.tap_stream_id)
            if catalog_entry:
                self._metadata = catalog_entry.metadata
                return self._metadata

        self._metadata = singer.MetadataMapping.get_standard_metadata(
            schema=self.schema,
            replication_method=self.forced_replication_method,
            key_properties=self.primary_keys or [],
            valid_replication_keys=(
                [self.replication_key] if self.replication_key else None
            ),
            schema_name=None,
        )

        # If there's no input catalog, select all streams
        if self._tap_input_catalog is None:
            self._metadata.root.selected = True

        return self._metadata

    @property
    def _singer_catalog_entry(self) -> singer.CatalogEntry:
        """Return catalog entry as specified by the Singer catalog spec.

        Returns:
            TODO
        """
        return singer.CatalogEntry(
            tap_stream_id=self.tap_stream_id,
            stream=self.name,
            schema=singer.Schema.from_dict(self.schema),
            metadata=self.metadata,
            key_properties=self.primary_keys or [],
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
        """TODO.

        Returns:
            TODO
        """
        return singer.Catalog([(self.tap_stream_id, self._singer_catalog_entry)])

    @property
    def config(self) -> Mapping[str, Any]:
        """Get stream configuration.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        return MappingProxyType(self._config)

    @property
    def tap_stream_id(self) -> str:
        """Return a unique stream ID.

        Default implementations will return `self.name` but this behavior may be
        overridden if required by the developer.

        Returns:
            Unique stream ID.
        """
        return self.name

    @property
    def replication_method(self) -> str:
        """Get replication method.

        Returns:
            Replication method to be used for this stream.
        """
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
        replication key values using :meth:`~singer_sdk.Stream.get_starting_timestamp()`
        for timestamp keys, or
        :meth:`~singer_sdk.Stream.get_starting_replication_key_value()` for
        non-timestamp keys.

        Returns:
            A writeable state dict for the entire tap.
        """
        return self._tap_state

    def get_context_state(self, context: dict | None) -> dict:
        """Return a writable state dict for the given context.

        Gives a partitioned context state if applicable; else returns stream state.
        A blank state will be created in none exists.

        This method is internal to the SDK and should not need to be overridden.
        Developers may access this property but this is not recommended except in
        advanced use cases. Instead, developers should access the latest stream
        replication key values using
        :meth:`~singer_sdk.Stream.get_starting_timestamp()` for timestamp keys, or
        :meth:`~singer_sdk.Stream.get_starting_replication_key_value()` for
        non-timestamp keys.

        Partition level may be overridden by
        :attr:`~singer_sdk.Stream.state_partitioning_keys` if set.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A partitioned context state if applicable; else returns stream state.
            A blank state will be created in none exists.
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
        """Get writable state.

        This method is internal to the SDK and should not need to be overridden.
        Developers may access this property but this is not recommended except in
        advanced use cases. Instead, developers should access the latest stream
        replication key values using :meth:`~singer_sdk.Stream.get_starting_timestamp()`
        for timestamp keys, or
        :meth:`~singer_sdk.Stream.get_starting_replication_key_value()` for
        non-timestamp keys.

        A blank state entry will be created if one doesn't already exist.

        Returns:
            A writable state dict for this stream.
        """
        return get_writeable_state_dict(self.tap_state, self.name)

    # Partitions

    @property
    def partitions(self) -> list[dict] | None:
        """Get stream partitions.

        Developers may override this property to provide a default partitions list.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.

        Returns:
            A list of partition key dicts (if applicable), otherwise `None`.
        """
        result: list[dict] = []
        for partition_state in (
            get_state_partitions_list(self.tap_state, self.name) or []
        ):
            result.append(partition_state["context"])
        return result or None

    # Private bookmarking methods

    def _increment_stream_state(
        self, latest_record: dict[str, Any], *, context: dict | None = None
    ) -> None:
        """Update state of stream or partition with data from the provided record.

        Raises `InvalidStreamSortException` is `self.is_sorted = True` and unsorted data
        is detected.

        Note: The default implementation does not advance any bookmarks unless
        `self.replication_method == 'INCREMENTAL'.

        Args:
            latest_record: TODO
            context: Stream partition or context dictionary.

        Raises:
            ValueError: TODO
        """
        # This also creates a state entry if one does not yet exist:
        state_dict = self.get_context_state(context)

        # Advance state bookmark values if applicable
        if latest_record and self.replication_method == REPLICATION_INCREMENTAL:
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
                check_sorted=self.check_sorted,
            )

    # Private message authoring methods:

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        singer.write_message(singer.StateMessage(value=self.tap_state))

    def _generate_schema_messages(self) -> Generator[singer.SchemaMessage, None, None]:
        """Generate schema messages from stream maps.

        Yields:
            Schema message objects.
        """
        bookmark_keys = [self.replication_key] if self.replication_key else None
        for stream_map in self.stream_maps:
            if isinstance(stream_map, RemoveRecordTransform):
                # Don't emit schema if the stream's records are all ignored.
                continue

            schema_message = singer.SchemaMessage(
                stream_map.stream_alias,
                stream_map.transformed_schema,
                stream_map.transformed_key_properties,
                bookmark_keys,
            )
            yield schema_message

    def _write_schema_message(self) -> None:
        """Write out a SCHEMA message with the stream schema."""
        for schema_message in self._generate_schema_messages():
            singer.write_message(schema_message)

    @property
    def mask(self) -> singer.SelectionMask:
        """Get a boolean mask for stream and property selection.

        Returns:
            A mapping of breadcrumbs to boolean values, representing stream and field
            selection.
        """
        if self._mask is None:
            self._mask = self.metadata.resolve_selection()
        return self._mask

    def _generate_record_messages(
        self,
        record: dict,
    ) -> Generator[singer.RecordMessage, None, None]:
        """Write out a RECORD message.

        Args:
            record: A single stream record.

        Yields:
            Record message objects.
        """
        pop_deselected_record_properties(record, self.schema, self.mask, self.logger)
        record = conform_record_data_types(
            stream_name=self.name,
            record=record,
            schema=self.schema,
            logger=self.logger,
        )
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            # Emit record if not filtered
            if mapped_record is not None:
                record_message = singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )

                yield record_message

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message.

        Args:
            record: A single stream record.
        """
        for record_message in self._generate_record_messages(record):
            singer.write_message(record_message)

    def _write_batch_message(
        self,
        encoding: BaseBatchFileEncoding,
        manifest: list[str],
    ) -> None:
        """Write out a BATCH message.

        Args:
            encoding: The encoding to use for the batch.
            manifest: A list of filenames for the batch.
        """
        singer.write_message(
            SDKBatchMessage(
                stream=self.name,
                encoding=encoding,
                manifest=manifest,
            )
        )

    def _log_metric(self, point: metrics.Point) -> None:
        """Log a single measurement.

        Args:
            point: A single measurement value.
        """
        metrics.log(self.metrics_logger, point=point)

    def log_sync_costs(self) -> None:
        """Log a summary of Sync costs.

        The costs are calculated via `calculate_sync_cost`.
        This method can be overridden to log results in a custom
        format. It is only called once at the end of the life of
        the stream.
        """
        if len(self._sync_costs) > 0:
            msg = f"Total Sync costs for stream {self.name}: {self._sync_costs}"
            self.logger.info(msg)

    def _check_max_record_limit(self, record_count: int) -> None:
        """TODO.

        Args:
            record_count: TODO.

        Raises:
            MaxRecordsLimitException: TODO.
        """
        if (
            self._MAX_RECORDS_LIMIT is not None
            and record_count >= self._MAX_RECORDS_LIMIT
        ):
            raise MaxRecordsLimitException(
                "Stream prematurely aborted due to the stream's max record "
                f"limit ({self._MAX_RECORDS_LIMIT}) being reached."
            )

    # Handle interim stream state

    def reset_state_progress_markers(self, state: dict | None = None) -> None:
        """Reset progress markers. If all=True, all state contexts will be set.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            state: State object to promote progress markers with.
        """
        if state is None or state == {}:
            context: dict | None
            for context in self.partitions or [{}]:
                context = context or None
                state = self.get_context_state(context)
                reset_state_progress_markers(state)
            return

        reset_state_progress_markers(state)

    def finalize_state_progress_markers(self, state: dict | None = None) -> None:
        """Reset progress markers. If all=True, all state contexts will be finalized.

        This method is internal to the SDK and should not need to be overridden.

        If all=True and the stream has children, child streams will also be finalized.

        Args:
            state: State object to promote progress markers with.
        """
        if state is None or state == {}:
            for child_stream in self.child_streams or []:
                child_stream.finalize_state_progress_markers()

            context: dict | None
            for context in self.partitions or [{}]:
                context = context or None
                state = self.get_context_state(context)
                finalize_state_progress_markers(state)
            return

        finalize_state_progress_markers(state)

    # Private sync methods:

    def _process_record(
        self,
        record: dict,
        child_context: dict | None = None,
        partition_context: dict | None = None,
    ) -> None:
        """Process a record.

        Args:
            record: The record to process.
            child_context: The child context.
            partition_context: The partition context.
        """
        partition_context = partition_context or {}
        child_context = copy.copy(
            self.get_child_context(record=record, context=child_context)
        )
        for key, val in partition_context.items():
            # Add state context to records if not already present
            if key not in record:
                record[key] = val

        # Sync children, except when primary mapper filters out the record
        if self.stream_maps[0].get_filter_result(record):
            self._sync_children(child_context)

    def _sync_records(
        self,
        context: dict | None = None,
        write_messages: bool = True,
    ) -> Generator[dict, Any, Any]:
        """Sync records, emitting RECORD and STATE messages.

        Args:
            context: Stream partition or context dictionary.
            write_messages: Whether to write Singer messages to stdout.

        Raises:
            InvalidStreamSortException: TODO

        Yields:
            Each record from the source.
        """
        # Initialize metrics
        record_counter = metrics.record_counter(self.name)
        timer = metrics.sync_timer(self.name)

        record_count = 0
        current_context: dict | None
        context_list: list[dict] | None
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        with record_counter, timer:
            for current_context in context_list or [{}]:
                record_counter.context = current_context
                timer.context = current_context

                partition_record_count = 0
                current_context = current_context or None
                state = self.get_context_state(current_context)
                state_partition_context = self._get_state_partition_context(
                    current_context
                )
                self._write_starting_replication_value(current_context)
                child_context: dict | None = (
                    None if current_context is None else copy.copy(current_context)
                )

                for record_result in self.get_records(current_context):
                    if isinstance(record_result, tuple):
                        # Tuple items should be the record and the child context
                        record, child_context = record_result
                    else:
                        record = record_result
                    try:
                        self._process_record(
                            record,
                            child_context=child_context,
                            partition_context=state_partition_context,
                        )
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

                    self._check_max_record_limit(record_count)

                    if selected:
                        if (
                            record_count - 1
                        ) % self.STATE_MSG_FREQUENCY == 0 and write_messages:
                            self._write_state_message()
                        if write_messages:
                            self._write_record_message(record)
                        self._increment_stream_state(record, context=current_context)

                        yield record

                        record_counter.increment()
                        record_count += 1
                        partition_record_count += 1

                if current_context == state_partition_context:
                    # Finalize per-partition state only if 1:1 with context
                    finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)

        if write_messages:
            # Reset interim bookmarks before emitting final STATE message:
            self._write_state_message()

    def _sync_batches(
        self,
        batch_config: BatchConfig,
        context: dict | None = None,
    ) -> None:
        """Sync batches, emitting BATCH messages.

        Args:
            batch_config: The batch configuration.
            context: Stream partition or context dictionary.
        """
        with metrics.batch_counter(self.name, context=context) as counter:
            for encoding, manifest in self.get_batches(batch_config, context):
                counter.increment()
                self._write_batch_message(encoding=encoding, manifest=manifest)
                self._write_state_message()

    # Public methods ("final", not recommended to be overridden)

    @final
    def sync(self, context: dict | None = None) -> None:
        """Sync this stream.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            context: Stream partition or context dictionary.
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
        if self.selected:
            self._write_schema_message()

        batch_config = self.get_batch_config(self.config)
        if batch_config:
            self._sync_batches(batch_config, context=context)
        else:
            # Sync the records themselves:
            for _ in self._sync_records(context=context):
                pass

    def _sync_children(self, child_context: dict) -> None:
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                child_stream.sync(context=child_context)

    # Overridable Methods

    def apply_catalog(self, catalog: singer.Catalog) -> None:
        """Apply a catalog dict, updating any settings overridden within the catalog.

        Developers may override this method in order to introduce advanced catalog
        parsing, or to explicitly fail on advanced catalog customizations which
        are not supported by the tap.

        Args:
            catalog: Catalog object passed to the tap. Defines schema, primary and
                replication keys, as well as selection metadata.
        """
        self._tap_input_catalog = catalog

        catalog_entry = catalog.get_stream(self.name)
        if catalog_entry:
            self.primary_keys = catalog_entry.key_properties
            self.replication_key = catalog_entry.replication_key
            if catalog_entry.replication_method:
                self.forced_replication_method = catalog_entry.replication_method

    def _get_state_partition_context(self, context: dict | None) -> dict | None:
        """Override state handling if Stream.state_partitioning_keys is specified.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            TODO
        """
        if context is None:
            return None

        if self.state_partitioning_keys is None:
            return context

        return {k: v for k, v in context.items() if k in self.state_partitioning_keys}

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.

        Developers may override this behavior to send specific information to child
        streams for context.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            A dictionary with context values for a child stream.

        Raises:
            NotImplementedError: If the stream has children but this method is not
                overriden.
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
    def get_records(self, context: dict | None) -> Iterable[dict | tuple[dict, dict]]:
        """Abstract record generator function. Must be overridden by the child class.

        Each record emitted should be a dictionary of property names to their values.
        Returns either a record dict or a tuple: (record_dict, child_context)

        A method which should retrieve data from the source and return records
        incrementally using the python `yield` operator.

        Only custom stream types need to define this method. REST and GraphQL streams
        should instead use the class-specific methods for REST or GraphQL, respectively.

        This method takes an optional `context` argument, which can be safely ignored
        unless the stream is a child stream or requires partitioning.
        More info: :doc:`/partitioning`.

        Parent streams can optionally return a tuple, in which
        case the second item in the tuple being a `child_context` dictionary for the
        stream's `context`.
        More info: :doc:`/parent_streams`

        Args:
            context: Stream partition or context dictionary.
        """
        pass

    def get_batch_config(self, config: Mapping) -> BatchConfig | None:
        """Return the batch config for this stream.

        Args:
            config: Tap configuration dictionary.

        Returns:
            Batch config for this stream.
        """
        raw = config.get("batch_config")
        return BatchConfig.from_dict(raw) if raw else None

    def get_batches(
        self,
        batch_config: BatchConfig,
        context: dict | None = None,
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                self._sync_records(context, write_messages=False),
                self.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with batch_config.storage.fs() as fs:
                with fs.open(filename, "wb") as f:
                    # TODO: Determine compression from config.
                    with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                        gz.writelines(
                            (json.dumps(record) + "\n").encode() for record in chunk
                        )
                file_url = fs.geturl(filename)

            yield batch_config.encoding, [file_url]

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        return row
