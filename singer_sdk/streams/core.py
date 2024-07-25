"""Stream abstract class."""

from __future__ import annotations

import abc
import copy
import datetime
import json
import typing as t
from os import PathLike
from pathlib import Path
from types import MappingProxyType

import singer_sdk._singerlib as singer
from singer_sdk import metrics
from singer_sdk.batch import Batcher
from singer_sdk.exceptions import (
    AbortedSyncFailedException,
    AbortedSyncPausedException,
    InvalidReplicationKeyException,
    InvalidStreamSortException,
    MaxRecordsLimitException,
)
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    SDKBatchMessage,
)
from singer_sdk.helpers._catalog import pop_deselected_record_properties
from singer_sdk.helpers._compat import datetime_fromisoformat
from singer_sdk.helpers._flattening import get_flattening_options
from singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    get_starting_replication_value,
    get_state_partitions_list,
    get_writeable_state_dict,
    increment_state,
    is_state_non_resumable,
    log_sort_error,
    reset_state_progress_markers,
    write_replication_key_signpost,
    write_starting_replication_value,
)
from singer_sdk.helpers._typing import (
    TypeConformanceLevel,
    conform_record_data_types,
    is_datetime_type,
)
from singer_sdk.helpers._util import utc_now
from singer_sdk.mapper import RemoveRecordTransform, SameRecordTransform, StreamMap

if t.TYPE_CHECKING:
    import logging

    from singer_sdk.helpers import types
    from singer_sdk.helpers._compat import Traversable
    from singer_sdk.tap_base import Tap

# Replication methods
REPLICATION_FULL_TABLE = "FULL_TABLE"
REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"


class Stream(metaclass=abc.ABCMeta):  # noqa: PLR0904
    """Abstract base class for tap streams.

    :ivar context: Stream partition or context dictionary.

    .. versionadded:: 0.39.0
       The ``context`` attribute.
    """

    STATE_MSG_FREQUENCY = 10000
    """Number of records between state messages."""

    ABORT_AT_RECORD_COUNT: int | None = None
    """
    If set, raise `MaxRecordsLimitException` if the limit is exceeded.
    """

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.RECURSIVE
    """Type conformance level for this stream.

    Field types in the schema are used to convert record field values to the correct
    type.

    Available options are:

    - ``TypeConformanceLevel.NONE``: No conformance is performed.
    - ``TypeConformanceLevel.RECURSIVE``: Conformance is performed recursively through
      all nested levels in the record.
    - ``TypeConformanceLevel.ROOT_ONLY``: Conformance is performed only on the
      root level.
    """

    # Used for nested stream relationships
    parent_stream_type: type[Stream] | None = None
    """Parent stream type for this stream. If this stream is a child stream, this should
    be set to the parent stream class.
    """

    ignore_parent_replication_key: bool = False

    selected_by_default: bool = True
    """Whether this stream is selected by default in the catalog."""

    def __init__(
        self,
        tap: Tap,
        schema: str | PathLike | dict[str, t.Any] | singer.Schema | None = None,
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
            msg = "Missing argument or class variable 'name'."
            raise ValueError(msg)

        self.logger: logging.Logger = tap.logger.getChild(self.name)
        self.metrics_logger = tap.metrics_logger
        self.tap_name: str = tap.name
        self.context: types.Context | None = None

        self._config: dict = dict(tap.config)
        self._tap = tap
        self._tap_state = tap.state
        self._tap_input_catalog: singer.Catalog | None = None
        self._stream_maps: list[StreamMap] | None = None
        self.forced_replication_method: str | None = None
        self._replication_key: str | None = None
        self._primary_keys: t.Sequence[str] | None = None
        self._state_partitioning_keys: list[str] | None = None
        self._schema_filepath: Path | Traversable | None = None
        self._metadata: singer.MetadataMapping | None = None
        self._mask: singer.SelectionMask | None = None
        self._schema: dict
        self._is_state_flushed: bool = True
        self._last_emitted_state: dict | None = None
        self._sync_costs: dict[str, int] = {}
        self.child_streams: list[Stream] = []
        if schema:
            if isinstance(schema, (PathLike, str)):
                if not Path(schema).is_file():
                    msg = f"Could not find schema file '{self.schema_filepath}'."
                    raise FileNotFoundError(msg)

                self._schema_filepath = Path(schema)
            elif isinstance(schema, dict):
                self._schema = schema
            elif isinstance(schema, singer.Schema):
                self._schema = schema.to_dict()
            else:
                msg = f"Unexpected type {type(schema).__name__} for arg 'schema'."
                raise ValueError(msg)

        if self.schema_filepath:
            self._schema = json.loads(self.schema_filepath.read_text())

        if not self.schema:
            msg = (
                f"Could not initialize schema for stream '{self.name}'. A valid schema "
                "object or filepath was not provided."
            )
            raise ValueError(msg)

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
                "Tap has custom mapper. Using %d provided map(s).",
                len(self.stream_maps),
            )
        else:
            self.logger.info(
                "No custom mapper provided for '%s'. Using SameRecordTransform.",
                self.name,
            )
            self._stream_maps = [
                SameRecordTransform(
                    stream_alias=self.name,
                    raw_schema=self.schema,
                    key_properties=self.primary_keys,
                    flattening_options=get_flattening_options(self.config),
                ),
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

        Raises:
            InvalidReplicationKeyException: If the schema does not contain the
                replication key.
        """
        if not self.replication_key:
            return False
        type_dict = self.schema.get("properties", {}).get(self.replication_key)
        if type_dict is None:
            msg = f"Field '{self.replication_key}' is not in schema for stream '{self.name}'"  # noqa: E501
            raise InvalidReplicationKeyException(msg)
        return is_datetime_type(type_dict)

    def get_starting_replication_key_value(
        self,
        context: types.Context | None,
    ) -> t.Any | None:  # noqa: ANN401
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

        .. note::

           This method requires :attr:`~singer_sdk.Stream.replication_key` to be set
           to a non-null value, indicating the stream should be synced incrementally.
        """
        state = self.get_context_state(context)

        return (
            get_starting_replication_value(state)
            if self.replication_method != REPLICATION_FULL_TABLE
            else None
        )

    def get_starting_timestamp(
        self,
        context: types.Context | None,
    ) -> datetime.datetime | None:
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

        .. note::

           This method requires :attr:`~singer_sdk.Stream.replication_key` to be set
           to a non-null value, indicating the stream should be synced incrementally.
        """
        value = self.get_starting_replication_key_value(context)

        if value is None:
            return None

        if not self.is_timestamp_replication_key:
            msg = f"The replication key {self.replication_key} is not of timestamp type"
            raise ValueError(msg)

        result = datetime_fromisoformat(value)
        return result if result.tzinfo else result.replace(tzinfo=datetime.timezone.utc)

    @property
    def selected(self) -> bool:
        """Check if stream is selected.

        Returns:
            True if the stream is selected.
        """
        return self.mask.get((), True)

    @selected.setter
    def selected(self, value: bool | None) -> None:
        """Set stream selection.

        Args:
            value: True if the stream is selected.
        """
        self.metadata.root.selected = value
        self._mask = self.metadata.resolve_selection()

    @t.final
    @property
    def has_selected_descendents(self) -> bool:
        """Check descendents.

        Returns:
            True if any child streams are selected, recursively.
        """
        return any(
            child.selected or child.has_selected_descendents
            for child in self.child_streams or []
        )

    @t.final
    @property
    def descendent_streams(self) -> list[Stream]:
        """Get child streams.

        Returns:
            A list of all children, recursively.
        """
        result: list[Stream] = [*self.child_streams]
        for child in self.child_streams:
            result += child.descendent_streams or []
        return result

    def _write_replication_key_signpost(
        self,
        context: types.Context | None,
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
            return max(value, start_date_value, key=datetime_fromisoformat)

        return value

    def _write_starting_replication_value(self, context: types.Context | None) -> None:
        """Write the starting replication value, if available.

        Args:
            context: Stream partition or context dictionary.
        """
        value = None
        state = self.get_context_state(context)

        if self.replication_key:
            replication_key_value = state.get("replication_key_value")
            if replication_key_value and self.replication_key == state.get(
                "replication_key",
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
        self,
        context: types.Context | None,  # noqa: ARG002
    ) -> datetime.datetime | t.Any | None:  # noqa: ANN401
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
        return utc_now() if self.is_timestamp_replication_key else None

    @property
    def schema_filepath(self) -> Path | Traversable | None:
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
    def primary_keys(self) -> t.Sequence[str] | None:
        """Get primary keys.

        Returns:
            A list of primary key(s) for the stream.
        """
        return self._primary_keys or []

    @primary_keys.setter
    def primary_keys(self, new_value: t.Sequence[str] | None) -> None:
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
        return self._replication_key or None

    @replication_key.setter
    def replication_key(self, new_value: str | None) -> None:
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
            selected_by_default=self.selected_by_default,
        )

        # If there's no input catalog, select all streams
        self._metadata.root.selected = (
            self._tap_input_catalog is None and self.selected_by_default
        )

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
    def config(self) -> t.Mapping[str, t.Any]:
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

    def get_context_state(self, context: types.Context | None) -> dict:
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
    def partitions(self) -> list[types.Context] | None:
        """Get stream partitions.

        Developers may override this property to provide a default partitions list.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.

        Returns:
            A list of partition key dicts (if applicable), otherwise `None`.
        """
        result: list[types.Mapping] = [
            partition_state["context"]
            for partition_state in (
                get_state_partitions_list(self.tap_state, self.name) or []
            )
        ]
        return result or None

    # Private bookmarking methods

    def _increment_stream_state(
        self,
        latest_record: types.Record,
        *,
        context: types.Context | None = None,
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
                msg = (
                    f"Could not detect replication key for '{self.name}' "
                    f"stream(replication method={self.replication_method})"
                )
                raise ValueError(msg)
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
        if (not self._is_state_flushed) and (
            self.tap_state != self._last_emitted_state
        ):
            self._tap.write_message(singer.StateMessage(value=self.tap_state))
            self._last_emitted_state = copy.deepcopy(self.tap_state)
            self._is_state_flushed = True

    def _generate_schema_messages(
        self,
    ) -> t.Generator[singer.SchemaMessage, None, None]:
        """Generate schema messages from stream maps.

        Yields:
            Schema message objects.
        """
        bookmark_keys = [self.replication_key] if self.replication_key else None
        for stream_map in self.stream_maps:
            if isinstance(stream_map, RemoveRecordTransform):
                # Don't emit schema if the stream's records are all ignored.
                continue

            yield singer.SchemaMessage(
                stream_map.stream_alias,
                stream_map.transformed_schema,
                stream_map.transformed_key_properties,
                bookmark_keys,
            )

    def _write_schema_message(self) -> None:
        """Write out a SCHEMA message with the stream schema."""
        for schema_message in self._generate_schema_messages():
            self._tap.write_message(schema_message)

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
        record: types.Record,
    ) -> t.Generator[singer.RecordMessage, None, None]:
        """Write out a RECORD message.

        Args:
            record: A single stream record.

        Yields:
            Record message objects.
        """
        pop_deselected_record_properties(record, self.schema, self.mask)
        record = conform_record_data_types(
            stream_name=self.name,
            record=record,
            schema=self.schema,
            level=self.TYPE_CONFORMANCE_LEVEL,
            logger=self.logger,
        )
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            # Emit record if not filtered
            if mapped_record is not None:
                yield singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )

    def _write_record_message(self, record: types.Record) -> None:
        """Write out a RECORD message.

        Args:
            record: A single stream record.
        """
        for record_message in self._generate_record_messages(record):
            self._tap.write_message(record_message)

        self._is_state_flushed = False

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
        self._tap.write_message(
            SDKBatchMessage(
                stream=self.name,
                encoding=encoding,
                manifest=manifest,
            ),
        )
        self._is_state_flushed = False

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

    def _check_max_record_limit(self, current_record_index: int) -> None:
        """Raise an exception if dry run record limit exceeded.

        Raised if we find dry run record limit exceeded,
        aka `current_record_index > self.ABORT_AT_RECORD_COUNT - 1`.

        Args:
            current_record_index: The zero-based index of the current record.
        """
        if (
            self.ABORT_AT_RECORD_COUNT is not None
            and current_record_index > self.ABORT_AT_RECORD_COUNT - 1
        ):
            self._abort_sync(
                abort_reason=MaxRecordsLimitException(
                    "Stream prematurely aborted due to the stream's max dry run "
                    f"record limit ({self.ABORT_AT_RECORD_COUNT}) being reached.",
                ),
            )

    def _abort_sync(self, abort_reason: Exception) -> None:
        """Handle a sync operation being aborted.

        This method will attempt to close out the sync operation as gracefully as
        possible - for instance, if a max runtime or record count is reached. This can
        also be called for `SIGTERM` and KeyboardInterrupt events.

        If a state message is pending, we will attempt to write it to STDOUT before
        shutting down.

        If the stream can reach a valid resumable state, then we will raise
        `AbortedSyncPausedException`. Otherwise, `AbortedSyncFailedException` will be
        raised.

        Args:
            abort_reason: The exception that triggered the sync to be aborted.

        Raises:
            AbortedSyncFailedException: Raised if sync could not reach a valid state.
            AbortedSyncPausedException: Raised if sync was able to be transitioned into
                a valid state without data loss or corruption.
        """
        self._write_state_message()  # Write out state message if pending.

        if self.replication_method == "FULL_TABLE":
            msg = "Sync operation aborted for stream in 'FULL_TABLE' replication mode."
            raise AbortedSyncFailedException(msg) from abort_reason

        if is_state_non_resumable(self.stream_state):
            msg = "Sync operation aborted and state is not in a resumable state."
            raise AbortedSyncFailedException(msg) from abort_reason

        # Else, the sync operation can be assumed to be in a valid resumable state.
        raise AbortedSyncPausedException from abort_reason

    # Handle interim stream state

    def reset_state_progress_markers(self, state: dict | None = None) -> None:
        """Reset progress markers. If all=True, all state contexts will be set.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            state: State object to promote progress markers with.
        """
        if state is None or state == {}:
            context: types.Context | None
            for context in self.partitions or [{}]:
                state = self.get_context_state(context or None)
                reset_state_progress_markers(state)
            return

        reset_state_progress_markers(state)

    def _finalize_state(self, state: dict | None = None) -> None:
        """Reset progress markers and state flushed flag to ensure state is written.

        Args:
            state: State object to promote progress markers with.
        """
        state = finalize_state_progress_markers(state)  # type: ignore[arg-type]
        self._is_state_flushed = False

    def finalize_state_progress_markers(self, state: dict | None = None) -> None:
        """Reset progress markers and emit state message if necessary.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            state: State object to promote progress markers with.
        """
        if state is None or state == {}:
            for child_stream in self.child_streams or []:
                child_stream.finalize_state_progress_markers()

            context: types.Context | None
            for context in self.partitions or [{}]:
                state = self.get_context_state(context or None)
                self._finalize_state(state)
        else:
            self._finalize_state(state)

        self._write_state_message()

    # Private sync methods:

    def _process_record(
        self,
        record: types.Record,
        child_context: types.Context | None = None,
        partition_context: types.Context | None = None,
    ) -> None:
        """Process a record.

        Args:
            record: The record to process.
            child_context: The child context.
            partition_context: The partition context.
        """
        partition_context = partition_context or {}
        for key, val in partition_context.items():
            # Add state context to records if not already present
            if key not in record:
                record[key] = val

        for context in self.generate_child_contexts(
            record=record,
            context=child_context,
        ):
            # Sync children, except when primary mapper filters out the record
            if self.stream_maps[0].get_filter_result(record):
                self._sync_children(copy.copy(context))

    def _sync_records(  # noqa: C901
        self,
        context: types.Context | None = None,
        *,
        write_messages: bool = True,
    ) -> t.Generator[dict, t.Any, t.Any]:
        """Sync records, emitting RECORD and STATE messages.

        Args:
            context: Stream partition or context dictionary.
            write_messages: Whether to write Singer messages to stdout.

        Raises:
            InvalidStreamSortException: Raised if sorting errors are found while
                syncing the records.

        Yields:
            Each record from the source.
        """
        # Initialize metrics
        record_counter = metrics.record_counter(self.name)
        timer = metrics.sync_timer(self.name)

        record_index = 0
        context_element: types.Context | None
        context_list: list[types.Context] | None
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        with record_counter, timer:
            for context_element in context_list or [{}]:
                record_counter.context = context_element
                timer.context = context_element

                current_context = context_element or None
                state = self.get_context_state(current_context)
                state_partition_context = self._get_state_partition_context(
                    current_context,
                )
                self._write_starting_replication_value(current_context)
                child_context: types.Context | None = (
                    None if current_context is None else copy.copy(current_context)
                )

                for idx, record_result in enumerate(self.get_records(current_context)):
                    self._check_max_record_limit(current_record_index=record_index)

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
                    except InvalidStreamSortException as ex:  # pragma: no cover
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_index + 1,
                            partition_record_count=idx + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise

                    if selected:
                        if write_messages:
                            self._write_record_message(record)

                        self._increment_stream_state(record, context=current_context)
                        if (
                            record_index + 1
                        ) % self.STATE_MSG_FREQUENCY == 0 and write_messages:
                            self._write_state_message()

                        record_counter.increment()
                        yield record

                    record_index += 1

                if current_context == state_partition_context:
                    # Finalize per-partition state only if 1:1 with context
                    self._finalize_state(state)

        if not context:
            # Finalize total stream only if we have the full context.
            # Otherwise will be finalized by tap at end of sync.
            self._finalize_state(self.stream_state)

        if write_messages:
            # Write final state message if we haven't already
            self._write_state_message()

    def _sync_batches(
        self,
        batch_config: BatchConfig,
        context: types.Context | None = None,
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

    @t.final
    def sync(self, context: types.Context | None = None) -> None:
        """Sync this stream.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            Exception: Any exception raised by the sync process.
        """
        msg = f"Beginning {self.replication_method.lower()} sync of '{self.name}'"
        if context:
            msg += f" with context: {context}"
        self.logger.info("%s...", msg)
        self.context = MappingProxyType(context) if context else None

        # Use a replication signpost, if available
        signpost = self.get_replication_key_signpost(context)
        if signpost:
            self._write_replication_key_signpost(context, signpost)

        # Send a SCHEMA message to the downstream target:
        if self.selected:
            self._write_schema_message()

        try:
            batch_config = self.get_batch_config(self.config)
            if batch_config:
                self._sync_batches(batch_config, context=context)
            else:
                # Sync the records themselves:
                for _ in self._sync_records(context=context):
                    pass
        except Exception:
            self.logger.exception(
                "An unhandled error occurred while syncing '%s'",
                self.name,
            )
            raise

    def _sync_children(self, child_context: types.Context | None) -> None:
        if child_context is None:
            self.logger.warning(
                "Context for child streams of '%s' is null, "
                "skipping sync of any child streams",
                self.name,
            )
            return

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

    def _get_state_partition_context(
        self,
        context: types.Context | None,
    ) -> types.Context | None:
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

    def get_child_context(
        self,
        record: types.Record,
        context: types.Context | None,
    ) -> types.Context | None:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.

        Developers may override this behavior to send specific information to child
        streams for context.

        Return ``None`` if no child streams should be synced, for example if the
        parent record was deleted and the child records can no longer be synced.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            A dictionary with context values for a child stream, or None if no child
            streams should be synced.

        Raises:
            NotImplementedError: If the stream has children but this method is not
                overridden.
        """
        if context is None:
            for child_stream in self.child_streams:
                if child_stream.state_partitioning_keys is None:
                    parent_type = type(self).__name__
                    child_type = type(child_stream).__name__
                    msg = (
                        "No child context behavior was defined between parent stream "
                        f"'{self.name}' and child stream '{child_stream.name}'. "
                        f"The parent stream must define "
                        f"`{parent_type}.get_child_context()` and/or the child stream "
                        f"must define `{child_type}.state_partitioning_keys`."
                    )
                    raise NotImplementedError(msg)

        return context or record

    def generate_child_contexts(
        self,
        record: types.Record,
        context: types.Context | None,
    ) -> t.Iterable[types.Context | None]:
        """Generate child contexts.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Yields:
            A child context for each child stream.
        """
        yield self.get_child_context(record=record, context=context)

    # Abstract Methods

    @abc.abstractmethod
    def get_records(
        self,
        context: types.Context | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
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

        If the child context object in the tuple is ``None``, the child streams will
        be skipped. This is useful for cases where the parent record was deleted and
        the child records can no longer be synced.

        More info: :doc:`/parent_streams`

        Args:
            context: Stream partition or context dictionary.
        """

    def get_batch_config(self, config: t.Mapping) -> BatchConfig | None:  # noqa: PLR6301
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
        context: types.Context | None = None,
    ) -> t.Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        batcher = Batcher(
            tap_name=self.tap_name,
            stream_name=self.name,
            batch_config=batch_config,
        )
        records = self._sync_records(context, write_messages=False)
        for manifest in batcher.get_batches(records=records):
            yield batch_config.encoding, manifest

    def post_process(  # noqa: PLR6301
        self,
        row: types.Record,
        context: types.Context | None = None,  # noqa: ARG002
    ) -> dict | None:
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
