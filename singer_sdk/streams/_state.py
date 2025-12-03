"""Stream state management."""

from __future__ import annotations

import logging
import typing as t

from singer_sdk.helpers._state import (
    PROGRESS_MARKERS,
    finalize_state_progress_markers,
    get_starting_replication_value,
    get_state_partitions_list,
    get_writeable_state_dict,
    increment_state,
    reset_state_progress_markers,
    write_replication_key_signpost,
    write_starting_replication_value,
)
from singer_sdk.singerlib.catalog import REPLICATION_FULL_TABLE

if t.TYPE_CHECKING:
    import datetime

    from singer_sdk.helpers import types


class StreamStateManager:
    """Manages state for a single stream including partitions and replication keys.

    This class encapsulates all state management logic for a stream, including:
    - Partition-level state handling
    - Replication key tracking
    - Progress markers
    - State finalization

    This separation allows state logic to be tested independently of streaming logic.
    """

    def __init__(
        self,
        *,
        tap_name: str,
        stream_name: str,
        tap_state: types.TapState,
        state_partitioning_keys: list[str] | None = None,
    ) -> None:
        """Initialize the StreamStateManager.

        Args:
            tap_name: Name of the tap.
            stream_name: Name of the stream.
            tap_state: Shared tap-level state dictionary.
            state_partitioning_keys: Keys used for state partitioning.
        """
        self.tap_name = tap_name
        self.stream_name = stream_name
        self.tap_state = tap_state
        self.state_partitioning_keys = state_partitioning_keys
        self.is_flushed = True
        self._logger = logging.getLogger(tap_name).getChild(stream_name)

    @property
    def stream_state(self) -> dict:
        """Get writable state dict for this stream.

        A blank state entry will be created if one doesn't already exist.

        Returns:
            A writable state dict for this stream.
        """
        return get_writeable_state_dict(self.tap_state, self.stream_name)

    def get_context_state(self, context: types.Context | None) -> dict:
        """Return a writable state dict for the given context.

        Gives a partitioned context state if applicable; else returns stream state.
        A blank state will be created if none exists.

        Partition level may be overridden by state_partitioning_keys if set.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A partitioned context state if applicable; else returns stream state.
            A blank state will be created if none exists.
        """
        if state_partition_context := self.get_state_partition_context(context):
            return get_writeable_state_dict(
                self.tap_state,
                self.stream_name,
                state_partition_context=state_partition_context,
            )
        return self.stream_state

    def get_state_partition_context(
        self,
        context: types.Context | None,
    ) -> types.Context | None:
        """Override state handling if state_partitioning_keys is specified.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            Filtered context based on state_partitioning_keys, or None.
        """
        if context is None:
            return None

        if self.state_partitioning_keys is None:
            return context

        return {k: v for k, v in context.items() if k in self.state_partitioning_keys}

    def write_starting_replication_value(
        self,
        context: types.Context | None,
        replication_method: str,
        replication_key: str | None,
        config: dict,
        compare_start_date_fn: t.Callable[[str, str], str] | None = None,
    ) -> None:
        """Write the starting replication value, if available.

        Args:
            context: Stream partition or context dictionary.
            replication_method: The replication method for the stream.
            replication_key: The replication key for the stream.
            config: Stream configuration containing start_date if applicable.
            compare_start_date_fn: Optional function to compare bookmark value
                with start_date and return the most recent.
        """
        if replication_method == REPLICATION_FULL_TABLE:
            self._logger.debug(
                "Stream '%s' is not configured for incremental replication. "
                "Not writing starting replication value.",
                self.stream_name,
            )
            return

        value = None
        state = self.get_context_state(context)

        if replication_key:
            replication_key_value = state.get("replication_key_value")
            if replication_key_value and replication_key == state.get(
                "replication_key",
            ):
                value = replication_key_value

            # Use start_date if it is more recent than the replication_key state
            if start_date_value := config.get("start_date"):
                if not value:
                    value = start_date_value
                elif compare_start_date_fn and value:
                    value = compare_start_date_fn(value, start_date_value)

            self._logger.info(
                "Starting incremental sync of '%s' with bookmark value: %s",
                self.stream_name,
                value,
            )

        write_starting_replication_value(state, value)

    def write_replication_key_signpost(
        self,
        context: types.Context | None,
        value: datetime.datetime | str | float | None,
    ) -> None:
        """Write the signpost value, if available.

        Args:
            context: Stream partition or context dictionary.
            value: The signpost value to write.
        """
        if not value:
            return

        state = self.get_context_state(context)
        write_replication_key_signpost(state, value)

    def increment_state(
        self,
        latest_record: types.Record,
        *,
        context: types.Context | None = None,
        replication_key: str,
        is_sorted: bool,
        check_sorted: bool,
    ) -> None:
        """Update state of stream or partition with data from the provided record.

        Raises `InvalidStreamSortException` if `is_sorted=True` and unsorted data
        is detected.

        Args:
            latest_record: The latest record to update state with.
            context: Stream partition or context dictionary.
            replication_key: The replication key field name.
            is_sorted: Whether the stream is expected to be sorted.
            check_sorted: Whether to check for sort violations.
        """
        # This also creates a state entry if one does not yet exist:
        state_dict = self.get_context_state(context)

        # Advance state bookmark values
        increment_state(
            state_dict,
            replication_key=replication_key,
            latest_record=latest_record,
            is_sorted=is_sorted,
            check_sorted=check_sorted,
        )

    def finalize_state(self, state: dict | None = None) -> None:
        """Reset progress markers and mark state as needing flush.

        Args:
            state: State object to promote progress markers with.
        """
        finalize_state_progress_markers(state)  # type: ignore[arg-type]
        self.is_flushed = False

    def reset_progress_markers(
        self,
        state: dict | None = None,
        partitions: list[dict] | None = None,
    ) -> None:
        """Reset progress markers.

        If state is None or contains partitions, all partition contexts will be reset.

        Args:
            state: State object to reset progress markers with.
            partitions: List of partition contexts to reset if state is None or contains
                partitions.
        """
        if state is None or state == {}:
            # Reset all partition states
            partition_list = partitions or [{}]
            for partition_context in partition_list:
                partition_state = self.get_context_state(partition_context or None)
                reset_state_progress_markers(partition_state)
            return

        # If state has partitions and partition list is provided, reset partition states
        if "partitions" in state and partitions:
            for partition_context in partitions:
                partition_state = self.get_context_state(partition_context or None)
                reset_state_progress_markers(partition_state)
            # Also reset stream-level progress markers if any
            reset_state_progress_markers(state)
            return

        # Reset progress markers on the provided state (stream or partition level)
        reset_state_progress_markers(state)

    def finalize_progress_markers(
        self,
        state: dict | None = None,
        partitions: list[dict] | None = None,
    ) -> None:
        """Reset progress markers and mark state for emission.

        Args:
            state: State object to promote progress markers with.
            partitions: List of partition contexts to finalize if state is None.
        """
        if state is None or state == {}:
            partition_list = partitions or [{}]
            for partition_context in partition_list:
                state = self.get_context_state(partition_context or None)
                self.finalize_state(state)
        else:
            self.finalize_state(state)

    def get_state_partitions(self) -> list[dict] | None:
        """Return a list of partitions defined in the state, or None if not defined.

        Returns:
            List of partition state dictionaries, or None.
        """
        return get_state_partitions_list(self.tap_state, self.stream_name)

    def get_starting_replication_value(
        self,
        context: types.Context | None,
        replication_method: str,
    ) -> t.Any | None:  # noqa: ANN401
        """Get starting replication key value from state.

        Will return the value of the stream's replication key when state exists.
        If no prior state exists, will return None.

        Args:
            context: Stream partition or context dictionary.
            replication_method: The replication method for the stream.

        Returns:
            Starting replication value.
        """
        state = self.get_context_state(context)

        return (
            get_starting_replication_value(state)
            if replication_method != REPLICATION_FULL_TABLE
            else None
        )

    def is_state_non_resumable(self, context: types.Context | None = None) -> bool:
        """Check if state is non-resumable.

        This is determined by checking for a "progress marker" tag in the state
        artifact.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            True when state is non-resumable.
        """
        state = self.get_context_state(context)
        return PROGRESS_MARKERS in state

    def log_sort_error(
        self,
        *,
        ex: Exception,
        current_context: types.Context | None,
        record_count: int,
        partition_record_count: int,
    ) -> None:
        """Log a sort error with detailed context information.

        Args:
            ex: The exception that was raised.
            current_context: Stream partition or context dictionary.
            record_count: Total record count across all partitions.
            partition_record_count: Record count within the current partition.
        """
        state_partition_context = self.get_state_partition_context(current_context)

        msg = f"Sorting error detected in '{self.stream_name}' on record #{record_count}. "  # noqa: E501
        if partition_record_count != record_count:
            msg += (
                f"Record was partition record "
                f"#{partition_record_count} with"
                f" state partition context {state_partition_context}. "
            )
        if current_context:
            msg += f"Context was {current_context!s}. "
        msg += str(ex)
        self._logger.error(msg)
