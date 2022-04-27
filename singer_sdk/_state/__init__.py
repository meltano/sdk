"""Helper functions for state and bookmark management."""

from datetime import datetime
from typing import List, Optional, Union

from singer_sdk._state.schema import (
    PartitionState,
    ProgressMarkers,
    StreamState,
    TapState,
    TReplKey,
)
from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers._typing import to_json_compatible

AnyState = Union[StreamState[TReplKey], PartitionState[TReplKey]]
AnyProgress = Union[
    ProgressMarkers[TReplKey],
    StreamState[TReplKey],
    PartitionState[TReplKey],
]


def get_state_if_exists(
    tap_state: TapState[TReplKey],
    tap_stream_id: str,
    state_partition_context: Optional[dict] = None,
) -> Optional[AnyState]:
    """Return the stream or partition state, creating a new one if it does not exist.

    Args:
        tap_state: The existing state dict which contains all streams.
        tap_stream_id: The id of the stream.
        state_partition_context: Keys which identify the partition context,
            by default None (not partitioned)

    Returns:
        Returns the state if exists, otherwise None
    """
    if not tap_state.bookmarks or tap_stream_id not in tap_state.bookmarks:
        return None

    stream_state = tap_state.bookmarks[tap_stream_id]

    if not state_partition_context:
        return stream_state

    if not stream_state.partitions:
        return None  # No partitions defined

    matched_partition = _find_in_partitions_list(
        stream_state.partitions,
        state_partition_context,
    )
    if matched_partition is None:
        return None  # Partition definition not present

    return matched_partition


def get_state_partitions_list(
    tap_state: TapState[TReplKey],
    tap_stream_id: str,
) -> Optional[List[PartitionState[TReplKey]]]:
    """Return a list of partitions defined in the state, or None if not defined.

    Args:
        tap_state: The existing state dict which contains all streams.
        tap_stream_id: The id of the stream.

    Returns:
        A list of partition bookmarks for the stream.
    """
    state = get_state_if_exists(tap_state, tap_stream_id)
    if isinstance(state, StreamState):
        return state.partitions
    return None


def _find_in_partitions_list(
    partitions: List[PartitionState[TReplKey]],
    state_partition_context: dict,
) -> Optional[PartitionState[TReplKey]]:
    found = [
        partition_state
        for partition_state in partitions
        if partition_state.context == state_partition_context
    ]
    if len(found) > 1:
        raise ValueError(
            f"State file contains duplicate entries for partition: "
            "{state_partition_context}.\n"
            f"Matching state values were: {str(found)}"
        )
    if found:
        return found[0]

    return None


def _create_in_partitions_list(
    partitions: List[PartitionState[TReplKey]],
    state_partition_context: dict,
) -> PartitionState[TReplKey]:
    # Existing partition not found. Creating new state entry in partitions list...
    new_partition_state = PartitionState[TReplKey](context=state_partition_context)
    partitions.append(new_partition_state)
    return new_partition_state


def get_writeable_state(
    tap_state: Optional[TapState[TReplKey]],
    tap_stream_id: str,
    state_partition_context: Optional[dict] = None,
) -> AnyState:
    """Return the stream or partition state, creating a new one if it does not exist.

    Args:
        tap_state: The existing state dict which contains all streams.
        tap_stream_id: The ID of the stream.
        state_partition_context: Keys which identify the partition context,
            by default None (not partitioned)

    Returns:
        Returns a writeable dict at the stream or partition level.

    Raises:
        ValueError: Raise an error if duplicate entries are found.
    """
    if tap_state is None:
        raise ValueError("Cannot write state to missing state dictionary.")

    if tap_stream_id not in tap_state.bookmarks:
        tap_state.bookmarks[tap_stream_id] = StreamState[TReplKey]()

    stream_state = tap_state.bookmarks[tap_stream_id]

    if not state_partition_context:
        return stream_state

    found = _find_in_partitions_list(stream_state.partitions, state_partition_context)
    if found:
        return found

    return _create_in_partitions_list(stream_state.partitions, state_partition_context)


def reset_state_progress_markers(
    stream_or_partition_state: AnyState,
) -> Optional[ProgressMarkers[TReplKey]]:
    """Wipe the state once sync is complete.

    For logging purposes, return the wiped 'progress_markers' object if it existed.

    Args:
        stream_or_partition_state: A bookmark object for the stream or partition.

    Returns:
        A progress markers object with some keys removed.
    """
    # Get and reset markers
    progress_markers = stream_or_partition_state.progress_markers
    stream_or_partition_state.progress_markers = None

    # Remove auto-generated human-readable note:
    if progress_markers is not None:
        progress_markers.Note = None

    # Return remaining 'progress_markers' if any:
    return progress_markers


def write_replication_key_signpost(
    stream_or_partition_state: AnyState,
    new_signpost_value: datetime,
) -> None:
    """Write initial replication value to state.

    Args:
        stream_or_partition_state: A bookmark object for the stream or partition.
        new_signpost_value: Value to write.
    """
    stream_or_partition_state.replication_key_signpost = to_json_compatible(
        new_signpost_value
    )


def write_starting_replication_value(
    stream_or_partition_state: AnyState,
    initial_value: TReplKey,
) -> None:
    """Write initial replication value to state.

    Args:
        stream_or_partition_state: A bookmark object for the stream or partition.
        initial_value: Value to write.
    """
    stream_or_partition_state.starting_replication_value = to_json_compatible(
        initial_value
    )


def get_starting_replication_value(
    stream_or_partition_state: Optional[AnyState],
) -> Optional[TReplKey]:
    """Retrieve initial replication marker value from state.

    Args:
        stream_or_partition_state: A bookmark object for the stream or partition.

    Returns:
        The starting replication value, if any.
    """
    if not stream_or_partition_state:
        return None
    return stream_or_partition_state.starting_replication_value


def increment_state(
    stream_or_partition_state: AnyState,
    latest_record: dict,
    replication_key: str,
    is_sorted: bool,
) -> None:
    """Update the state using data from the latest record.

    Args:
        stream_or_partition_state: A bookmark object for the stream or partition.
        latest_record: Stream record from which to get the replication value from.
        replication_key: Name of field to extract as replication value.
        is_sorted: Whether the stream emits records ordered by replication key value,
            ascending.

    Raises:
        InvalidStreamSortException: If is_sorted=True and unsorted
            data is detected in the stream.
    """
    progress: AnyProgress = stream_or_partition_state
    if not is_sorted:
        if stream_or_partition_state.progress_markers is None:
            stream_or_partition_state.progress_markers = ProgressMarkers[TReplKey](
                Note="Progress is not resumable if interrupted."
            )
        progress = stream_or_partition_state.progress_markers

    old_rk_value = to_json_compatible(progress.replication_key_value)
    new_rk_value = to_json_compatible(latest_record[replication_key])

    if old_rk_value is None or new_rk_value >= old_rk_value:
        progress.replication_key = replication_key
        progress.replication_key_value = new_rk_value
        return

    if is_sorted:
        raise InvalidStreamSortException(
            f"Unsorted data detected in stream. Latest value '{new_rk_value}' is "
            f"smaller than previous max '{old_rk_value}'."
        )


def finalize_state_progress_markers(
    stream_or_partition_state: AnyState,
) -> Optional[ProgressMarkers[TReplKey]]:
    """Promote or wipe progress markers once sync is complete.

    Args:
        stream_or_partition_state: A bookmark object for the stream or partition.

    Returns:
        A progress markers object with some keys removed.
    """
    signpost_value = stream_or_partition_state.replication_key_signpost
    stream_or_partition_state.replication_key_signpost = None

    stream_or_partition_state.starting_replication_value = None

    progress_markers = stream_or_partition_state.progress_markers

    if progress_markers is not None:
        if progress_markers.replication_key is not None:
            # Replication keys valid (only) after sync is complete
            stream_or_partition_state.replication_key = progress_markers.replication_key
            progress_markers.replication_key = None

            new_rk_value = progress_markers.replication_key_value
            progress_markers.replication_key_value = None

            if signpost_value and new_rk_value > signpost_value:
                new_rk_value = signpost_value

            stream_or_partition_state.replication_key_value = new_rk_value

    # Wipe and return any markers that have not been promoted
    return reset_state_progress_markers(stream_or_partition_state)
