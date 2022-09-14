"""Helper functions for state and bookmark management."""

import datetime
from typing import Any, Callable, List, Optional, Union, cast

from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers._typing import to_json_compatible

PROGRESS_MARKERS = "progress_markers"
PROGRESS_MARKER_NOTE = "Note"
SIGNPOST_MARKER = "replication_key_signpost"
STARTING_MARKER = "starting_replication_value"


def get_state_if_exists(
    tap_state: dict,
    tap_stream_id: str,
    state_partition_context: Optional[dict] = None,
    key: Optional[str] = None,
) -> Optional[Any]:
    """Return the stream or partition state, creating a new one if it does not exist.

    Parameters
    ----------
    tap_state : dict
        the existing state dict which contains all streams.
    tap_stream_id : str
        the id of the stream
    state_partition_context : Optional[dict], optional
        keys which identify the partition context, by default None (not partitioned)
    key : Optional[str], optional
        name of the key searched for, by default None (return entire state if found)

    Returns
    -------
    Optional[Any]
        Returns the state if exists, otherwise None

    Raises
    ------
    ValueError
        Raised if state is invalid or cannot be parsed.

    """
    if "bookmarks" not in tap_state:
        return None
    if tap_stream_id not in tap_state["bookmarks"]:
        return None

    stream_state = tap_state["bookmarks"][tap_stream_id]
    if not state_partition_context:
        if key:
            return stream_state.get(key, None)
        return stream_state
    if "partitions" not in stream_state:
        return None  # No partitions defined

    matched_partition = _find_in_partitions_list(
        stream_state["partitions"], state_partition_context
    )
    if matched_partition is None:
        return None  # Partition definition not present
    if key:
        return matched_partition.get(key, None)
    return matched_partition


def get_state_partitions_list(
    tap_state: dict, tap_stream_id: str
) -> Optional[List[dict]]:
    """Return a list of partitions defined in the state, or None if not defined."""
    return (get_state_if_exists(tap_state, tap_stream_id) or {}).get("partitions", None)


def _find_in_partitions_list(
    partitions: List[dict], state_partition_context: dict
) -> Optional[dict]:
    found = [
        partition_state
        for partition_state in partitions
        if partition_state["context"] == state_partition_context
    ]
    if len(found) > 1:
        raise ValueError(
            f"State file contains duplicate entries for partition: "
            "{state_partition_context}.\n"
            f"Matching state values were: {str(found)}"
        )
    if found:
        return cast(dict, found[0])

    return None


def _create_in_partitions_list(
    partitions: List[dict], state_partition_context: dict
) -> dict:
    # Existing partition not found. Creating new state entry in partitions list...
    new_partition_state = {"context": state_partition_context}
    partitions.append(new_partition_state)
    return new_partition_state


def get_writeable_state_dict(
    tap_state: dict, tap_stream_id: str, state_partition_context: Optional[dict] = None
) -> dict:
    """Return the stream or partition state, creating a new one if it does not exist.

    Parameters
    ----------
    tap_state : dict
        the existing state dict which contains all streams.
    tap_stream_id : str
        the id of the stream
    state_partition_context : Optional[dict], optional
        keys which identify the partition context, by default None (not partitioned)

    Returns
    -------
    dict
        Returns a writeable dict at the stream or partition level.

    Raises
    ------
    ValueError
        Raise an error if duplicate entries are found.

    """
    if tap_state is None:
        raise ValueError("Cannot write state to missing state dictionary.")

    if "bookmarks" not in tap_state:
        tap_state["bookmarks"] = {}
    if tap_stream_id not in tap_state["bookmarks"]:
        tap_state["bookmarks"][tap_stream_id] = {}
    stream_state = cast(dict, tap_state["bookmarks"][tap_stream_id])
    if not state_partition_context:
        return stream_state

    if "partitions" not in stream_state:
        stream_state["partitions"] = []
    stream_state_partitions: List[dict] = stream_state["partitions"]
    found = _find_in_partitions_list(stream_state_partitions, state_partition_context)
    if found:
        return found

    return _create_in_partitions_list(stream_state_partitions, state_partition_context)


def write_stream_state(
    tap_state,
    tap_stream_id: str,
    key,
    val,
    *,
    state_partition_context: Optional[dict] = None,
) -> None:
    """Write stream state."""
    state_dict = get_writeable_state_dict(
        tap_state, tap_stream_id, state_partition_context=state_partition_context
    )
    state_dict[key] = val


def reset_state_progress_markers(stream_or_partition_state: dict) -> Optional[dict]:
    """Wipe the state once sync is complete.

    For logging purposes, return the wiped 'progress_markers' object if it existed.
    """
    progress_markers = stream_or_partition_state.pop(PROGRESS_MARKERS, {})
    # Remove auto-generated human-readable note:
    progress_markers.pop(PROGRESS_MARKER_NOTE, None)
    # Return remaining 'progress_markers' if any:
    return progress_markers or None


def write_replication_key_signpost(
    stream_or_partition_state: dict,
    new_signpost_value: Any,
) -> None:
    """Write signpost value."""
    stream_or_partition_state[SIGNPOST_MARKER] = to_json_compatible(new_signpost_value)


def write_starting_replication_value(
    stream_or_partition_state: dict,
    initial_value: Any,
) -> None:
    """Write initial replication value to state."""
    stream_or_partition_state[STARTING_MARKER] = to_json_compatible(initial_value)


def get_starting_replication_value(stream_or_partition_state: dict):
    """Retrieve initial replication marker value from state."""
    if not stream_or_partition_state:
        return None
    return stream_or_partition_state.get(STARTING_MARKER)


def increment_state(
    stream_or_partition_state: dict,
    latest_record: dict,
    replication_key: str,
    is_sorted: bool,
    check_sorted: bool,
) -> None:
    """Update the state using data from the latest record.

    Raises InvalidStreamSortException if is_sorted=True, check_sorted=True and unsorted
    data is detected in the stream.
    """
    progress_dict = stream_or_partition_state
    if not is_sorted:
        if PROGRESS_MARKERS not in stream_or_partition_state:
            stream_or_partition_state[PROGRESS_MARKERS] = {
                PROGRESS_MARKER_NOTE: "Progress is not resumable if interrupted."
            }
        progress_dict = stream_or_partition_state[PROGRESS_MARKERS]
    old_rk_value = to_json_compatible(progress_dict.get("replication_key_value"))
    new_rk_value = to_json_compatible(latest_record[replication_key])
    if old_rk_value is None or not check_sorted or new_rk_value >= old_rk_value:
        progress_dict["replication_key"] = replication_key
        progress_dict["replication_key_value"] = new_rk_value
        return

    if is_sorted:
        raise InvalidStreamSortException(
            f"Unsorted data detected in stream. Latest value '{new_rk_value}' is "
            f"smaller than previous max '{old_rk_value}'."
        )


def _greater_than_signpost(
    signpost: Union[datetime.datetime, str, int, float],
    new_value: Union[datetime.datetime, str, int, float],
) -> bool:
    """Compare and return True if new_value is greater than signpost."""
    return (  # fails if signpost and bookmark are incompatible types
        new_value > signpost  # type: ignore
    )


def finalize_state_progress_markers(stream_or_partition_state: dict) -> Optional[dict]:
    """Promote or wipe progress markers once sync is complete."""
    signpost_value = stream_or_partition_state.pop(SIGNPOST_MARKER, None)
    stream_or_partition_state.pop(STARTING_MARKER, None)
    if PROGRESS_MARKERS in stream_or_partition_state:
        if "replication_key" in stream_or_partition_state[PROGRESS_MARKERS]:
            # Replication keys valid (only) after sync is complete
            progress_markers = stream_or_partition_state[PROGRESS_MARKERS]
            stream_or_partition_state["replication_key"] = progress_markers.pop(
                "replication_key"
            )
            new_rk_value = progress_markers.pop("replication_key_value")
            if signpost_value and _greater_than_signpost(signpost_value, new_rk_value):
                new_rk_value = signpost_value
            stream_or_partition_state["replication_key_value"] = new_rk_value
    # Wipe and return any markers that have not been promoted
    return reset_state_progress_markers(stream_or_partition_state)


def log_sort_error(
    ex: Exception,
    log_fn: Callable,
    stream_name: str,
    current_context: Optional[dict],
    state_partition_context: Optional[dict],
    record_count: int,
    partition_record_count: int,
) -> None:
    """Log a sort error."""
    msg = f"Sorting error detected in '{stream_name}'." f"on record #{record_count}. "
    if partition_record_count != record_count:
        msg += (
            f"Record was partition record "
            f"#{partition_record_count} with"
            f" state partition context {state_partition_context}. "
        )
    if current_context:
        msg += f"Context was {str(current_context)}. "
    msg += str(ex)
    log_fn(msg)
