"""Helper functions for state and bookmark management."""

import datetime
from typing import Any, List, Optional, Union

from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers._typing import to_json_compatible

PROGRESS_MARKERS = "progress_markers"
PROGRESS_MARKER_NOTE = "Note"
SIGNPOST_MARKER = "replication_key_signpost"


def get_state_if_exists(
    state: dict,
    tap_stream_id: str,
    partition: Optional[dict] = None,
    key: Optional[str] = None,
) -> Optional[Any]:
    """Return the stream or partition state, creating a new one if it does not exist.

    Parameters
    ----------
    state : dict
        the existing state dict which contains all streams.
    tap_stream_id : str
        the id of the stream
    partition : Optional[dict], optional
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
    if "bookmarks" not in state:
        return None
    if tap_stream_id not in state["bookmarks"]:
        return None

    stream_state = state["bookmarks"][tap_stream_id]
    if not partition:
        if key:
            return stream_state.get(key, None)
        return stream_state
    if "partitions" not in stream_state:
        return None

    stream_state_partitions = stream_state["partitions"]
    found = [
        partition_state
        for partition_state in stream_state_partitions
        if partition_state["context"] == partition
    ]
    if not found:
        return None  # Partition definition not present
    if len(found) > 1:
        raise ValueError(
            f"State file contains duplicate entries for partition: {partition}"
        )

    matched_partition: dict = found[0]
    if key:
        return matched_partition.get(key, None)
    return matched_partition


def get_state_partitions_list(state: dict, tap_stream_id: str) -> Optional[List[dict]]:
    """Return a list of partitions defined in the state, or None if not defined."""
    return (get_state_if_exists(state, tap_stream_id) or {}).get("partitions", None)


def get_writeable_state_dict(
    state: dict, tap_stream_id: str, partition: Optional[dict] = None
) -> dict:
    """Return the stream or partition state, creating a new one if it does not exist.

    Parameters
    ----------
    state : dict
        the existing state dict which contains all streams.
    tap_stream_id : str
        the id of the stream
    partition : Optional[dict], optional
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
    if state is None:
        raise ValueError("Cannot write state to missing state dictionary.")
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    if tap_stream_id not in state["bookmarks"]:
        state["bookmarks"][tap_stream_id] = {}
    stream_state = state["bookmarks"][tap_stream_id]
    if not partition:
        return stream_state
    if "partitions" not in stream_state:
        stream_state["partitions"] = []
    stream_state_partitions = stream_state["partitions"]
    found = [
        partition_state
        for partition_state in stream_state_partitions
        if partition_state["context"] == partition
    ]
    if len(found) > 1:
        raise ValueError(
            f"State file contains duplicate entries for partition: {partition}"
        )
    if found:
        return found[0]
    # Existing partition not found. Creating new state entry in partitions list...
    new_partition_state = {"context": partition}
    stream_state_partitions.append(new_partition_state)
    return new_partition_state


def read_stream_state(
    state,
    tap_stream_id: str,
    key=None,
    default: Any = None,
    *,
    partition: Optional[dict] = None,
) -> Any:
    """Read stream state."""
    state_dict = get_writeable_state_dict(state, tap_stream_id, partition=partition)
    if key:
        return state_dict.get(key, default)
    return state_dict or default


def write_stream_state(
    state, tap_stream_id: str, key, val, *, partition: Optional[dict] = None
) -> None:
    """Write stream state."""
    state_dict = get_writeable_state_dict(state, tap_stream_id, partition=partition)
    state_dict[key] = val


def reset_state_progress_markers(state: dict) -> Optional[dict]:
    """Wipe the state once sync is complete.

    For logging purposes, return the wiped 'progress_markers' object if it existed.
    """
    progress_markers = state.get(PROGRESS_MARKERS, {})
    # Remove auto-generated human-readable note:
    progress_markers.pop(PROGRESS_MARKER_NOTE, None)
    # Return remaining 'progress_markers' if any:
    return progress_markers or None


def write_replication_key_signpost(
    state: dict,
    new_signpost_value: Any,
) -> None:
    """Write signpost value."""
    state[SIGNPOST_MARKER] = new_signpost_value


def increment_state(
    state: dict,
    latest_record: dict,
    replication_key: str,
    replication_key_signpost: Optional[Any],
    is_sorted: bool,
) -> None:
    """Update the state using data from the latest record.

    Raises InvalidStreamSortException if is_sorted=True and unsorted
    data is detected in the stream.
    """
    progress_dict = state
    if not is_sorted:
        if PROGRESS_MARKERS not in state:
            state[PROGRESS_MARKERS] = {
                PROGRESS_MARKER_NOTE: "Progress is not resumable if interrupted."
            }
        progress_dict = state[PROGRESS_MARKERS]
    old_rk_value = to_json_compatible(progress_dict.get("replication_key_value"))
    new_rk_value = to_json_compatible(latest_record[replication_key])
    if old_rk_value is None or new_rk_value >= old_rk_value:
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
    return new_value > signpost


def finalize_state_progress_markers(state: dict) -> Optional[dict]:
    """Promote or wipe progress markers once sync is complete."""
    signpost_value = state.pop(SIGNPOST_MARKER, None)
    if PROGRESS_MARKERS in state:
        if "replication_key" in state[PROGRESS_MARKERS]:
            # Replication keys valid (only) after sync is complete
            progress_markers = state[PROGRESS_MARKERS]
            state["replication_key"] = progress_markers.pop("replication_key")
            new_rk_value = progress_markers.pop("replication_key_value")
            if signpost_value and _greater_than_signpost(signpost_value, new_rk_value):
                new_rk_value = signpost_value
            state["replication_key_value"] = new_rk_value
    # Wipe and return any markers that have not been promoted
    return reset_state_progress_markers(state)
