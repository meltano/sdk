"""Helper functions for state and bookmark management."""


from typing import Any, List, Optional


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


def wipe_stream_state_keys(
    state: dict,
    tap_stream_id: str,
    wipe_keys: List[str] = None,
    *,
    except_keys: List[str] = None,
    partition: Optional[dict] = None,
) -> None:
    """Wipe bookmarks.

    You may specify a list to wipe or a list to keep, but not both.
    """
    state_dict = get_writeable_state_dict(state, tap_stream_id, partition=partition)

    if except_keys and wipe_keys:
        raise ValueError(
            "Incorrect number of arguments. "
            "Expected `except_keys` or `wipe_keys` but not both."
        )
    if except_keys:
        wipe_keys = [
            found_key for found_key in state_dict.keys() if found_key not in except_keys
        ]
    wipe_keys = wipe_keys or []
    for wipe_key in wipe_keys:
        if wipe_key in state:
            state_dict.pop(wipe_key)
    return
