"""Helper functions for state and bookmark management."""


from typing import Any, List, Optional


def get_stream_state_dict(
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
    if not partition:
        return state["bookmarks"][tap_stream_id]
    if "partitions" not in state["bookmarks"][tap_stream_id]:
        state["bookmarks"][tap_stream_id]["partitions"] = []
    else:
        found = [
            partition_state
            for partition_state in state["bookmarks"][tap_stream_id]["partitions"]
            if partition_state.get("context") == partition
        ]
        if len(found) > 1:
            raise ValueError(
                "State file contains duplicate entries for partition definition: "
                f"{partition}"
            )
        if found:
            return found[0]
    # Existing partition not found. Creating new state entry in partitions list...
    new_dict = {"context": partition}
    state["bookmarks"][tap_stream_id]["partitions"].append(new_dict)
    return new_dict


def read_stream_state(
    state,
    tap_stream_id: str,
    key=None,
    default: Any = None,
    *,
    partition: Optional[dict] = None,
) -> Any:
    state_dict = get_stream_state_dict(state, tap_stream_id, partition=partition)
    if key:
        return state_dict.get(key, default)
    return state_dict or default


def write_stream_state(
    state, tap_stream_id: str, key, val, *, partition: Optional[dict] = None
) -> None:
    state_dict = get_stream_state_dict(state, tap_stream_id, partition=partition)
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
    state_dict = get_stream_state_dict(state, tap_stream_id, partition=partition)

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
        state_dict.pop(wipe_key)
    return
