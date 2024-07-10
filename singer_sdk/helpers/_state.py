"""Helper functions for state and bookmark management."""

from __future__ import annotations

import logging
import typing as t

from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers._typing import to_json_compatible

if t.TYPE_CHECKING:
    import datetime

    from singer_sdk.helpers import types

    _T = t.TypeVar("_T", datetime.datetime, str, int, float)

PROGRESS_MARKERS = "progress_markers"
PROGRESS_MARKER_NOTE = "Note"
SIGNPOST_MARKER = "replication_key_signpost"
STARTING_MARKER = "starting_replication_value"

logger = logging.getLogger("singer_sdk")


def get_state_if_exists(
    tap_state: dict,
    tap_stream_id: str,
    state_partition_context: dict | None = None,
    key: str | None = None,
) -> t.Any | None:  # noqa: ANN401
    """Return the stream or partition state, creating a new one if it does not exist.

    Args:
        tap_state: the existing state dict which contains all streams.
        tap_stream_id: the id of the stream
        state_partition_context: keys which identify the partition context,
            by default None (not partitioned)
        key: name of the key searched for, by default None (return entire state if
            found)

    Returns:
        Returns the state if exists, otherwise None

    Raises:
        ValueError: Raised if state is invalid or cannot be parsed.
    """
    if "bookmarks" not in tap_state:
        return None
    if tap_stream_id not in tap_state["bookmarks"]:
        return None

    stream_state = tap_state["bookmarks"][tap_stream_id]
    if not state_partition_context:
        return stream_state.get(key, None) if key else stream_state
    if "partitions" not in stream_state:
        return None  # No partitions defined

    matched_partition = _find_in_partitions_list(
        stream_state["partitions"],
        state_partition_context,
    )
    if matched_partition is None:
        return None  # Partition definition not present
    return matched_partition.get(key, None) if key else matched_partition


def get_state_partitions_list(tap_state: dict, tap_stream_id: str) -> list[dict] | None:
    """Return a list of partitions defined in the state, or None if not defined."""
    return (get_state_if_exists(tap_state, tap_stream_id) or {}).get("partitions", None)  # type: ignore[no-any-return]


def _find_in_partitions_list(
    partitions: list[dict],
    state_partition_context: types.Context,
) -> dict | None:
    found = [
        partition_state
        for partition_state in partitions
        if partition_state["context"] == state_partition_context
    ]
    if len(found) > 1:
        msg = (
            "State file contains duplicate entries for partition: "
            f"{state_partition_context}.\nMatching state values were: {found!s}"
        )
        raise ValueError(msg)
    return found[0] if found else None


def _create_in_partitions_list(
    partitions: list[dict],
    state_partition_context: types.Context,
) -> dict:
    # Existing partition not found. Creating new state entry in partitions list...
    new_partition_state = {"context": state_partition_context}
    partitions.append(new_partition_state)
    return new_partition_state


def get_writeable_state_dict(
    tap_state: dict,
    tap_stream_id: str,
    state_partition_context: types.Context | None = None,
) -> dict:
    """Return the stream or partition state, creating a new one if it does not exist.

    Args:
        tap_state: the existing state dict which contains all streams.
        tap_stream_id: the id of the stream
        state_partition_context: keys which identify the partition context,
            by default None (not partitioned)

    Returns:
        Returns a writeable dict at the stream or partition level.

    Raises:
        ValueError: Raise an error if duplicate entries are found.
    """
    if tap_state is None:
        msg = "Cannot write state to missing state dictionary."
        raise ValueError(msg)

    if "bookmarks" not in tap_state:
        tap_state["bookmarks"] = {}
    if tap_stream_id not in tap_state["bookmarks"]:
        tap_state["bookmarks"][tap_stream_id] = {}
    stream_state = t.cast(dict, tap_state["bookmarks"][tap_stream_id])
    if not state_partition_context:
        return stream_state

    if "partitions" not in stream_state:
        stream_state["partitions"] = []
    stream_state_partitions: list[dict] = stream_state["partitions"]
    if found := _find_in_partitions_list(
        stream_state_partitions,
        state_partition_context,
    ):
        return found

    return _create_in_partitions_list(stream_state_partitions, state_partition_context)


def write_stream_state(
    tap_state: dict,
    tap_stream_id: str,
    key: str,
    val: t.Any,  # noqa: ANN401
    *,
    state_partition_context: dict | None = None,
) -> None:
    """Write stream state."""
    state_dict = get_writeable_state_dict(
        tap_state,
        tap_stream_id,
        state_partition_context=state_partition_context,
    )
    state_dict[key] = val


def reset_state_progress_markers(stream_or_partition_state: dict) -> dict | None:
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
    new_signpost_value: t.Any,  # noqa: ANN401
) -> None:
    """Write signpost value."""
    stream_or_partition_state[SIGNPOST_MARKER] = to_json_compatible(new_signpost_value)


def write_starting_replication_value(
    stream_or_partition_state: dict,
    initial_value: t.Any,  # noqa: ANN401
) -> None:
    """Write initial replication value to state."""
    stream_or_partition_state[STARTING_MARKER] = to_json_compatible(initial_value)


def get_starting_replication_value(stream_or_partition_state: dict) -> t.Any | None:  # noqa: ANN401
    """Retrieve initial replication marker value from state."""
    if not stream_or_partition_state:
        return None
    return stream_or_partition_state.get(STARTING_MARKER)


def increment_state(
    stream_or_partition_state: dict,
    *,
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
                PROGRESS_MARKER_NOTE: "Progress is not resumable if interrupted.",
            }
            logger.warning(
                "Stream is assumed to be unsorted, progress is not resumable if "
                "interrupted",
                extra={"replication_key": replication_key},
            )
        progress_dict = stream_or_partition_state[PROGRESS_MARKERS]
    old_rk_value = to_json_compatible(progress_dict.get("replication_key_value"))
    new_rk_value = to_json_compatible(latest_record[replication_key])

    if new_rk_value is None:
        logger.warning("New replication value is null")
        return

    if old_rk_value is None or not check_sorted or new_rk_value >= old_rk_value:
        progress_dict["replication_key"] = replication_key
        progress_dict["replication_key_value"] = new_rk_value
        return

    if is_sorted:
        msg = (
            f"Unsorted data detected in stream. Latest value '{new_rk_value}' is "
            f"smaller than previous max '{old_rk_value}'."
        )
        raise InvalidStreamSortException(msg)


def _greater_than_signpost(
    signpost: _T,
    new_value: _T,
) -> bool:
    """Compare and return True if new_value is greater than signpost."""
    # fails if signpost and bookmark are incompatible types
    return new_value > signpost


def is_state_non_resumable(stream_or_partition_state: dict) -> bool:
    """Return True when state is non-resumable.

    This is determined by checking for a "progress marker" tag in the state artifact.
    """
    return PROGRESS_MARKERS in stream_or_partition_state


def finalize_state_progress_markers(stream_or_partition_state: dict) -> dict | None:
    """Promote or wipe progress markers once sync is complete.

    This marks any non-resumable progress markers as finalized. If there are
    valid bookmarks present, they will be promoted to be resumable.
    """
    signpost_value = stream_or_partition_state.pop(SIGNPOST_MARKER, None)
    stream_or_partition_state.pop(STARTING_MARKER, None)
    if (
        is_state_non_resumable(stream_or_partition_state)
        and "replication_key" in stream_or_partition_state[PROGRESS_MARKERS]
    ):
        # Replication keys valid (only) after sync is complete
        progress_markers = stream_or_partition_state[PROGRESS_MARKERS]
        stream_or_partition_state["replication_key"] = progress_markers.pop(
            "replication_key",
        )
        new_rk_value = progress_markers.pop("replication_key_value")
        if signpost_value and _greater_than_signpost(signpost_value, new_rk_value):
            new_rk_value = signpost_value
        stream_or_partition_state["replication_key_value"] = new_rk_value
    # Wipe and return any markers that have not been promoted
    return reset_state_progress_markers(stream_or_partition_state)


def log_sort_error(
    *,
    ex: Exception,
    log_fn: t.Callable,
    stream_name: str,
    current_context: types.Context | None,
    state_partition_context: types.Context | None,
    record_count: int,
    partition_record_count: int,
) -> None:
    """Log a sort error."""
    msg = f"Sorting error detected in '{stream_name}' on record #{record_count}. "
    if partition_record_count != record_count:
        msg += (
            f"Record was partition record "
            f"#{partition_record_count} with"
            f" state partition context {state_partition_context}. "
        )
    if current_context:
        msg += f"Context was {current_context!s}. "
    msg += str(ex)
    log_fn(msg)
