"""Bookmark and state helpers.

Ports the ``singer.bookmarks`` module from ``singer-python`` (Apache-2.0),
operating on the legacy nested state layout::

    {
        "bookmarks": {
            "my_stream": {"replication_key_value": "...", "offset": {...}},
        },
        "currently_syncing": "my_stream",
    }
"""

from __future__ import annotations

import typing as t

__all__ = [
    "clear_bookmark",
    "clear_offset",
    "ensure_bookmark_path",
    "get_bookmark",
    "get_currently_syncing",
    "get_offset",
    "reset_stream",
    "set_currently_syncing",
    "set_offset",
    "write_bookmark",
]

_State: t.TypeAlias = "dict[str, t.Any]"


def ensure_bookmark_path(state: _State, path: t.Iterable[t.Any]) -> _State:
    """Ensure that a nested dictionary path exists in the state.

    Args:
        state: The state dictionary.
        path: The sequence of keys to create if missing.

    Returns:
        The state dictionary.
    """
    submap = state
    for path_component in path:
        if submap.get(path_component) is None:
            submap[path_component] = {}
        submap = submap[path_component]
    return state


def write_bookmark(
    state: _State,
    tap_stream_id: str,
    key: str,
    val: t.Any,  # noqa: ANN401
) -> _State:
    """Write a bookmark value for a stream.

    Args:
        state: The state dictionary.
        tap_stream_id: The stream identifier.
        key: The bookmark key.
        val: The bookmark value.

    Returns:
        The state dictionary.
    """
    state = ensure_bookmark_path(state, ["bookmarks", tap_stream_id])
    state["bookmarks"][tap_stream_id][key] = val
    return state


def get_bookmark(
    state: _State,
    tap_stream_id: str,
    key: str,
    default: t.Any = None,  # noqa: ANN401
) -> t.Any:  # noqa: ANN401
    """Get a bookmark value for a stream.

    Args:
        state: The state dictionary.
        tap_stream_id: The stream identifier.
        key: The bookmark key.
        default: The value to return if the bookmark is absent.

    Returns:
        The bookmark value, or the default.
    """
    return state.get("bookmarks", {}).get(tap_stream_id, {}).get(key, default)


def clear_bookmark(state: _State, tap_stream_id: str, key: str) -> _State:
    """Remove a bookmark value for a stream.

    Args:
        state: The state dictionary.
        tap_stream_id: The stream identifier.
        key: The bookmark key.

    Returns:
        The state dictionary.
    """
    state = ensure_bookmark_path(state, ["bookmarks", tap_stream_id])
    state["bookmarks"][tap_stream_id].pop(key, None)
    return state


def reset_stream(state: _State, tap_stream_id: str) -> _State:
    """Reset all bookmarks for a stream.

    Args:
        state: The state dictionary.
        tap_stream_id: The stream identifier.

    Returns:
        The state dictionary.
    """
    state = ensure_bookmark_path(state, ["bookmarks", tap_stream_id])
    state["bookmarks"][tap_stream_id] = {}
    return state


def set_offset(
    state: _State,
    tap_stream_id: str,
    offset_key: str,
    offset_value: t.Any,  # noqa: ANN401
) -> _State:
    """Set an offset value for a stream.

    Args:
        state: The state dictionary.
        tap_stream_id: The stream identifier.
        offset_key: The offset key.
        offset_value: The offset value.

    Returns:
        The state dictionary.
    """
    state = ensure_bookmark_path(state, ["bookmarks", tap_stream_id, "offset"])
    state["bookmarks"][tap_stream_id]["offset"][offset_key] = offset_value
    return state


def clear_offset(state: _State, tap_stream_id: str) -> _State:
    """Clear all offsets for a stream.

    Args:
        state: The state dictionary.
        tap_stream_id: The stream identifier.

    Returns:
        The state dictionary.
    """
    state = ensure_bookmark_path(state, ["bookmarks", tap_stream_id])
    state["bookmarks"][tap_stream_id]["offset"] = {}
    return state


def get_offset(
    state: _State,
    tap_stream_id: str,
    default: t.Any = None,  # noqa: ANN401
) -> t.Any:  # noqa: ANN401
    """Get the offsets for a stream.

    Args:
        state: The state dictionary.
        tap_stream_id: The stream identifier.
        default: The value to return if no offset is present.

    Returns:
        The offset mapping, or the default.
    """
    return state.get("bookmarks", {}).get(tap_stream_id, {}).get("offset", default)


def set_currently_syncing(state: _State, tap_stream_id: str | None) -> _State:
    """Record the stream currently being synced.

    Args:
        state: The state dictionary.
        tap_stream_id: The stream identifier, or None to clear it.

    Returns:
        The state dictionary.
    """
    state["currently_syncing"] = tap_stream_id
    return state


def get_currently_syncing(
    state: _State,
    default: t.Any = None,  # noqa: ANN401
) -> t.Any:  # noqa: ANN401
    """Get the stream currently being synced.

    Args:
        state: The state dictionary.
        default: The value to return if no stream is being synced.

    Returns:
        The stream identifier, or the default.
    """
    return state.get("currently_syncing", default)
