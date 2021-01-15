"""Helper functions, helper classes, and decorators."""

import pytz

from datetime import datetime
from typing import Any, List, Optional, Tuple, Union, cast

import singer


COMMON_SECRET_KEYS = [
    "db_password",
    "password",
    "access_key",
    "private_key",
    "client_id",
    "client_secret",
    "refresh_token",
    "access_token",
]
COMMON_SECRET_KEY_SUFFIXES = ["access_key_id"]


def is_common_secret_key(key_name: str) -> bool:
    """Return true if the key_name value matches a known secret name or pattern."""
    if key_name in COMMON_SECRET_KEYS:
        return True
    if any(
        [
            key_name.lower().endswith(key_suffix)
            for key_suffix in COMMON_SECRET_KEY_SUFFIXES
        ]
    ):
        return True
    return False


class SecretString(str):
    """For now, this class wraps a sensitive string to be identified as such later."""

    def __init__(self, contents):
        self.contents = contents

    def __repr__(self) -> str:
        return self.contents.__repr__()

    def __str__(self) -> str:
        return self.contents.__str__()


class classproperty(property):
    def __get__(self, obj, objtype=None):
        return super(classproperty, self).__get__(objtype)

    def __set__(self, obj, value):
        super(classproperty, self).__set__(type(obj), value)

    def __delete__(self, obj):
        super(classproperty, self).__delete__(type(obj))


def utc_now():
    return datetime.utcnow().replace(tzinfo=pytz.UTC)


def get_catalog_entries(catalog_dict: dict) -> List[dict]:
    if "streams" not in catalog_dict:
        raise ValueError("Catalog does not contain expected 'streams' collection.")
    if not catalog_dict.get("streams"):
        raise ValueError("Catalog does not contain any streams.")
    return cast(List[dict], catalog_dict.get("streams"))


def get_catalog_entry_name(catalog_entry: dict) -> str:
    result = catalog_entry.get("stream", catalog_entry.get("tap_stream_id", None))
    if not result:
        raise ValueError(
            "Stream name could not be identified due to missing or blank"
            "'stream' and 'tap_stream_id' values."
        )
    return result


def get_catalog_entry_schema(catalog_entry: dict) -> dict:
    result = catalog_entry.get("schema", None)
    if not result:
        raise ValueError(
            "Stream does not have a valid schema. Please check that the catalog file "
            "is properly formatted."
        )
    return result


def get_property_schema(schema: dict, property: str, warn=True) -> Optional[dict]:
    if property not in schema["properties"]:
        return None
    return schema["properties"][property]


def is_boolean_type(property_schema: dict) -> Optional[bool]:
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "boolean" in property_type or property_type == "boolean":
            return True
    return False


def ensure_stream_state_exists(
    state: dict, tap_stream_id: str, substream: str = None
) -> None:
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    if tap_stream_id not in state["bookmarks"]:
        state["bookmarks"][tap_stream_id] = {}
    if substream:
        if "substreams" not in state["bookmarks"][tap_stream_id]:
            state["bookmarks"][tap_stream_id]["substreams"] = {}
        if substream not in state["bookmarks"][tap_stream_id]["substreams"]:
            state["bookmarks"][tap_stream_id]["substreams"][substream] = {}


def write_stream_state(state, stream_id_or_tuple: Union[str, Tuple], key, val) -> None:
    tap_stream_id, substream = _parse_stream_and_substream(stream_id_or_tuple)
    ensure_stream_state_exists(state, tap_stream_id, substream)
    if not substream:
        state["bookmarks"][tap_stream_id][key] = val
    else:
        state["bookmarks"][tap_stream_id]["substreams"][substream][key] = val


def get_state_substream_ids(state, stream_id: str) -> Optional[List[str]]:
    substreams = read_stream_state(state, stream_id, default={}).get("substreams")
    if not substreams:
        return None
    return substreams.keys()


def read_stream_state(
    state, stream_id_or_tuple: Union[str, Tuple], key=None, default: Any = None
) -> Any:
    tap_stream_id, substream = _parse_stream_and_substream(stream_id_or_tuple)
    ensure_stream_state_exists(state, tap_stream_id, substream)
    if not substream:
        state_dict = state.get("bookmarks", {}).get(tap_stream_id, {})
    else:
        state_dict = (
            state.get("bookmarks", {})
            .get(tap_stream_id, {})
            .get("substreams", {})
            .get(substream, {})
        )
    if key:
        return state_dict.get(key, default)
    else:
        return state_dict or default


def wipe_stream_state(state, stream_id_or_tuple: Union[str, Tuple], key):
    tap_stream_id, substream = _parse_stream_and_substream(stream_id_or_tuple)
    ensure_stream_state_exists(state, tap_stream_id, substream)
    if not substream:
        return state.get("bookmarks", {}).get(tap_stream_id, {}).pop(key)
    else:
        return (
            state.get("bookmarks", {})
            .get(tap_stream_id, {})
            .get("substreams", {})
            .get(substream, {})
            .pop(key)
        )


def _parse_stream_and_substream(
    stream_id_or_tuple: Union[str, Tuple]
) -> Tuple[str, Optional[str]]:
    substream: Optional[str] = None
    if isinstance(stream_id_or_tuple, str):
        tap_stream_id: str = stream_id_or_tuple
    else:
        tap_stream_id = stream_id_or_tuple[0]
        if len(tap_stream_id) > 1:
            substream = tap_stream_id[1]
    return tap_stream_id, substream
