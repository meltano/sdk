"""Helper functions, helper classes, and decorators."""

from decimal import Decimal
import pytz

from datetime import datetime
from typing import Any, List, Optional, cast


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


def get_stream_state_dict(
    state: dict, tap_stream_id: str, shard: Optional[dict] = None
) -> dict:
    """Return the stream or shard state, creating a new one if it does not exist.

    Parameters
    ----------
    state : dict
        the existing state dict which contains all streams.
    tap_stream_id : str
        the id of the stream
    shard : Optional[dict], optional
        keys which identify the shard context, by default None (treat as non-sharded)

    Returns
    -------
    dict
        Returns a writeable dict at the stream or shard level.

    Raises
    ------
    ValueError
        Raise an error if duplicate entries are found.
    """
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    if tap_stream_id not in state["bookmarks"]:
        state["bookmarks"][tap_stream_id] = {}
    if shard:
        if "shards" not in state["bookmarks"][tap_stream_id]:
            state["bookmarks"][tap_stream_id]["shards"] = []
        found = [
            shard
            for shard in state["bookmarks"][tap_stream_id]["shards"]
            if shard.get("context") == shard
        ]
        if len(found) > 1:
            raise ValueError(
                f"State file contains duplicate entries for shard definition: {shard}"
            )
        if not found:
            new_dict = {"context": shard}
            state["bookmarks"][tap_stream_id]["shards"].append(new_dict)
            return new_dict
        return found[0]


def read_stream_state(
    state,
    tap_stream_id: str,
    key=None,
    default: Any = None,
    *,
    shard: Optional[dict] = None,
) -> Any:
    state_dict = get_stream_state_dict(state, tap_stream_id, shard=shard)
    if key:
        return state_dict.get(key, default)
    return state_dict or default


def write_stream_state(
    state, tap_stream_id: str, key, val, *, shard: Optional[dict] = None
) -> None:
    state_dict = get_stream_state_dict(state, tap_stream_id, shard=shard)
    state_dict[key] = val


def wipe_stream_state(
    state, tap_stream_id: str, key, *, shard: Optional[dict] = None
) -> None:
    state_dict = get_stream_state_dict(state, tap_stream_id, shard=shard)
    state_dict.pop(key)


def _float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: _float_to_decimal(v) for k, v in value.items()}
    return value
