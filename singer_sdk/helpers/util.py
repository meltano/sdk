"""General helper functions, helper classes, and decorators."""

from decimal import Decimal
from typing import TypeVar
import pytz

from datetime import datetime
from typing import Callable, List, Optional, Type, cast

T = TypeVar("T")
V = TypeVar("V")


class classproperty:
    def __init__(self, getter: Callable[[Type], V]) -> None:
        self.getter = getter

    def __get__(self, instance: Optional[T], owner: Type[T]) -> V:
        return self.getter(owner)


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


def _float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: _float_to_decimal(v) for k, v in value.items()}
    return value
