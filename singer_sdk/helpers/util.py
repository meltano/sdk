"""General helper functions, helper classes, and decorators."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path, PurePath

import pendulum
from typing import Any, Dict, List, Optional, Union, cast


def read_json_file(path: Union[PurePath, str]) -> Dict[str, Any]:
    """Read json file, thowing an error if missing."""
    if not path:
        raise RuntimeError("Could not open file. Filepath not provided.")
    if Path(path).exists():
        return json.loads(Path(path).read_text())
    else:
        msg = f"File at '{path}' was not found."
        for template in [f"{path}.template"]:
            if Path(template).exists():
                msg += f"\nFor more info, please see the sample template at: {template}"
        raise FileExistsError(msg)


def utc_now():
    """Return current time in UTC."""
    return pendulum.utcnow()


def get_catalog_entries(catalog_dict: dict) -> List[dict]:
    """Parse the catalog dict and return a list of catalog entries."""
    if "streams" not in catalog_dict:
        raise ValueError("Catalog does not contain expected 'streams' collection.")
    if not catalog_dict.get("streams"):
        raise ValueError("Catalog does not contain any streams.")
    return cast(List[dict], catalog_dict.get("streams"))


def get_catalog_entry_name(catalog_entry: dict) -> str:
    """Return the name of the provided catalog entry dict."""
    result = catalog_entry.get("stream", catalog_entry.get("tap_stream_id", None))
    if not result:
        raise ValueError(
            "Stream name could not be identified due to missing or blank"
            "'stream' and 'tap_stream_id' values."
        )
    return result


def get_catalog_entry_schema(catalog_entry: dict) -> dict:
    """Return the JSON Schema dict for the specified catalog entry dict."""
    result = catalog_entry.get("schema", None)
    if not result:
        raise ValueError(
            "Stream does not have a valid schema. Please check that the catalog file "
            "is properly formatted."
        )
    return result


def get_selected_schema(schema: dict, catalog_entry: dict) -> dict:
    """Return a copy of the provided JSON schema, dropping any fields not selected."""
    result = deepcopy(schema)  # we don't want to modify the original
    _pop_deselected_schema(result, catalog_entry, [])
    return result


def _pop_deselected_schema(
    schema: dict, catalog_entry: dict, breadcrumb: List[str]
) -> None:
    """Pop anything from schema that is not selected.

    Walk through schema, starting at the inde in breadcrumb, recursivley update in
    place.
    """
    for prop, val in schema.items():
        if not schema.get("selected", True) in [True, "selected"]:
            schema.pop(prop)
            continue
        md = _get_metadata_by_breadcrumb(catalog_entry, breadcrumb + [prop])
        if md and not md.get("selected", True) in [True, "selected"]:
            schema.pop(prop)
            continue
        if is_object_type(val):
            # call recursively in case any subproperties are deselected.
            _pop_deselected_schema(val, catalog_entry, breadcrumb + [prop])


def _get_metadata_by_breadcrumb(
    catalog_entry: dict, breadcrumb: List[str]
) -> Optional[dict]:
    md: Optional[dict] = catalog_entry.get("metadata")
    if not md:
        return None
    for md_entry in md:
        if md_entry.get("breadcrumb", None) == breadcrumb:
            return md_entry
    return None


def get_property_schema(schema: dict, property: str) -> Optional[dict]:
    """Given the provided JSON Schema, return the property by name specified.

    If property name does not exist in schema, return None.
    """
    if property not in schema["properties"]:
        return None
    return schema["properties"][property]


def is_boolean_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is a boolean or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "boolean" in property_type or property_type == "boolean":
            return True
    return False


def is_object_type(property_schema: dict) -> Optional[bool]:
    """Return true if the JSON Schema type is an object or None if detection fails."""
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "object" in property_type or property_type == "object":
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
