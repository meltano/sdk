"""Private helper functions for catalog and selection logic."""

from copy import deepcopy
from typing import Optional, List
from singer_sdk.helpers._typing import is_object_type


def get_selected_schema(schema: dict, catalog_entry: dict) -> dict:
    """Return a copy of the provided JSON schema, dropping any fields not selected."""
    result = deepcopy(schema)  # we don't want to modify the original
    _pop_deselected_schema(result, catalog_entry, [])
    return result


def _pop_deselected_schema(
    schema: dict, catalog_entry: dict, breadcrumb: List[str]
) -> None:
    """Pop anything from schema that is not selected.

    Walk through schema, starting at the index in breadcrumb, recursively update in
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
