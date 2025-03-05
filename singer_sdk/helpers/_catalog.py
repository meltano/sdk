"""Private helper functions for catalog and selection logic."""

from __future__ import annotations

import typing as t
from copy import deepcopy

from singer_sdk.helpers._typing import is_object_type

if t.TYPE_CHECKING:
    from singer_sdk.singerlib import Catalog, SelectionMask


# TODO: this was previously cached using the `memoization` library. However, the
# `functools.lru_cache` decorator does not support non-hashable arguments.
# It is possible that this function is not a bottleneck, but if it is, we should
# consider implementing a custom LRU cache decorator that supports non-hashable
# arguments.
def get_selected_schema(
    stream_name: str,
    schema: dict,
    mask: SelectionMask,
) -> dict:
    """Return a copy of the provided JSON schema, dropping any fields not selected."""
    new_schema = deepcopy(schema)
    _pop_deselected_schema(new_schema, mask, stream_name, ())
    return new_schema


def _pop_deselected_schema(
    schema: dict,
    mask: SelectionMask,
    stream_name: str,
    breadcrumb: tuple[str, ...],
) -> None:
    """Remove anything from schema that is not selected.

    Walk through schema, starting at the index in breadcrumb, recursively updating in
    place.
    """
    schema_at_breadcrumb = schema
    for crumb in breadcrumb:
        schema_at_breadcrumb = schema_at_breadcrumb.get(crumb, {})

    if not isinstance(schema_at_breadcrumb, dict):  # pragma: no cover
        msg = (  # type: ignore[unreachable]
            "Expected dictionary type instead of "
            f"'{type(schema_at_breadcrumb).__name__}' '{schema_at_breadcrumb}' for "
            f"'{stream_name}' bookmark '{breadcrumb!s}' in '{schema}'"
        )
        # TODO: this should be a ValueError, but it's a breaking change.
        raise ValueError(msg)  # noqa: TRY004

    if "properties" not in schema_at_breadcrumb:
        return

    for property_name, property_def in list(schema_at_breadcrumb["properties"].items()):
        property_breadcrumb: tuple[str, ...] = (
            *breadcrumb,
            "properties",
            property_name,
        )
        selected = mask[property_breadcrumb]
        if not selected:
            schema_at_breadcrumb["properties"].pop(property_name, None)
            continue

        if is_object_type(property_def):
            # call recursively in case any subproperties are deselected.
            _pop_deselected_schema(
                schema,
                mask,
                stream_name,
                property_breadcrumb,
            )


def pop_deselected_record_properties(
    record: dict[str, t.Any],
    schema: dict,
    mask: SelectionMask,
    breadcrumb: tuple[str, ...] = (),
) -> None:
    """Remove anything from record properties that is not selected.

    Walk through properties, starting at the index in breadcrumb, recursively
    updating in place.
    """
    for property_name, val in record.copy().items():
        property_breadcrumb = (*breadcrumb, "properties", property_name)
        selected = mask[property_breadcrumb]
        if not selected:
            record.pop(property_name)
            continue

        if isinstance(val, dict):
            # call recursively in case any subproperties are deselected.
            pop_deselected_record_properties(
                val,
                schema,
                mask,
                property_breadcrumb,
            )


def deselect_all_streams(catalog: Catalog) -> None:
    """Deselect all streams in catalog dictionary."""
    for entry in catalog.streams:
        set_catalog_stream_selected(catalog, entry.tap_stream_id, selected=False)


def set_catalog_stream_selected(
    catalog: Catalog,
    stream_name: str,
    *,
    selected: bool,
    breadcrumb: tuple[str, ...] | None = None,
) -> None:
    """Return True if the property is selected for extract.

    Breadcrumb of `[]` or `None` indicates the stream itself. Otherwise, the
    breadcrumb is the path to a property within the stream.
    """
    breadcrumb = breadcrumb or ()
    if not isinstance(breadcrumb, tuple):  # pragma: no cover
        msg = (  # type: ignore[unreachable]
            f"Expected tuple value for breadcrumb '{breadcrumb}'. Got "
            f"{type(breadcrumb).__name__}"
        )
        # TODO: this should be a ValueError, but it's a breaking change.
        raise ValueError(msg)  # noqa: TRY004

    catalog_entry = catalog.get_stream(stream_name)
    if not catalog_entry:
        msg = f"Catalog entry missing for '{stream_name}'. Skipping."
        raise ValueError(msg)

    md_entry = catalog_entry.metadata[breadcrumb]
    md_entry.selected = selected
