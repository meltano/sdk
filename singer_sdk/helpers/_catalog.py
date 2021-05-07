"""Private helper functions for catalog and selection logic."""

from copy import deepcopy
from typing import Optional, Tuple, cast
from logging import Logger

from singer import metadata
from singer.catalog import Catalog

from singer_sdk.helpers._typing import is_object_type

from memoization import cached


_MAX_LRU_CACHE = 500


def is_stream_selected(
    catalog: Optional[dict],
    stream_name: str,
    logger: Logger,
):
    """Return True if the stream is selected for extract."""
    return is_property_selected(catalog, stream_name, breadcrumb=(), logger=logger)


@cached(max_size=_MAX_LRU_CACHE)
def is_property_selected(  # noqa: C901  # ignore 'too complex'
    catalog: Optional[dict],
    stream_name: str,
    breadcrumb: Optional[Tuple[str, ...]],
    logger: Logger,
) -> bool:
    """Return True if the property is selected for extract.

    Breadcrumb of `[]` or `None` indicates the stream itself. Otherwise, the
    breadcrumb is the path to a property within the stream.
    """
    breadcrumb = breadcrumb or cast(Tuple[str, ...], ())
    if isinstance(breadcrumb, str):
        breadcrumb = tuple([breadcrumb])
    if not isinstance(breadcrumb, tuple):
        raise ValueError(
            f"Expected tuple value for breadcrumb '{breadcrumb}'. "
            f"Got {type(breadcrumb).__name__}"
        )

    if not catalog:
        return True

    catalog_obj = Catalog.from_dict(catalog)
    catalog_entry = catalog_obj.get_stream(stream_name)
    if not catalog_entry:
        logger.warning(f"Catalog entry missing for '{stream_name}'. Skipping.")
        return False

    if not catalog_entry.metadata:
        return True

    md_map = metadata.to_map(catalog_entry.metadata)
    md_entry = md_map.get(breadcrumb)
    parent_value = None
    if len(breadcrumb) > 0:
        parent_value = is_property_selected(
            catalog, stream_name, tuple(list(breadcrumb)[:-1]), logger
        )
    if parent_value is False:
        return parent_value

    if not md_entry:
        logger.warning(
            f"Catalog entry missing for '{stream_name}':'{breadcrumb}'. "
            f"Using parent value of selected={parent_value}."
        )
        return parent_value or False

    if md_entry.get("inclusion") == "unsupported":
        return False

    if md_entry.get("inclusion") == "automatic":
        if md_entry.get("selected") is False:
            logger.warning(
                f"Property '{':'.join(breadcrumb)}' was deselected while also set"
                "for automatic inclusion. Ignoring selected==False input."
            )
        return True

    if "selected" in md_entry:
        return md_entry["selected"]

    if md_entry.get("inclusion") == "available":
        return True

    raise ValueError(
        f"Could not detect selection status for '{stream_name}' breadcrumb "
        f"'{breadcrumb}' using metadata: {md_map}"
    )


@cached(max_size=_MAX_LRU_CACHE)
def get_selected_schema(catalog: dict, stream_name: str, logger: Logger) -> dict:
    """Return a copy of the provided JSON schema, dropping any fields not selected."""
    catalog_obj = Catalog.from_dict(catalog)
    catalog_entry = catalog_obj.get_stream(stream_name)
    schema = deepcopy(catalog_entry.schema.to_dict())
    _pop_deselected_schema(schema, catalog, stream_name, cast(Tuple[str], ()), logger)
    return schema


def _pop_deselected_schema(
    schema: dict,
    catalog: dict,
    stream_name: str,
    breadcrumb: Tuple[str, ...],
    logger: Logger,
) -> None:
    """Remove anything from schema that is not selected.

    Walk through schema, starting at the index in breadcrumb, recursively updating in
    place.
    """
    breadcrumb = breadcrumb or ("properties",)
    for property_name, val in list(schema.get("properties", {}).items()):
        property_breadcrumb: Tuple[str, ...] = tuple(list(breadcrumb) + [property_name])
        selected = is_property_selected(
            catalog, stream_name, property_breadcrumb, logger
        )
        if not selected:
            schema["properties"].pop(property_name)
            continue

        if is_object_type(val):
            # call recursively in case any subproperties are deselected.
            _pop_deselected_schema(
                val, catalog, stream_name, property_breadcrumb, logger
            )


def pop_deselected_record_properties(
    record: dict,
    catalog: dict,
    stream_name: str,
    logger: Logger,
    breadcrumb: Tuple[str, ...] = (),
) -> None:
    """Remove anything from record properties that is not selected.

    Walk through properties, starting at the index in breadcrumb, recursively
    updating in place.
    """
    for property_name, val in list(record.items()):
        property_breadcrumb: Tuple[str, ...] = tuple(list(breadcrumb) + [property_name])
        selected = is_property_selected(
            catalog, stream_name, property_breadcrumb, logger
        )
        if not selected:
            record.pop(property_name)
            continue

        if isinstance(val, dict):
            # call recursively in case any subproperties are deselected.
            pop_deselected_record_properties(
                val, catalog, stream_name, logger, property_breadcrumb
            )
