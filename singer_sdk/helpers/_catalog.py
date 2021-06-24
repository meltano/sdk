"""Private helper functions for catalog and selection logic."""

from copy import deepcopy
from typing import Optional, Tuple, cast, List
from logging import Logger

from singer.metadata import to_map as metadata_to_map
from singer.catalog import Catalog

from singer_sdk.helpers._typing import is_object_type

from memoization import cached


_MAX_LRU_CACHE = 500


def is_stream_selected(
    catalog: Optional[dict],
    stream_name: str,
    logger: Logger,
) -> bool:
    """Return True if the stream is selected for extract.

    If catalog is None, this function will always return True.
    """
    if catalog is None:
        return True

    catalog_obj = Catalog.from_dict(catalog)
    catalog_entry = catalog_obj.get_stream(stream_name)
    if not catalog_entry:
        logger.warning(f"Catalog entry missing for '{stream_name}'. Skipping.")
        return False

    if not catalog_entry.metadata:
        return True

    return is_property_selected(
        stream_name,
        catalog_entry.schema,
        catalog_entry.metadata,
        breadcrumb=(),
        logger=logger,
    )


@cached(max_size=_MAX_LRU_CACHE)
def is_property_selected(  # noqa: C901  # ignore 'too complex'
    stream_name: str,
    schema: dict,
    metadata: dict,
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

    if not metadata:
        # Default to true if no metadata to say otherwise
        return True

    md_map = metadata_to_map(metadata)
    md_entry = md_map.get(breadcrumb)
    parent_value = None
    if len(breadcrumb) > 0:
        parent_breadcrumb = tuple(list(breadcrumb)[:-2])
        parent_value = is_property_selected(
            stream_name, schema, metadata, parent_breadcrumb, logger
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
        return cast(bool, md_entry["selected"])

    if md_entry.get("inclusion") == "available":
        return True

    raise ValueError(
        f"Could not detect selection status for '{stream_name}' breadcrumb "
        f"'{breadcrumb}' using metadata: {md_map}"
    )


@cached(max_size=_MAX_LRU_CACHE)
def get_selected_schema(
    stream_name: str, schema: dict, metadata: dict, logger: Logger
) -> dict:
    """Return a copy of the provided JSON schema, dropping any fields not selected."""
    new_schema = deepcopy(schema)
    _pop_deselected_schema(
        new_schema, metadata, stream_name, cast(Tuple[str], ()), logger
    )
    return new_schema


def _pop_deselected_schema(
    schema: dict,
    metadata: dict,
    stream_name: str,
    breadcrumb: Tuple[str, ...],
    logger: Logger,
) -> None:
    """Remove anything from schema that is not selected.

    Walk through schema, starting at the index in breadcrumb, recursively updating in
    place.
    """
    schema_at_breadcrumb = schema
    for crumb in breadcrumb:
        schema_at_breadcrumb = schema_at_breadcrumb.get(crumb, {})

    if not isinstance(schema_at_breadcrumb, dict):
        raise ValueError(
            f"Expected dictionary type instead of "
            f"'{type(schema_at_breadcrumb).__name__}' '{schema_at_breadcrumb}' "
            f"for '{stream_name}' bookmark '{str(breadcrumb)}' in '{schema}'"
        )
    for property_name, property_def in list(schema_at_breadcrumb["properties"].items()):
        property_breadcrumb: Tuple[str, ...] = tuple(
            list(breadcrumb) + ["properties", property_name]
        )
        selected = is_property_selected(
            stream_name, schema, metadata, property_breadcrumb, logger
        )
        if not selected:
            schema_at_breadcrumb["properties"].pop(property_name, None)
            continue

        if is_object_type(property_def):
            # call recursively in case any subproperties are deselected.
            _pop_deselected_schema(
                schema, metadata, stream_name, property_breadcrumb, logger
            )


def pop_deselected_record_properties(
    record: dict,
    schema: dict,
    metadata: dict,
    stream_name: str,
    logger: Logger,
    breadcrumb: Tuple[str, ...] = (),
) -> None:
    """Remove anything from record properties that is not selected.

    Walk through properties, starting at the index in breadcrumb, recursively
    updating in place.
    """
    for property_name, val in list(record.items()):
        property_breadcrumb: Tuple[str, ...] = tuple(
            list(breadcrumb) + ["properties", property_name]
        )
        selected = is_property_selected(
            stream_name, schema, metadata, property_breadcrumb, logger
        )
        if not selected:
            record.pop(property_name)
            continue

        if isinstance(val, dict):
            # call recursively in case any subproperties are deselected.
            pop_deselected_record_properties(
                val, schema, metadata, stream_name, logger, property_breadcrumb
            )


def deselect_all_streams(catalog: dict) -> None:
    """Deselect all streams in catalog dictionary."""
    for stream_name in get_catalog_stream_names(catalog):
        set_catalog_stream_selected(catalog, stream_name, selected=False)


def get_catalog_stream_names(catalog: dict) -> List[str]:
    """Deselect all streams in catalog dictionary."""
    return [catalog_entry["stream"] for catalog_entry in catalog.get("streams", [])]


def set_catalog_stream_selected(
    catalog: dict,
    stream_name: str,
    selected: bool,
    breadcrumb: Optional[Tuple[str, ...]] = None,
) -> None:
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

    catalog_obj = Catalog.from_dict(catalog)
    catalog_entry = catalog_obj.get_stream(stream_name)
    if not catalog_entry:
        raise ValueError(f"Catalog entry missing for '{stream_name}'. Skipping.")

    md_map = metadata_to_map(catalog_entry.metadata)
    md_entry = md_map.get(breadcrumb)
    md_entry["selected"] = selected
