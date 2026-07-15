"""Functional helpers for Singer catalog metadata.

Ports the ``singer.metadata`` module from ``singer-python`` (Apache-2.0),
operating on the legacy compiled-map representation: a dictionary keyed by
breadcrumb tuples, e.g. ``()`` for the stream and ``("properties", "id")``
for a field.

For an object-oriented interface, see
:class:`singer.catalog.MetadataMapping`.
"""

from __future__ import annotations

import typing as t
from collections import defaultdict
from collections.abc import Mapping

__all__ = [
    "delete",
    "get",
    "get_standard_metadata",
    "new",
    "to_list",
    "to_map",
    "write",
]

Breadcrumb = tuple[str, ...]
CompiledMetadata = dict[Breadcrumb, dict[str, t.Any]]

#: Anything with breadcrumb keys and either raw dicts or objects exposing
#: ``to_dict()`` as values, e.g. a :class:`singer.catalog.MetadataMapping`.
_CompiledMetadataLike = Mapping[Breadcrumb, t.Any]


def new() -> CompiledMetadata:
    """Create a new, empty compiled metadata mapping.

    Returns:
        An empty compiled metadata mapping.
    """
    return defaultdict(dict)


def to_map(
    raw_metadata: t.Iterable[dict[str, t.Any]] | _CompiledMetadataLike,
) -> CompiledMetadata:
    """Compile metadata entries into a map keyed by breadcrumb.

    Accepts the legacy list-of-entries form (``[{"breadcrumb": [...],
    "metadata": {...}}, ...]``), as well as anything already keyed by
    breadcrumb, such as an already-compiled mapping or a
    :class:`singer.catalog.MetadataMapping` — in the latter case, each value
    is converted to a raw dictionary via its ``to_dict()`` method.

    Args:
        raw_metadata: Metadata entries as found in a catalog.

    Returns:
        A compiled metadata mapping.
    """
    if isinstance(raw_metadata, Mapping):
        return _compile_mapping(t.cast("_CompiledMetadataLike", raw_metadata))
    return {tuple(md["breadcrumb"]): md["metadata"] for md in raw_metadata}


def _compile_mapping(mapping: _CompiledMetadataLike) -> CompiledMetadata:
    return {
        breadcrumb: (entry.to_dict() if hasattr(entry, "to_dict") else entry)
        for breadcrumb, entry in mapping.items()
    }


def to_list(compiled_metadata: CompiledMetadata) -> list[dict[str, t.Any]]:
    """Convert a compiled metadata mapping back to a list of entries.

    Args:
        compiled_metadata: A compiled metadata mapping.

    Returns:
        Metadata entries as found in a catalog.
    """
    return [
        {"breadcrumb": list(k), "metadata": v} for k, v in compiled_metadata.items()
    ]


def delete(compiled_metadata: CompiledMetadata, breadcrumb: Breadcrumb, k: str) -> None:
    """Delete a metadata key for a breadcrumb.

    Args:
        compiled_metadata: A compiled metadata mapping.
        breadcrumb: The breadcrumb tuple.
        k: The metadata key to delete.
    """
    del compiled_metadata[breadcrumb][k]


def write(
    compiled_metadata: CompiledMetadata,
    breadcrumb: Breadcrumb,
    k: str,
    val: t.Any,  # noqa: ANN401
) -> CompiledMetadata:
    """Set a metadata key for a breadcrumb.

    Creates the breadcrumb entry if it does not already exist.

    Args:
        compiled_metadata: A compiled metadata mapping.
        breadcrumb: The breadcrumb tuple.
        k: The metadata key to set.
        val: The metadata value.

    Returns:
        The compiled metadata mapping.

    Raises:
        ValueError: If the value is None.
    """
    if val is None:
        msg = "Cannot write a None value"
        raise ValueError(msg)

    breadcrumb_metadata = compiled_metadata.get(breadcrumb, {})
    breadcrumb_metadata[k] = val
    compiled_metadata[breadcrumb] = breadcrumb_metadata

    return compiled_metadata


def get(
    compiled_metadata: CompiledMetadata,
    breadcrumb: Breadcrumb,
    k: str,
) -> t.Any:  # noqa: ANN401
    """Get a metadata key for a breadcrumb.

    Args:
        compiled_metadata: A compiled metadata mapping.
        breadcrumb: The breadcrumb tuple.
        k: The metadata key to get.

    Returns:
        The metadata value, or None if absent.
    """
    return compiled_metadata.get(breadcrumb, {}).get(k)


def get_standard_metadata(
    schema: dict[str, t.Any] | None = None,
    schema_name: str | None = None,
    key_properties: list[str] | None = None,
    valid_replication_keys: list[str] | None = None,
    replication_method: str | None = None,
) -> list[dict[str, t.Any]]:
    """Build standard metadata for a stream.

    Args:
        schema: The stream's JSON schema.
        schema_name: The schema name.
        key_properties: The stream's key properties.
        valid_replication_keys: Fields that can be used as replication keys.
        replication_method: The forced replication method.

    Returns:
        Metadata entries as found in a catalog.
    """
    mdata: CompiledMetadata = {}

    if key_properties is not None:
        mdata = write(mdata, (), "table-key-properties", key_properties)
    if replication_method:
        mdata = write(mdata, (), "forced-replication-method", replication_method)
    if valid_replication_keys is not None:
        mdata = write(mdata, (), "valid-replication-keys", valid_replication_keys)
    if schema:
        mdata = write(mdata, (), "inclusion", "available")

        if schema_name:
            mdata = write(mdata, (), "schema-name", schema_name)
        for field_name in schema.get("properties", {}):
            if (key_properties and field_name in key_properties) or (
                valid_replication_keys and field_name in valid_replication_keys
            ):
                inclusion = "automatic"
            else:
                inclusion = "available"

            mdata = write(mdata, ("properties", field_name), "inclusion", inclusion)

    return to_list(mdata)
