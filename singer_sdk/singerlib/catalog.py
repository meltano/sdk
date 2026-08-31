"""Alias for :mod:`singer.catalog`."""

from __future__ import annotations

from singer.catalog import (
    REPLICATION_FULL_TABLE,
    REPLICATION_INCREMENTAL,
    REPLICATION_LOG_BASED,
    AnyMetadata,
    Breadcrumb,
    Catalog,
    CatalogEntry,
    Metadata,
    MetadataMapping,
    SelectionMask,
    StreamMetadata,
)

__all__ = [
    "REPLICATION_FULL_TABLE",
    "REPLICATION_INCREMENTAL",
    "REPLICATION_LOG_BASED",
    "AnyMetadata",
    "Breadcrumb",
    "Catalog",
    "CatalogEntry",
    "Metadata",
    "MetadataMapping",
    "SelectionMask",
    "StreamMetadata",
]
