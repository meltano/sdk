"""Low-level Singer components for building taps and targets.

A lightweight, (mostly) drop-in replacement for ``singer-python`` and
``pipelinewise-singer-python``.
"""

from __future__ import annotations

from singer import (
    bookmarks,
    catalog,
    exceptions,
    messages,
    metrics,
    schema,
    utils,
)
from singer._dropin import (
    write_message,
    write_record,
    write_schema,
    write_state,
    write_version,
)
from singer.batch import BaseBatchFileEncoding, BatchMessage
from singer.bookmarks import (
    clear_bookmark,
    clear_offset,
    get_bookmark,
    get_currently_syncing,
    get_offset,
    reset_stream,
    set_currently_syncing,
    set_offset,
    write_bookmark,
)
from singer.catalog import (
    Catalog,
    CatalogEntry,
    Metadata,
    MetadataMapping,
    SelectionMask,
    StreamMetadata,
)
from singer.logger import get_logger
from singer.messages import (
    ActivateVersionMessage,
    Message,
    RecordMessage,
    SchemaMessage,
    SingerMessageType,
    StateMessage,
    exclude_null_dict,
    format_message,
)
from singer.schema import Schema, resolve_schema_references
from singer.utils import strftime, strptime_to_utc

__all__ = [
    "ActivateVersionMessage",
    "BaseBatchFileEncoding",
    "BatchMessage",
    "Catalog",
    "CatalogEntry",
    "Message",
    "Metadata",
    "MetadataMapping",
    "RecordMessage",
    "Schema",
    "SchemaMessage",
    "SelectionMask",
    "SingerMessageType",
    "StateMessage",
    "StreamMetadata",
    "bookmarks",
    "catalog",
    "clear_bookmark",
    "clear_offset",
    "exceptions",
    "exclude_null_dict",
    "format_message",
    "get_bookmark",
    "get_currently_syncing",
    "get_logger",
    "get_offset",
    "messages",
    "metrics",
    "reset_stream",
    "resolve_schema_references",
    "schema",
    "set_currently_syncing",
    "set_offset",
    "strftime",
    "strptime_to_utc",
    "utils",
    "write_bookmark",
    "write_message",
    "write_record",
    "write_schema",
    "write_state",
    "write_version",
]
