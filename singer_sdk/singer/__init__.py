"""Singer Protocol package."""

from singer_sdk.singer.catalog import (
    Catalog,
    CatalogEntry,
    Metadata,
    MetadataMapping,
    SelectionMask,
    StreamMetadata,
)
from singer_sdk.singer.messages import (
    ActivateVersionMessage,
    Message,
    RecordMessage,
    SchemaMessage,
    SingerMessageType,
    StateMessage,
    exclude_null_dict,
    write_message,
)
from singer_sdk.singer.schema import Schema
