import logging
from dataclasses import dataclass, fields
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, cast

from singer.catalog import Catalog as BaseCatalog
from singer.catalog import CatalogEntry as BaseCatalogEntry
from singer.schema import Schema

Breadcrumb = Tuple[str, ...]

logger = logging.getLogger(__name__)


class SelectionMask(Dict[Breadcrumb, bool]):
    """Boolean mask for property selection in schemas and records."""

    def __missing__(self, breadcrumb: Breadcrumb) -> bool:
        """Handle missing breadcrumbs.

        - Properties default to parent value if available.
        - Root (stream) defaults to True.
        """
        if len(breadcrumb) >= 2:
            parent = breadcrumb[:-2]
            return self[parent]
        else:
            return True


@dataclass
class Metadata:
    """Base stream or property metadata."""

    class InclusionType(str, Enum):
        """Catalog inclusion types."""

        AVAILABLE = "available"
        AUTOMATIC = "automatic"
        UNSUPPORTED = "unsupported"

    inclusion: Optional[InclusionType] = None
    selected: Optional[bool] = None
    selected_by_default: Optional[bool] = None

    @classmethod
    def from_dict(cls, value: Dict[str, Any]):
        """Parse metadata dictionary."""
        return cls(
            **{
                field.name: value.get(field.name.replace("_", "-"))
                for field in fields(cls)
            }
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to a JSON-encodeable dictionary."""
        result = {}

        for field in fields(self):
            value = getattr(self, field.name)
            if value is not None:
                result[field.name.replace("_", "-")] = value

        return result


@dataclass
class StreamMetadata(Metadata):
    """Stream metadata."""

    table_key_properties: Optional[List[str]] = None
    forced_replication_method: Optional[str] = None
    valid_replication_keys: Optional[List[str]] = None
    schema_name: Optional[str] = None


class MetadataMapping(Dict[Breadcrumb, Union[Metadata, StreamMetadata]]):
    """Stream metadata mapping."""

    @classmethod
    def from_iterable(cls, iterable: Iterable[Dict[str, Any]]):
        """Create a metadata mapping from an iterable of metadata dictionaries."""
        mapping = cls()
        for d in iterable:
            breadcrumb = tuple(d["breadcrumb"])
            metadata = d["metadata"]
            if breadcrumb:
                mapping[breadcrumb] = Metadata.from_dict(metadata)
            else:
                mapping[breadcrumb] = StreamMetadata.from_dict(metadata)

        return mapping

    def to_list(self) -> List[Dict[str, Any]]:
        """Convert mapping to a JSON-encodable list."""
        return [
            {"breadcrumb": list(k), "metadata": v.to_dict()} for k, v in self.items()
        ]

    def __missing__(self, breadcrumb: Breadcrumb):
        """Handle missing metadata entries."""
        self[breadcrumb] = Metadata() if breadcrumb else StreamMetadata()
        return self[breadcrumb]

    @property
    def root(self):
        """Get stream (root) metadata from this mapping."""
        meta: StreamMetadata = self[()]
        return meta

    @classmethod
    def get_standard_metadata(
        cls,
        schema: Optional[Dict[str, Any]] = None,
        schema_name: Optional[str] = None,
        key_properties: Optional[List[str]] = None,
        valid_replication_keys: Optional[List[str]] = None,
        replication_method: Optional[str] = None,
    ):
        """Get default metadata for a stream."""
        mapping = cls()
        root = StreamMetadata(
            table_key_properties=key_properties,
            forced_replication_method=replication_method,
            valid_replication_keys=valid_replication_keys,
        )

        if schema:
            root.inclusion = Metadata.InclusionType.AVAILABLE

            if schema_name:
                root.schema_name = schema_name

            for field_name in schema.get("properties", {}).keys():
                if key_properties and field_name in key_properties:
                    entry = Metadata(inclusion=Metadata.InclusionType.AUTOMATIC)
                else:
                    entry = Metadata(inclusion=Metadata.InclusionType.AVAILABLE)

                mapping[("properties", field_name)] = entry

        mapping[()] = root

        return mapping

    def resolve_selection(self) -> SelectionMask:
        """Resolve selection for metadata breadcrumbs and store them in a mapping."""
        return SelectionMask(
            (breadcrumb, self._breadcrumb_is_selected(breadcrumb))
            for breadcrumb in self
        )

    def _breadcrumb_is_selected(self, breadcrumb: Breadcrumb) -> bool:
        """Determine if a property breadcrumb is selected based on existing metadata.

        An empty breadcrumb (empty tuple) indicates the stream itself. Otherwise, the
        breadcrumb is the path to a property within the stream.
        """
        if not self:
            # Default to true if no metadata to say otherwise
            return True

        md_entry = self.get(breadcrumb, Metadata())
        parent_value = None

        if len(breadcrumb) > 0:
            parent_breadcrumb = breadcrumb[:-2]
            parent_value = self._breadcrumb_is_selected(parent_breadcrumb)

        if parent_value is False:
            return parent_value

        if md_entry.inclusion == Metadata.InclusionType.UNSUPPORTED:
            if md_entry.selected is True:
                logger.debug(
                    "Property '%s' was selected but is not supported. "
                    "Ignoring selected==True input.",
                    ":".join(breadcrumb),
                )
            return False

        if md_entry.inclusion == Metadata.InclusionType.AUTOMATIC:
            if md_entry.selected is False:
                logger.debug(
                    "Property '%s' was deselected while also set "
                    "for automatic inclusion. Ignoring selected==False input.",
                    ":".join(breadcrumb),
                )
            return True

        if md_entry.selected is not None:
            return md_entry.selected

        if md_entry.selected_by_default is not None:
            return md_entry.selected_by_default

        logger.debug(
            "Selection metadata omitted for '%s'. "
            "Using parent value of selected=%s.",
            breadcrumb,
            parent_value,
        )
        return parent_value or False


@dataclass
class CatalogEntry(BaseCatalogEntry):
    """Singer catalog entry."""

    tap_stream_id: str
    metadata: MetadataMapping
    schema: Schema
    stream: Optional[str] = None
    key_properties: Optional[List[str]] = None
    replication_key: Optional[str] = None
    is_view: Optional[bool] = None
    database: Optional[str] = None
    table: Optional[str] = None
    row_count: Optional[int] = None
    stream_alias: Optional[str] = None
    replication_method: Optional[str] = None

    @classmethod
    def from_dict(cls, stream: Dict[str, Any]):
        """Create a catalog entry from a dictionary."""
        return cls(
            tap_stream_id=stream["tap_stream_id"],
            stream=stream.get("stream"),
            replication_key=stream.get("replication_key"),
            key_properties=stream.get("key_properties"),
            database=stream.get("database_name"),
            table=stream.get("table_name"),
            schema=Schema.from_dict(stream.get("schema", {})),
            is_view=stream.get("is_view"),
            stream_alias=stream.get("stream_alias"),
            metadata=MetadataMapping.from_iterable(stream.get("metadata", [])),
            replication_method=stream.get("replication_method"),
        )

    def to_dict(self):
        """Convert entry to a dictionary."""
        d = super().to_dict()
        d["metadata"] = self.metadata.to_list()
        return d


class Catalog(Dict[str, CatalogEntry], BaseCatalog):
    """Singer catalog mapping of stream entries."""

    @classmethod
    def from_dict(cls, data: Dict[str, List[Dict[str, Any]]]) -> "Catalog":
        """Create a catalog from a dictionary."""
        instance = cls()
        for stream in data.get("streams", []):
            entry = CatalogEntry.from_dict(stream)
            instance[entry.tap_stream_id] = entry
        return instance

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary representation of the catalog.

        Returns:
            A dictionary with the defined catalog streams.
        """
        return cast(Dict[str, Any], super().to_dict())

    @property
    def streams(self) -> List[CatalogEntry]:
        """Get catalog entries."""
        return list(self.values())

    def add_stream(self, entry: CatalogEntry) -> None:
        """Add a stream entry to the catalog."""
        self[entry.tap_stream_id] = entry

    def get_stream(self, stream_id: str) -> Optional[CatalogEntry]:
        """Retrieve a stream entry from the catalog."""
        return self.get(stream_id)
