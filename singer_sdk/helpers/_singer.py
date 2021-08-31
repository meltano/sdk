from dataclasses import dataclass, fields
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple

from singer.catalog import Catalog as BaseCatalog, CatalogEntry as BaseCatalogEntry
from singer.schema import Schema


@dataclass
class Metadata:
    """Stream or property metadata."""

    class InclusionType(str, Enum):
        """Catalog inclusion types."""

        AVAILABLE = "available"
        AUTOMATIC = "automatic"
        UNSUPPORTED = "unsupported"

    inclusion: Optional[InclusionType] = None
    selected: Optional[bool] = None
    selected_by_default: Optional[bool] = None
    table_key_properties: Optional[List[str]] = None
    forced_replication_method: Optional[str] = None
    valid_replication_keys: Optional[List[str]] = None
    schema_name: Optional[str] = None

    @classmethod
    def from_dict(cls, value: Dict[str, Any]):
        """Parse metadata dictionary."""
        return cls(
            **{
                field.name.replace("-", "_"): value.get(field.name)
                for field in fields(cls)
            }
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to a JSON-encodable dictionary."""
        result = {}

        for field in fields(self):
            value = getattr(self, field.name)
            if value is not None:
                result[field.name.replace("_", "-")] = value

        return result


class MetadataMapping(Dict[Tuple[str, ...], Metadata]):
    """Stream metadata mapping."""

    @classmethod
    def from_iterable(cls, iterable: Iterable[Dict[str, Any]]):
        """Create a metadata mapping from an iterable of metadata dictionaries."""
        if not iterable:
            return cls()
        return cls(
            (tuple(d["breadcrumb"]), Metadata.from_dict(d["metadata"]))
            for d in iterable
        )

    def to_list(self) -> List[Dict[str, Any]]:
        """Convert mapping to a JSON-encodable list."""
        return [
            {"breadcrumb": list(k), "metadata": v.to_dict()} for k, v in self.items()
        ]

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
        root = Metadata(
            table_key_properties=key_properties,
            forced_replication_method=replication_method,
            valid_replication_keys=valid_replication_keys,
        )

        if schema:
            root.inclusion = Metadata.InclusionType.AVAILABLE

            if schema_name:
                root.schema_name = schema_name

            for field_name in schema["properties"].keys():
                if key_properties and field_name in key_properties:
                    entry = Metadata(inclusion=Metadata.InclusionType.AUTOMATIC)
                else:
                    entry = Metadata(inclusion=Metadata.InclusionType.AVAILABLE)

                mapping[("properties", field_name)] = entry

        mapping[()] = root

        return mapping


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
    def from_dict(cls, data: Dict[str, List[Dict[str, Any]]]):
        """Create a catalog from a dictionary."""
        instance = cls()
        for stream in data.get("streams", []):
            entry = CatalogEntry.from_dict(stream)
            instance[entry.tap_stream_id] = entry
        return instance

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
