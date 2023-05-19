from __future__ import annotations

import enum
import logging
import typing as t
from dataclasses import dataclass, fields

from singer_sdk._singerlib.schema import Schema

if t.TYPE_CHECKING:
    from typing_extensions import TypeAlias


Breadcrumb = t.Tuple[str, ...]

logger = logging.getLogger(__name__)


class SelectionMask(t.Dict[Breadcrumb, bool]):
    """Boolean mask for property selection in schemas and records."""

    def __missing__(self, breadcrumb: Breadcrumb) -> bool:
        """Handle missing breadcrumbs.

        - Properties default to parent value if available.
        - Root (stream) defaults to True.

        Args:
            breadcrumb: Breadcrumb to check.

        Returns:
            True if the breadcrumb is selected, False otherwise.
        """
        if len(breadcrumb) >= 2:  # noqa: PLR2004
            parent = breadcrumb[:-2]
            return self[parent]

        return True


@dataclass
class Metadata:
    """Base stream or property metadata."""

    class InclusionType(str, enum.Enum):
        """Catalog inclusion types."""

        AVAILABLE = "available"
        AUTOMATIC = "automatic"
        UNSUPPORTED = "unsupported"

    inclusion: InclusionType | None = None
    selected: bool | None = None
    selected_by_default: bool | None = None

    @classmethod
    def from_dict(cls: type[Metadata], value: dict[str, t.Any]) -> Metadata:
        """Parse metadata dictionary.

        Args:
            value: Metadata dictionary.

        Returns:
            Metadata object.
        """
        return cls(
            **{
                object_field.name: value.get(object_field.name.replace("_", "-"))
                for object_field in fields(cls)
            },
        )

    def to_dict(self) -> dict[str, t.Any]:
        """Convert metadata to a JSON-encodeable dictionary.

        Returns:
            Metadata object.
        """
        result = {}

        for object_field in fields(self):
            value = getattr(self, object_field.name)
            if value is not None:
                result[object_field.name.replace("_", "-")] = value

        return result


@dataclass
class StreamMetadata(Metadata):
    """Stream metadata."""

    table_key_properties: list[str] | None = None
    forced_replication_method: str | None = None
    valid_replication_keys: list[str] | None = None
    schema_name: str | None = None


AnyMetadata: TypeAlias = t.Union[Metadata, StreamMetadata]


class MetadataMapping(t.Dict[Breadcrumb, AnyMetadata]):
    """Stream metadata mapping."""

    @classmethod
    def from_iterable(
        cls: type[MetadataMapping],
        iterable: t.Iterable[dict[str, t.Any]],
    ) -> MetadataMapping:
        """Create a metadata mapping from an iterable of metadata dictionaries.

        Args:
            iterable: t.Iterable of metadata dictionaries.

        Returns:
            Metadata mapping.
        """
        mapping = cls()
        for d in iterable:
            breadcrumb = tuple(d["breadcrumb"])
            metadata = d["metadata"]
            if breadcrumb:
                mapping[breadcrumb] = Metadata.from_dict(metadata)
            else:
                mapping[breadcrumb] = StreamMetadata.from_dict(metadata)

        return mapping

    def to_list(self) -> list[dict[str, t.Any]]:
        """Convert mapping to a JSON-encodable list.

        Returns:
            List of metadata dictionaries.
        """
        return [
            {"breadcrumb": list(k), "metadata": v.to_dict()} for k, v in self.items()
        ]

    def __missing__(self, breadcrumb: Breadcrumb) -> AnyMetadata:
        """Handle missing metadata entries.

        Args:
            breadcrumb: Breadcrumb to check.

        Returns:
            Metadata object.
        """
        self[breadcrumb] = Metadata() if breadcrumb else StreamMetadata()
        return self[breadcrumb]

    @property
    def root(self) -> StreamMetadata:
        """Get stream (root) metadata from this mapping.

        Returns:
            Stream metadata.
        """
        return self[()]  # type: ignore[return-value]

    @classmethod
    def get_standard_metadata(
        cls: type[MetadataMapping],
        *,
        schema: dict[str, t.Any] | None = None,
        schema_name: str | None = None,
        key_properties: list[str] | None = None,
        valid_replication_keys: list[str] | None = None,
        replication_method: str | None = None,
        selected_by_default: bool | None = None,
    ) -> MetadataMapping:
        """Get default metadata for a stream.

        Args:
            schema: Stream schema.
            schema_name: Stream schema name.
            key_properties: Stream key properties.
            valid_replication_keys: Stream valid replication keys.
            replication_method: Stream replication method.
            selected_by_default: Whether the stream is selected by default.

        Returns:
            Metadata mapping.
        """
        mapping = cls()
        root = StreamMetadata(
            table_key_properties=key_properties,
            forced_replication_method=replication_method,
            valid_replication_keys=valid_replication_keys,
            selected_by_default=selected_by_default,
        )

        if schema:
            root.inclusion = Metadata.InclusionType.AVAILABLE

            if schema_name:
                root.schema_name = schema_name

            for field_name in schema.get("properties", {}):
                if (
                    key_properties
                    and field_name in key_properties
                    or (valid_replication_keys and field_name in valid_replication_keys)
                ):
                    entry = Metadata(inclusion=Metadata.InclusionType.AUTOMATIC)
                else:
                    entry = Metadata(inclusion=Metadata.InclusionType.AVAILABLE)

                mapping[("properties", field_name)] = entry

        mapping[()] = root

        return mapping

    def resolve_selection(self) -> SelectionMask:
        """Resolve selection for metadata breadcrumbs and store them in a mapping.

        Returns:
            Selection mask.
        """
        return SelectionMask(
            (breadcrumb, self._breadcrumb_is_selected(breadcrumb))
            for breadcrumb in self
        )

    def _breadcrumb_is_selected(self, breadcrumb: Breadcrumb) -> bool:  # noqa: PLR0911
        """Determine if a property breadcrumb is selected based on existing metadata.

        An empty breadcrumb (empty tuple) indicates the stream itself. Otherwise, the
        breadcrumb is the path to a property within the stream.

        Args:
            breadcrumb: Breadcrumb to check.

        Returns:
            True if the breadcrumb is selected, False otherwise.
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
class CatalogEntry:
    """Singer catalog entry."""

    tap_stream_id: str
    metadata: MetadataMapping
    schema: Schema
    stream: str | None = None
    key_properties: list[str] | None = None
    replication_key: str | None = None
    is_view: bool | None = None
    database: str | None = None
    table: str | None = None
    row_count: int | None = None
    stream_alias: str | None = None
    replication_method: str | None = None

    @classmethod
    def from_dict(cls: type[CatalogEntry], stream: dict[str, t.Any]) -> CatalogEntry:
        """Create a catalog entry from a dictionary.

        Args:
            stream: A dictionary with the defined catalog stream.

        Returns:
            A catalog entry.
        """
        return cls(
            tap_stream_id=stream["tap_stream_id"],
            stream=stream.get("stream"),
            replication_key=stream.get("replication_key"),
            key_properties=stream.get("key_properties"),
            database=stream.get("database_name"),
            table=stream.get("table_name"),
            schema=Schema.from_dict(stream.get("schema", {})),
            is_view=stream.get("is_view"),
            row_count=stream.get("row_count"),
            stream_alias=stream.get("stream_alias"),
            metadata=MetadataMapping.from_iterable(stream.get("metadata", [])),
            replication_method=stream.get("replication_method"),
        )

    def to_dict(self) -> dict[str, t.Any]:  # noqa: C901
        """Convert entry to a dictionary.

        Returns:
            A dictionary representation of the catalog entry.
        """
        result: dict[str, t.Any] = {}
        if self.tap_stream_id:
            result["tap_stream_id"] = self.tap_stream_id
        if self.database:
            result["database_name"] = self.database
        if self.table:
            result["table_name"] = self.table
        if self.replication_key is not None:
            result["replication_key"] = self.replication_key
        if self.replication_method is not None:
            result["replication_method"] = self.replication_method
        if self.key_properties is not None:
            result["key_properties"] = self.key_properties
        if self.schema is not None:
            schema = self.schema.to_dict()  # pylint: disable=no-member
            result["schema"] = schema
        if self.is_view is not None:
            result["is_view"] = self.is_view
        if self.stream is not None:
            result["stream"] = self.stream
        if self.row_count is not None:
            result["row_count"] = self.row_count
        if self.stream_alias is not None:
            result["stream_alias"] = self.stream_alias
        if self.metadata is not None:
            result["metadata"] = self.metadata.to_list()
        return result


class Catalog(t.Dict[str, CatalogEntry]):
    """Singer catalog mapping of stream entries."""

    @classmethod
    def from_dict(
        cls: type[Catalog],
        data: dict[str, list[dict[str, t.Any]]],
    ) -> Catalog:
        """Create a catalog from a dictionary.

        Args:
            data: A dictionary with the defined catalog streams.

        Returns:
            A catalog.
        """
        instance = cls()
        for stream in data.get("streams", []):
            entry = CatalogEntry.from_dict(stream)
            instance[entry.tap_stream_id] = entry
        return instance

    def to_dict(self) -> dict[str, t.Any]:
        """Return a dictionary representation of the catalog.

        Returns:
            A dictionary with the defined catalog streams.
        """
        return {"streams": [stream.to_dict() for stream in self.streams]}

    @property
    def streams(self) -> list[CatalogEntry]:
        """Get catalog entries.

        Returns:
            A list of catalog entries.
        """
        return list(self.values())

    def add_stream(self, entry: CatalogEntry) -> None:
        """Add a stream entry to the catalog.

        Args:
            entry: The stream entry to add.
        """
        self[entry.tap_stream_id] = entry

    def get_stream(self, stream_id: str) -> CatalogEntry | None:
        """Retrieve a stream entry from the catalog.

        Args:
            stream_id: The tap stream id of the stream to retrieve.

        Returns:
            The stream entry if found, otherwise None.
        """
        return self.get(stream_id)
