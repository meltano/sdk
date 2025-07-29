"""Schema sources for dynamic schema loading."""

from __future__ import annotations

import importlib.resources
import json
import sys
import typing as t
from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from types import ModuleType

import requests
from referencing.exceptions import PointerToNowhere

from singer_sdk.exceptions import DiscoveryError
from singer_sdk.singerlib.schema import resolve_schema_references

if sys.version_info < (3, 12):
    from typing_extensions import override
else:
    from typing import override  # noqa: ICN003

if t.TYPE_CHECKING:
    from singer_sdk.helpers._compat import Traversable
    from singer_sdk.streams.core import Stream


class SchemaNotFoundError(DiscoveryError):
    """Raised when a schema is not found."""


class SchemaNotValidError(DiscoveryError):
    """Raised when a schema is not valid."""


class SchemaSource(ABC):
    """Abstract base class for schema sources."""

    def __init__(self) -> None:
        """Initialize the schema source with caching."""
        self._schema_cache: dict[str | None, dict[str, t.Any]] = {}

    def get_schema(self, key: str, /) -> dict[str, t.Any]:
        """Convenience method to get a schema component.

        Args:
            key: The schema component name to retrieve.

        Returns:
            A JSON schema dictionary.

        Raises:
            SchemaNotValidError: If the schema is not a JSON object.
        """
        if key not in self._schema_cache:
            schema = self.fetch_schema(key)
            if not isinstance(schema, dict):
                msg = "Schema must be a JSON object"  # type: ignore[unreachable]
                raise SchemaNotValidError(msg)
            self._schema_cache[key] = schema
        return self._schema_cache[key]

    @abstractmethod
    def fetch_schema(self, key: str) -> dict[str, t.Any]:
        """Retrieve a JSON schema from this source.

        Args:
            key: The schema component name to retrieve.

        Returns:
            A JSON schema dictionary.

        Raises:
            ValueError: If the component is not found or invalid.
            requests.RequestException: If fetching schema from URL fails.
        """


class StreamSchema:
    """Stream schema.

    A stream schema is a schema that is used to validate the records of a stream.
    """

    def __init__(self, schema_source: SchemaSource, *, key: str | None = None) -> None:
        """Initialize the stream schema.

        Args:
            schema_source: The schema source to use.
            key: The Optional key to use to get the schema from the schema source.
                by default the stream name will be used.
        """
        self.schema_source = schema_source
        self.key = key

    def __get__(self, obj: Stream, objtype: type[Stream]) -> dict[str, t.Any]:
        """Get the schema from the schema source.

        Args:
            obj: The object to get the schema from.
            objtype: The type of the object to get the schema from.

        Returns:
            A JSON schema dictionary.
        """
        return self.schema_source.get_schema(self.key or obj.name)


class OpenAPISchema(SchemaSource):
    """Schema source for OpenAPI specifications.

    Supports loading schemas from a local or remote OpenAPI 3.1 specification.

    Example:
        openapi_schema = OpenAPISchema("https://api.example.com/openapi.json")
        stream_schema = openapi_schema("ProjectListItem")
    """

    def __init__(self, source: str | Path | Traversable) -> None:
        """Initialize the OpenAPI schema source.

        Args:
            source: URL, file path, or Traversable object pointing to an OpenAPI spec.
        """
        super().__init__()
        self.source = source

    @cached_property
    def spec(self) -> dict[str, t.Any]:
        """Get the loaded OpenAPI specification.

        Returns:
            The full OpenAPI specification as a dictionary.
        """
        return self._load_spec()

    def _load_remote_spec(self, url: str) -> dict[str, t.Any]:  # noqa: PLR6301
        """Load the OpenAPI specification from a remote source.

        Returns:
            The OpenAPI specification as a dictionary.
        """
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def _load_local_spec(self, path: str | Path | Traversable) -> dict[str, t.Any]:
        """Load the OpenAPI specification from a local source.

        Returns:
            The OpenAPI specification as a dictionary.
        """
        path = Path(self.source) if isinstance(self.source, str) else self.source
        content = path.read_text(encoding="utf-8")
        return json.loads(content)  # type: ignore[no-any-return]

    def _load_spec(self) -> dict[str, t.Any]:
        """Load the OpenAPI specification from the source.

        Returns:
            The OpenAPI specification as a dictionary.

        Raises:
            DiscoveryError: If the specification cannot be loaded from a remote or local
                source.
            SchemaNotValidError: If the specification format is invalid.
        """
        try:
            if isinstance(self.source, str) and self.source.startswith(
                ("http://", "https://")
            ):
                # Load from URL
                spec = self._load_remote_spec(self.source)
            else:
                # Load from local file
                spec = self._load_local_spec(self.source)
        except Exception as e:
            msg = f"Failed to load OpenAPI specification from {self.source}"
            raise DiscoveryError(msg) from e

        if not isinstance(spec, dict):
            msg = "OpenAPI specification must be a JSON object"
            raise SchemaNotValidError(msg)

        return spec

    @override
    def fetch_schema(self, key: str) -> dict[str, t.Any]:
        """Retrieve a schema from the OpenAPI specification.

        Args:
            key: The schema component name to retrieve from #/components/schemas/.

        Returns:
            A JSON schema dictionary.

        Raises:
            SchemaNotFoundError: If the schema component is not found.
        """
        schema = {
            "$ref": f"#/components/schemas/{key}",
            "components": self.spec.get("components", {}),
        }
        try:
            resolved_schema = resolve_schema_references(schema)
            return resolved_schema.get("components", {}).get("schemas", {}).get(key, {})  # type: ignore[no-any-return]
        except PointerToNowhere as e:
            msg = f"Schema component '{key}' not found"
            raise SchemaNotFoundError(msg) from e


class SchemaDirectory(SchemaSource):
    """Schema source for local file-based schemas."""

    def __init__(
        self,
        dir_path: str | Path | Traversable | ModuleType,
        *,
        extension: str = "json",
    ) -> None:
        """Initialize the file schema source.

        Args:
            dir_path: Path to a directory containing JSON schema files.
            extension: The extension of the schema files.
        """
        super().__init__()
        if isinstance(dir_path, ModuleType):
            self.dir_path = importlib.resources.files(dir_path)
        elif isinstance(dir_path, str):
            self.dir_path = Path(dir_path)
        else:
            self.dir_path = dir_path

        self.extension = extension

    @override
    def fetch_schema(self, key: str) -> dict[str, t.Any]:
        """Retrieve schema from the file.

        Args:
            key: Ignored for file schemas.

        Returns:
            A JSON schema dictionary.

        Raises:
            SchemaNotFoundError: If the schema file is not found.
            SchemaNotValidError: If the schema file is invalid.
        """
        file_path = self.dir_path.joinpath(f"{key}.{self.extension}")
        try:
            return json.loads(file_path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
        except FileNotFoundError as e:
            msg = f"Schema file not found for '{key}'"
            raise SchemaNotFoundError(msg) from e
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON for '{key}' schema"
            raise SchemaNotValidError(msg) from e
