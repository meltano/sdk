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

from singer_sdk.exceptions import DiscoveryError
from singer_sdk.singerlib.schema import resolve_schema_references

if sys.version_info >= (3, 11):
    from typing import assert_never  # noqa: ICN003
else:
    from typing_extensions import assert_never


if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if sys.version_info >= (3, 13):
    from typing import TypeVar  # noqa: ICN003
else:
    from typing_extensions import TypeVar

if t.TYPE_CHECKING:
    from singer_sdk.helpers._compat import Traversable
    from singer_sdk.streams.core import Stream


class SchemaNotFoundError(DiscoveryError):
    """Raised when a schema is not found."""


class SchemaNotValidError(DiscoveryError):
    """Raised when a schema is not valid."""


class UnsupportedOpenAPISpec(Exception):
    """Raised when the OpenAPI specification is not supported."""


_TKey = TypeVar("_TKey", bound=t.Hashable, default=str)


class SchemaSource(ABC, t.Generic[_TKey]):
    """Abstract base class for schema sources."""

    def __init__(self) -> None:
        """Initialize the schema source with caching."""
        self._schema_cache: dict[_TKey, dict[str, t.Any]] = {}

    @t.final
    def get_schema(self, key: _TKey, /) -> dict[str, t.Any]:
        """Convenience method to get a schema component.

        Args:
            key: The schema component name to retrieve.

        Returns:
            A JSON schema dictionary.

        Raises:
            SchemaNotFoundError: If the schema is not found or cannot be fetched.
            SchemaNotValidError: If the schema is not a JSON object.
        """
        if key not in self._schema_cache:
            try:
                schema = self.fetch_schema(key)
            except Exception as e:
                msg = f"Schema not found for '{key}'"
                raise SchemaNotFoundError(msg) from e
            if not isinstance(schema, dict):
                msg = "Schema must be a JSON object"  # type: ignore[unreachable]
                raise SchemaNotValidError(msg)
            self._schema_cache[key] = schema
        return self._schema_cache[key]

    @abstractmethod
    def fetch_schema(self, key: _TKey) -> dict[str, t.Any]:
        """Retrieve a JSON schema from this source.

        Args:
            key: The schema component name to retrieve.

        Returns:
            A JSON schema dictionary.

        Raises:
            ValueError: If the component is not found or invalid.
            requests.RequestException: If fetching schema from URL fails.
        """


class StreamSchema(t.Generic[_TKey]):
    """Stream schema descriptor.

    Assign a `StreamSchema` descriptor to a stream class to dynamically load the schema
    from a schema source.

    Example:
        class MyStream(Stream):
            schema = StreamSchema(SchemaDirectory("schemas"))
    """

    def __init__(
        self,
        schema_source: SchemaSource[_TKey],
        *,
        key: _TKey | None = None,
    ) -> None:
        """Initialize the stream schema.

        Args:
            schema_source: The schema source to use.
            key: The Optional key to use to get the schema from the schema source.
                by default the stream name will be used.
        """
        self.schema_source = schema_source
        self.key = key

    @t.final
    def __get__(self, obj: Stream, objtype: type[Stream]) -> dict[str, t.Any]:
        """Get the schema from the schema source.

        Args:
            obj: The object to get the schema from.
            objtype: The type of the object to get the schema from.

        Returns:
            A JSON schema dictionary.
        """
        return self.get_stream_schema(obj, objtype)

    def get_stream_schema(
        self,
        stream: Stream,
        stream_class: type[Stream],  # noqa: ARG002
    ) -> dict[str, t.Any]:
        """Get the schema from the stream instance or class.

        Args:
            stream: The stream instance to get the schema from.
            stream_class: The stream class to get the schema from.

        Returns:
            A JSON schema dictionary.
        """
        return self.schema_source.get_schema(self.key or stream.name)  # type: ignore[arg-type]


class OpenAPISchema(SchemaSource[_TKey]):
    """Schema source for OpenAPI specifications.

    Supports loading schemas from a local or remote OpenAPI 2.0 or 3.x specification.

    Example:
        openapi_schema = OpenAPISchema("https://api.example.com/openapi.json")
        stream_schema = openapi_schema("ProjectListItem")
    """

    def __init__(
        self,
        source: str | Path | Traversable,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the OpenAPI schema source.

        Args:
            source: URL, file path, or Traversable object pointing to an OpenAPI spec.
            *args: Additional arguments to pass to the superclass constructor.
            **kwargs: Additional keyword arguments to pass to the superclass
                constructor.
        """
        super().__init__(*args, **kwargs)
        self.source = source

    @cached_property
    def spec(self) -> dict[str, t.Any]:
        """Get the loaded OpenAPI specification.

        Returns:
            The full OpenAPI specification as a dictionary.
        """
        return self._load_spec()

    @property
    def spec_version(self) -> t.Literal["v2", "v3"]:
        """Get the OpenAPI specification version.

        Returns:
            The OpenAPI specification version.

        Raises:
            UnsupportedOpenAPISpec: If the OpenAPI specification format is unknown.
        """
        if "swagger" in self.spec:
            return "v2"
        if "openapi" in self.spec:
            return "v3"

        msg = "Unknown OpenAPI specification format"
        raise UnsupportedOpenAPISpec(msg)

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
        """
        if isinstance(self.source, str) and self.source.startswith(
            ("http://", "https://")
        ):
            return self._load_remote_spec(self.source)
        return self._load_local_spec(self.source)

    def get_unresolved_schema(self, key: _TKey) -> dict[str, t.Any]:
        """Build the base schema for the given key.

        By default, this method treats the key as a component name and builds a
        reference to it. It can be overridden to support other key types, such as
        endpoint paths.

        Args:
            key: The key to build the schema for.

        Returns:
            A JSON schema dictionary.
        """
        if self.spec_version == "v2":
            return {"$ref": f"#/definitions/{key}"}

        if self.spec_version == "v3":
            return {"$ref": f"#/components/schemas/{key}"}

        assert_never(self.spec_version)

    def resolve_schema(self, key: _TKey) -> dict[str, t.Any]:
        """Resolve the schema references.

        Args:
            key: The schema component name to resolve.

        Returns:
            A JSON schema dictionary.
        """
        if self.spec_version == "v2":
            components_key = "definitions"
            components = self.spec.get(components_key, {})
        elif self.spec_version == "v3":
            components_key = "components"
            components = self.spec.get(components_key, {})
        else:
            assert_never(self.spec_version)

        schema = {
            **self.get_unresolved_schema(key),
            components_key: components,
        }
        resolved_schema = resolve_schema_references(schema)
        resolved_schema.pop(components_key)
        return resolved_schema

    @override
    def fetch_schema(self, key: _TKey) -> dict[str, t.Any]:
        """Retrieve a schema from the OpenAPI specification.

        Args:
            key: The schema component name to retrieve. The format of the key
                depends on the implementation of `build_base_schema`.

        Returns:
            A JSON schema dictionary.
        """
        return self.resolve_schema(key)


class SchemaDirectory(SchemaSource):
    """Schema source for local file-based schemas."""

    def __init__(
        self,
        dir_path: str | Path | Traversable | ModuleType,
        *args: t.Any,
        extension: str = "json",
        **kwargs: t.Any,
    ) -> None:
        """Initialize the file schema source.

        Args:
            dir_path: Path to a directory containing JSON schema files.
            *args: Additional arguments to pass to the superclass constructor.
            **kwargs: Additional keyword arguments to pass to the superclass
                constructor.
            extension: The extension of the schema files.
        """
        super().__init__(*args, **kwargs)
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
        """
        file_path = self.dir_path.joinpath(f"{key}.{self.extension}")
        return json.loads(file_path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
