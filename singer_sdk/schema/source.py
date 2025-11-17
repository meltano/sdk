"""Schema sources for dynamic schema loading."""

from __future__ import annotations

import importlib.resources
import json
import sys
import typing as t
from abc import ABC, abstractmethod
from copy import deepcopy
from functools import cached_property
from pathlib import Path
from types import ModuleType
from urllib.parse import urlparse

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
    from collections.abc import Callable, Sequence

    from singer_sdk.helpers._compat import Traversable
    from singer_sdk.streams.core import Stream


Schema: t.TypeAlias = dict[str, t.Any]


class SchemaNotFoundError(DiscoveryError):
    """Raised when a schema is not found."""


class SchemaNotValidError(DiscoveryError):
    """Raised when a schema is not valid."""


class UnsupportedOpenAPISpec(Exception):
    """Raised when the OpenAPI specification is not supported."""


_TKey = TypeVar("_TKey", bound=t.Hashable, default=str)


class SchemaPreprocessor(t.Protocol):
    """Protocol for schema preprocessors that transform schemas before use.

    Implementations can normalize, validate, or modify schemas as needed.
    """

    def preprocess_schema(
        self,
        schema: Schema,
        *,
        key_properties: Sequence[str] = (),
    ) -> Schema:
        """Pre-process a schema.

        Args:
            schema: A JSON schema to preprocess.
            key_properties: The stream's key properties.

        Returns:
            The preprocessed schema.
        """


class SchemaSource(ABC, t.Generic[_TKey]):
    """Abstract base class for schema sources."""

    def __init__(self) -> None:
        """Initialize the schema source with caching."""
        self._schema_cache: dict[_TKey, Schema] = {}
        self._preprocessor: SchemaPreprocessor | None = None

    @t.final
    def preprocess_schema(
        self,
        schema: Schema,
        *,
        key_properties: Sequence[str] = (),
    ) -> Schema:
        """Pre-process the schema before providing it to the stream.

        Args:
            schema: A JSON schema.
            key_properties: The stream's key properties.

        Returns:
            The pre-processed schema.
        """
        if self._preprocessor is None:
            return schema
        return self._preprocessor.preprocess_schema(
            schema,
            key_properties=key_properties,
        )

    @t.final
    def get_schema(
        self,
        key: _TKey,
        /,
        *,
        key_properties: Sequence[str] = (),
    ) -> Schema:
        """Convenience method to get a schema component.

        Args:
            key: The schema component name to retrieve.
            key_properties: The stream's key properties.

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

        return self.preprocess_schema(
            self._schema_cache[key],
            key_properties=key_properties,
        )

    @abstractmethod
    def fetch_schema(self, key: _TKey) -> Schema:
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

    Example with OpenAPI:
        class MyStream(Stream):
            schema = StreamSchema(OpenAPISchema("openapi.json"))

    Example with custom OpenAPI preprocessor:
        class MyStream(Stream):
            schema = StreamSchema(
                OpenAPISchema(
                    "openapi.json",
                    preprocessor=CustomPreprocessor(),
                )
            )
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
    def __get__(self, obj: Stream, objtype: type[Stream]) -> Schema:
        """Get the schema from the schema source.

        Args:
            obj: The object to get the schema from.
            objtype: The type of the object to get the schema from.

        Returns:
            A JSON schema dictionary.
        """
        return self.get_stream_schema(obj, objtype)

    def get_stream_schema(self, stream: Stream, stream_class: type[Stream]) -> Schema:  # noqa: ARG002
        """Get the schema from the stream instance or class.

        Args:
            stream: The stream instance to get the schema from.
            stream_class: The stream class to get the schema from.

        Returns:
            A JSON schema dictionary.
        """
        return self.schema_source.get_schema(
            self.key or stream.name,  # type: ignore[arg-type]
            key_properties=stream.primary_keys,
        )


def _load_yaml(content: bytes) -> dict[str, t.Any]:
    import yaml  # noqa: PLC0415

    return yaml.safe_load(content)  # type: ignore[no-any-return]


class OpenAPISchemaNormalizer(SchemaPreprocessor):
    """A schema pre-processor that normalizes OpenAPI JSON schemas.

    Converts OpenAPI-specific schema features into standard JSON Schema constructs:
    - Converts `nullable: true` to type arrays with "null"
    - Unwraps single-element `oneOf` constructs
    - Merges `allOf` constructs into a single object schema
    - Removes `enum` keywords
    - Recursively processes nested object properties and array items
    """

    def handle_object(
        self,
        schema: Schema,
        *,
        key_properties: Sequence[str] = (),
    ) -> Schema:
        """Handle JSON object schemas.

        Args:
            schema: A JSON schema.
            key_properties: The stream's key properties.

        Returns:
            The processed object schema.
        """
        schema["nullable"] = len(key_properties) == 0
        for prop, prop_schema in schema.get("properties", {}).items():
            prop_schema["nullable"] = prop not in key_properties
            schema["properties"][prop] = self.normalize_schema(prop_schema)
        return schema

    def handle_array_items(self, schema: Schema) -> Schema:
        """Handle JSON array item schemas.

        Args:
            schema: A JSON schema.

        Returns:
            The processed array items schema.
        """
        return self.normalize_schema(schema)

    def handle_enum(self, schema: Schema) -> Schema:  # noqa: PLR6301
        """Handle enum values in a JSON schema.

        Args:
            schema: A JSON schema.

        Returns:
            The schema with `enum` handled.
        """
        schema.pop("enum", None)
        return schema

    def handle_all_of(self, subschemas: list[Schema]) -> Schema:  # noqa: PLR6301
        """Handle allOf constructs in a JSON schema.

        Args:
            subschemas: A list of JSON schemas.

        Returns:
            A merged JSON schema.
        """
        if not subschemas:
            return {}

        # If the allOf array has only one element, just flatten it.
        if len(subschemas) == 1:
            return subschemas[0]

        # TODO: merge subschemas:
        # - Taking the most restrictive constraints for each property
        # - Merging `required` arrays
        # - Handling type intersections carefully
        result: dict[str, t.Any] = {}
        for subschema in subschemas:
            for key, value in subschema.items():
                if key == "properties" and key in result:
                    # Deep merge properties
                    result["properties"] |= value
                else:
                    result[key] = value

        return result

    def normalize_schema(
        self,
        schema: Schema,
        *,
        key_properties: Sequence[str] = (),
    ) -> Schema:
        """Normalize an OpenAPI schema to standard JSON Schema.

        This method applies a series of transformations to convert OpenAPI-specific
        schema features (like 'nullable') into standard JSON Schema constructs.

        Args:
            schema: The schema to normalize.
            key_properties: The stream's key properties.

        Returns:
            A normalized schema dictionary.
        """
        result = deepcopy(schema)
        schema_type: str | list[str] = result.get("type", [])

        if "object" in schema_type:
            result = self.handle_object(result, key_properties=key_properties)

        elif "array" in schema_type and (items := result.get("items")):
            result["items"] = self.handle_array_items(items)

        if "allOf" in result:
            result = self.normalize_schema(self.handle_all_of(result["allOf"]))

        if "oneOf" in result and len(result["oneOf"]) == 1:
            (inner,) = result.pop("oneOf")
            result.update(self.normalize_schema(inner))
            schema_type = result.get("type", [])

        types = [schema_type] if isinstance(schema_type, str) else schema_type
        if result.pop("nullable", False) and "null" not in types:
            result["type"] = [*types, "null"]

        # Remove 'enum' keyword
        if "enum" in result:
            result = self.handle_enum(result)

        return result

    @override
    def preprocess_schema(
        self,
        schema: Schema,
        *,
        key_properties: Sequence[str] = (),
    ) -> Schema:
        return self.normalize_schema(schema, key_properties=key_properties)


class OpenAPISchema(SchemaSource[_TKey]):
    """Schema source for OpenAPI specifications.

    Supports loading schemas from a local or remote OpenAPI 2.0 or 3.x specification
    in JSON or YAML format.

    Example:
        openapi_schema = OpenAPISchema("https://api.example.com/openapi.json")
        stream_schema = openapi_schema("ProjectListItem")
    """

    def __init__(
        self,
        source: str | Path | Traversable,
        *args: t.Any,
        preprocessor: OpenAPISchemaNormalizer | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the OpenAPI schema source.

        Args:
            source: URL, file path, or Traversable object pointing to an OpenAPI spec
                in JSON or YAML format.
            preprocessor: Optional schema normalizer. If not provided, an
                OpenAPISchemaNormalizer instance will be created automatically to
                normalize OpenAPI schemas to standard JSON Schema.
            *args: Additional arguments to pass to the superclass constructor.
            **kwargs: Additional keyword arguments to pass to the superclass
                constructor.
        """
        super().__init__(*args, **kwargs)
        self.source = source
        self._content_loaders: dict[str, Callable[[bytes], dict[str, t.Any]]] = {
            ".json": json.loads,
            ".yaml": _load_yaml,
            ".yml": _load_yaml,
        }
        self._preprocessor = preprocessor or OpenAPISchemaNormalizer()

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

    def _load_spec(self) -> dict[str, t.Any]:
        """Load the OpenAPI specification from the source.

        Returns:
            The OpenAPI specification as a dictionary.

        Raises:
            UnsupportedOpenAPISpec: If the OpenAPI specification file type is not
                supported.
        """
        if isinstance(self.source, str) and self.source.startswith(
            ("http://", "https://")
        ):
            response = requests.get(self.source, timeout=30)
            response.raise_for_status()
            content = response.content

            ext: str | None = None
            if content_type := response.headers.get("Content-Type"):
                if "application/yaml" in content_type:
                    ext = ".yaml"
                elif "application/json" in content_type:
                    ext = ".json"

            ext = ext or f".{urlparse(self.source).path.split('.').pop()}"
        else:
            path = Path(self.source) if isinstance(self.source, str) else self.source
            with importlib.resources.as_file(path) as tmp_path:
                content = tmp_path.read_bytes()
                ext = tmp_path.suffix

        if loader := self._content_loaders.get(ext.lower()):
            return loader(content)

        msg = f"Unsupported OpenAPI file type: {ext}"
        raise UnsupportedOpenAPISpec(msg)

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

    def resolve_schema(self, key: _TKey) -> Schema:
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
    def fetch_schema(self, key: _TKey) -> Schema:
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
    def fetch_schema(self, key: str) -> Schema:
        """Retrieve schema from the file.

        Args:
            key: Ignored for file schemas.

        Returns:
            A JSON schema dictionary.
        """
        file_path = self.dir_path.joinpath(f"{key}.{self.extension}")
        return json.loads(file_path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
