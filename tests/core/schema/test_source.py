"""Tests for schema sources functionality."""

from __future__ import annotations

import json
import json.decoder
import typing as t
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

import pytest
import requests

from singer_sdk.exceptions import DiscoveryError
from singer_sdk.helpers._compat import Traversable
from singer_sdk.schema.source import (
    OpenAPISchema,
    SchemaDirectory,
    SchemaNotFoundError,
    SchemaNotValidError,
    SchemaSource,
    StreamSchema,
)
from singer_sdk.streams.core import Stream

if t.TYPE_CHECKING:
    from collections.abc import Mapping


class MockStream(Stream):
    """Mock stream for testing."""

    name = "test_stream"

    def get_records(self, context: Mapping[str, t.Any] | None):  # noqa: ARG002
        """Mock method."""
        return []


class TestSchemaSource:
    """Test the abstract SchemaSource class."""

    def test_schema_source_is_abstract(self):
        """Test that SchemaSource cannot be instantiated directly."""
        with pytest.raises(TypeError):
            SchemaSource()


class TestSchemaDirectory:
    """Test the SchemaDirectory class."""

    def test_dir_source_init(self):
        """Test FileSchema initialization."""
        source = SchemaDirectory("/path/to/schemas")
        assert source.dir_path == Path("/path/to/schemas")

    def test_dir_source_fetch_schema(self, tmp_path: Path):
        """Test getting schema from FileSchema."""
        schema_dict = {"type": "object", "properties": {"id": {"type": "string"}}}

        schema_file = tmp_path / "users.json"
        schema_file.write_text(json.dumps(schema_dict))

        source = SchemaDirectory(tmp_path)
        result = source.get_schema("users")
        assert result == schema_dict

        # Schema is cached
        schema_file.unlink()
        result = source.get_schema("users")
        assert result == schema_dict

    def test_dir_source_with_module(self):
        """Test FileSchema with a module."""
        schema_dict = {"type": "object", "properties": {"id": {"type": "string"}}}

        mock_module = Mock(spec=ModuleType)
        mock_traversable = Mock(spec=Traversable)
        mock_file_path = Mock(spec=Traversable)
        mock_traversable.joinpath.return_value = mock_file_path
        mock_file_path.read_text.return_value = json.dumps(schema_dict)

        with patch("importlib.resources.files", return_value=mock_traversable):
            source = SchemaDirectory(mock_module)
            result = source.get_schema("users")
            assert result == schema_dict

    def test_dir_source_with_traversable(self):
        """Test FileSchema with a Traversable object."""
        schema_dict = {"type": "object", "properties": {"id": {"type": "string"}}}

        # Create a mock Traversable object
        mock_traversable = Mock(spec=Traversable)
        mock_file_path = Mock(spec=Traversable)
        mock_traversable.joinpath.return_value = mock_file_path
        mock_file_path.read_text.return_value = json.dumps(schema_dict)

        source = SchemaDirectory(mock_traversable)
        result = source.get_schema("users")
        assert result == schema_dict
        mock_traversable.joinpath.assert_called_once_with("users.json")
        mock_file_path.read_text.assert_called_once()

    def test_dir_source_invalid_json(self, tmp_path: Path):
        """Test FileSchema with invalid JSON content."""
        schema_file = tmp_path / "invalid.json"

        # JSON array, not object
        schema_file.write_text('["this", "is", "not", "an", "object"]')

        source = SchemaDirectory(tmp_path)
        with pytest.raises(SchemaNotValidError, match="Schema must be a JSON object"):
            source.get_schema("invalid")

    def test_dir_source_file_not_found(self, tmp_path: Path):
        """Test FileSchema with a file that does not exist."""
        source = SchemaDirectory(tmp_path)
        with pytest.raises(
            SchemaNotFoundError,
            match="Schema file not found for 'users'",
        ):
            source.get_schema("users")

    def test_dir_source_invalid_json_file(self, tmp_path: Path):
        """Test FileSchema with invalid JSON content."""
        schema_file = tmp_path / "invalid.json"

        # JSON array, not object
        schema_file.write_text("this is not a valid json object")

        source = SchemaDirectory(tmp_path)
        with pytest.raises(
            SchemaNotValidError,
            match="Invalid JSON for 'invalid' schema",
        ):
            source.get_schema("invalid")


class TestOpenAPISchema:
    """Test the OpenAPISchema class."""

    @pytest.fixture(scope="session")
    def resolved_user_schema(self) -> dict[str, t.Any]:
        """User schema for testing."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
            },
            "required": ["id", "name"],
        }

    @pytest.fixture(scope="session")
    def sample_openapi_spec(self) -> dict[str, t.Any]:
        """Sample OpenAPI spec for testing."""
        return {
            "openapi": "3.0.0",
            "info": {"title": "Test API", "version": "1.0.0"},
            "components": {
                "schemas": {
                    "Email": {
                        "type": "string",
                        "format": "email",
                    },
                    "User": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "email": {"$ref": "#/components/schemas/Email"},
                        },
                        "required": ["id", "name"],
                    },
                    "Project": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "title": {"type": "string"},
                            "description": {"type": "string"},
                            "owner": {"$ref": "#/components/schemas/User"},
                        },
                        "required": ["id", "title"],
                    },
                }
            },
        }

    def test_openapi_schema_init_with_url(self):
        """Test OpenAPISchema initialization with URL."""
        source = OpenAPISchema("https://api.example.com/openapi.json")
        assert source.source == "https://api.example.com/openapi.json"

    def test_openapi_schema_init_with_path(self):
        """Test OpenAPISchema initialization with file path."""
        source = OpenAPISchema("/path/to/openapi.json")
        assert source.source == "/path/to/openapi.json"

    @patch("requests.get")
    def test_openapi_load_from_url(
        self,
        mock_get,
        sample_openapi_spec: dict[str, t.Any],
    ):
        """Test loading OpenAPI spec from URL."""
        mock_response = Mock()
        mock_response.json.return_value = sample_openapi_spec
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = OpenAPISchema("https://api.example.com/openapi.json")
        spec = source.spec

        assert spec == sample_openapi_spec
        mock_get.assert_called_once_with(
            "https://api.example.com/openapi.json",
            timeout=30,
        )
        mock_response.raise_for_status.assert_called_once()

    @patch("requests.get")
    def test_openapi_load_from_url_error(self, mock_get):
        """Test error handling when loading from URL."""
        mock_get.side_effect = requests.RequestException("Network error")

        source = OpenAPISchema("https://api.example.com/openapi.json")

        with pytest.raises(
            DiscoveryError,
            match="Failed to load OpenAPI specification",
        ) as exc:
            _ = source.spec

        assert isinstance(exc.value.__cause__, requests.RequestException)

    def test_openapi_load_from_file(
        self,
        tmp_path: Path,
        sample_openapi_spec: dict[str, t.Any],
    ):
        """Test loading OpenAPI spec from file."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text(json.dumps(sample_openapi_spec))

        source = OpenAPISchema(openapi_file)
        spec = source.spec
        assert spec == sample_openapi_spec

    def test_openapi_load_from_file_error(self, tmp_path: Path):
        """Test error handling when loading from file."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text("this is not a valid json object")

        source = OpenAPISchema(openapi_file)
        with pytest.raises(
            DiscoveryError,
            match="Failed to load OpenAPI specification",
        ) as exc:
            _ = source.spec

        assert isinstance(exc.value.__cause__, json.decoder.JSONDecodeError)

    def test_openapi_component(
        self,
        tmp_path: Path,
        resolved_user_schema: dict[str, t.Any],
        sample_openapi_spec: dict[str, t.Any],
    ):
        """Test getting full spec when no component specified."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text(json.dumps(sample_openapi_spec))

        source = OpenAPISchema(openapi_file)
        result = source.get_schema("User")
        assert result == resolved_user_schema

    def test_openapi_invalid_component(
        self,
        tmp_path: Path,
        sample_openapi_spec: dict[str, t.Any],
    ):
        """Test error when requesting invalid component."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text(json.dumps(sample_openapi_spec))

        source = OpenAPISchema(openapi_file)
        with pytest.raises(
            SchemaNotFoundError,
            match="Failed to resolve schema references for 'InvalidComponent'",
        ):
            source.get_schema("InvalidComponent")

    def test_openapi_schema_spec_caching(
        self,
        tmp_path: Path,
        sample_openapi_spec: dict[str, t.Any],
    ):
        """Test that spec property caches the loaded specification."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text(json.dumps(sample_openapi_spec))

        source = OpenAPISchema(openapi_file)
        # First access should load and cache
        spec1 = source.spec
        # Second access should return cached version
        spec2 = source.spec
        assert spec1 is spec2  # Same object reference
        assert spec1 == sample_openapi_spec

    def test_openapi_schema_with_traversable(
        self,
        sample_openapi_spec: dict[str, t.Any],
        resolved_user_schema: dict[str, t.Any],
    ):
        """Test OpenAPISchema with a Traversable object."""
        # Create a mock Traversable object
        mock_traversable = Mock(spec=Traversable)
        mock_traversable.read_text.return_value = json.dumps(sample_openapi_spec)

        source = OpenAPISchema(mock_traversable)
        result = source.get_schema("User")
        assert result == resolved_user_schema
        mock_traversable.read_text.assert_called_once()


class TestStreamSchemaDescriptor:
    """Test the StreamSchema descriptor."""

    @pytest.fixture
    def foo_schema(self) -> dict[str, t.Any]:
        """Schema dictionary."""
        return {"type": "object", "properties": {"id": {"type": "string"}}}

    @pytest.fixture
    def schema_source(
        self,
        foo_schema: dict[str, t.Any],
        tmp_path: Path,
    ) -> SchemaDirectory:
        """Schema source."""
        schema_file = tmp_path / "foo.json"
        schema_file.write_text(json.dumps(foo_schema))
        return SchemaDirectory(tmp_path)

    def test_get_stream_schema(
        self,
        foo_schema: dict[str, t.Any],
        schema_source: SchemaDirectory,
    ):
        """Test the StreamSchema descriptor."""
        stream = Mock(spec=Stream)
        stream.name = "foo"

        stream_schema = StreamSchema(schema_source)
        assert stream_schema.get_stream_schema(stream, type(stream)) == foo_schema

    def test_stream_schema_descriptor(
        self,
        foo_schema: dict[str, t.Any],
        schema_source: SchemaDirectory,
    ):
        """Test the StreamSchema descriptor."""

        class FooStream:
            name = "foo"
            schema: t.ClassVar[StreamSchema] = StreamSchema(schema_source)

        stream = FooStream()
        assert stream.schema == foo_schema

    def test_stream_schema_descriptor_with_explicit_key(
        self,
        foo_schema: dict[str, t.Any],
        schema_source: SchemaDirectory,
    ):
        """Test StreamSchema descriptor with an explicit custom key."""

        class BarStream:
            name = "bar"
            schema: t.ClassVar[StreamSchema] = StreamSchema(schema_source, key="foo")

        stream = BarStream()
        assert stream.schema == foo_schema

    def test_stream_schema_descriptor_key_not_found(
        self,
        schema_source: SchemaDirectory,
    ):
        """Test StreamSchema descriptor with an explicit custom key."""

        class BarStream:
            name = "bar"
            schema: t.ClassVar[StreamSchema] = StreamSchema(schema_source)

        stream = BarStream()
        with pytest.raises(
            SchemaNotFoundError,
            match="Schema file not found for 'bar'",
        ):
            _ = stream.schema
