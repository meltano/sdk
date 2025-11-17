"""Tests for schema sources functionality."""

from __future__ import annotations

import json
import json.decoder
import platform
import sys
import typing as t
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

import pytest
import requests
import yaml
from referencing.exceptions import PointerToNowhere
from toolz.dicttoolz import get_in

from singer_sdk.helpers._compat import Traversable
from singer_sdk.schema.source import (
    OpenAPISchema,
    SchemaDirectory,
    SchemaNotFoundError,
    SchemaNotValidError,
    SchemaSource,
    StreamSchema,
    UnsupportedOpenAPISpec,
)
from singer_sdk.streams.core import Stream

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from collections.abc import Mapping


@pytest.fixture(scope="session")
def resolved_user_schema() -> dict[str, t.Any]:
    """User schema for testing."""
    return {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "name": {"type": ["string", "null"]},
            "email": {"type": ["string", "null"], "format": "email"},
        },
        "required": ["id", "name"],
    }


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(
            {
                "openapi": "3.0.0",
                "info": {"title": "Test API", "version": "1.0.0"},
                "paths": {
                    "/users": {
                        "get": {
                            "summary": "Get a list of users",
                            "responses": {
                                "200": {
                                    "description": "A list of users",
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "type": "array",
                                                "items": {
                                                    "$ref": "#/components/schemas/User",
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
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
            },
            id="openapi-3.0",
        ),
        pytest.param(
            {
                "swagger": "2.0",
                "info": {"title": "Test API", "version": "1.0.0"},
                "definitions": {
                    "Email": {
                        "type": "string",
                        "format": "email",
                    },
                    "User": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "email": {"$ref": "#/definitions/Email"},
                        },
                        "required": ["id", "name"],
                    },
                },
            },
            id="openapi-2.0",
        ),
    ],
)
def openapi_spec(request: pytest.FixtureRequest) -> dict[str, t.Any]:
    """Parameterized OpenAPI spec fixture for testing both 2.0 and 3.0 versions."""
    return request.param  # type: ignore[no-any-return]


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
            match="Schema not found for 'users'",
        ) as exc:
            source.get_schema("users")

        assert isinstance(exc.value.__cause__, FileNotFoundError)

    def test_dir_source_invalid_json_file(self, tmp_path: Path):
        """Test FileSchema with invalid JSON content."""
        schema_file = tmp_path / "invalid.json"

        # JSON array, not object
        schema_file.write_text("this is not a valid json object")

        source = SchemaDirectory(tmp_path)
        with pytest.raises(
            SchemaNotFoundError,
            match="Schema not found for 'invalid'",
        ) as exc:
            source.get_schema("invalid")
        assert isinstance(exc.value.__cause__, json.decoder.JSONDecodeError)


class TestOpenAPISchema:
    """Test the OpenAPISchema class."""

    def test_openapi_schema_init_with_url(self):
        """Test OpenAPISchema initialization with URL."""
        source = OpenAPISchema("https://api.example.com/openapi.json")
        assert source.source == "https://api.example.com/openapi.json"

    def test_openapi_schema_init_with_path(self):
        """Test OpenAPISchema initialization with file path."""
        source = OpenAPISchema("/path/to/openapi.json")
        assert source.source == "/path/to/openapi.json"

    @pytest.mark.parametrize(
        "url,headers",
        [
            pytest.param(
                "https://api.example.com/openapi",
                {"content-type": "application/json"},
                id="application/json",
            ),
            pytest.param(
                "https://api.example.com/openapi",
                {"content-type": "application/json; charset=utf-8"},
                id="application/json; charset=utf-8",
            ),
            pytest.param(
                "https://api.example.com/openapi",
                {"content-type": "application/yaml"},
                id="application/yaml",
            ),
            pytest.param(
                "https://api.example.com/openapi.yaml",
                {},
                id="no content-type",
            ),
            pytest.param(
                "https://api.example.com/openapi.yml",
                {"content-type": "text/plain"},
                id="text/plain",
            ),
        ],
    )
    @patch("requests.get")
    def test_openapi_load_from_url(
        self,
        mock_get,
        openapi_spec: dict[str, t.Any],
        url: str,
        headers: dict[str, str],
    ):
        """Test loading OpenAPI spec from URL."""
        response = requests.Response()
        response._content = (
            json.dumps(openapi_spec).encode("utf-8")
            if "application/json" in headers.get("content-type", "")
            else yaml.dump(openapi_spec).encode("utf-8")
        )
        response.status_code = 200
        response.headers.update(headers)
        mock_get.return_value = response

        source = OpenAPISchema(url)
        spec = source.spec

        assert spec == openapi_spec
        mock_get.assert_called_once_with(url, timeout=30)

    @patch("requests.get")
    def test_openapi_load_from_url_error(self, mock_get):
        """Test error handling when loading from URL."""
        mock_get.side_effect = requests.RequestException("Network error")

        source = OpenAPISchema("https://api.example.com/openapi.json")

        with pytest.raises(SchemaNotFoundError) as exc:
            _ = source.get_schema("User")

        assert isinstance(exc.value.__cause__, requests.RequestException)
        assert exc.value.__cause__.args == ("Network error",)

    @pytest.mark.parametrize(
        "file_extension",
        [
            ".json",
            ".yaml",
            ".yml",
        ],
    )
    def test_openapi_load_from_file(
        self,
        tmp_path: Path,
        openapi_spec: dict[str, t.Any],
        file_extension: str,
    ):
        """Test loading OpenAPI spec from file."""
        openapi_file = tmp_path / f"openapi{file_extension}"
        openapi_file.write_text(
            json.dumps(openapi_spec)
            if file_extension == ".json"
            else yaml.dump(openapi_spec)
        )

        source = OpenAPISchema(openapi_file)
        spec = source.spec
        assert spec == openapi_spec

    def test_openapi_load_json_from_file_error(self, tmp_path: Path):
        """Test error handling when loading from file."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text("this is not a valid json object")

        source = OpenAPISchema(openapi_file)
        with pytest.raises(SchemaNotFoundError) as exc:
            _ = source.get_schema("User")

        assert isinstance(exc.value.__cause__, json.decoder.JSONDecodeError)

    def test_openapi_load_yaml_from_file_error(self, tmp_path: Path):
        """Test error handling when loading from file."""
        openapi_file = tmp_path / "openapi.yaml"
        openapi_file.write_text("{ NOT VALID YAML")

        source = OpenAPISchema(openapi_file)
        with pytest.raises(SchemaNotFoundError) as exc:
            _ = source.get_schema("User")

        assert isinstance(exc.value.__cause__, yaml.YAMLError)

    def test_openapi_load_not_a_file_error(self, tmp_path: Path):
        """Test error handling when loading from file."""
        openapi_dir = tmp_path / "openapi"
        openapi_dir.mkdir()

        source = OpenAPISchema(openapi_dir)
        err = IsADirectoryError if platform.system() != "Windows" else PermissionError
        with pytest.raises(err):
            _ = source.spec

    @pytest.mark.parametrize("file_format", ["json", "yaml"])
    def test_openapi_component(
        self,
        tmp_path: Path,
        resolved_user_schema: dict[str, t.Any],
        openapi_spec: dict[str, t.Any],
        file_format: str,
    ):
        """Test getting a component from OpenAPI spec (both 2.0 and 3.0)."""
        file_extension = ".json" if file_format == "json" else ".yaml"
        openapi_file = tmp_path / f"openapi{file_extension}"
        content = (
            json.dumps(openapi_spec)
            if file_format == "json"
            else yaml.dump(openapi_spec)
        )
        openapi_file.write_text(content)

        source = OpenAPISchema(openapi_file)
        result = source.get_schema("User", key_properties=("id",))
        assert result == resolved_user_schema

    def test_openapi_unknown_spec(
        self,
        tmp_path: Path,
    ):
        """Test getting a component from an unknown OpenAPI spec."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text(json.dumps({"info": "some info"}))

        source = OpenAPISchema(openapi_file)
        with pytest.raises(
            SchemaNotFoundError,
            match="Schema not found for 'User'",
        ) as exc:
            source.get_schema("User")

        assert isinstance(exc.value.__cause__, UnsupportedOpenAPISpec)

    def test_openapi_invalid_component(
        self,
        tmp_path: Path,
        openapi_spec: dict[str, t.Any],
    ):
        """Test error when requesting invalid component."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text(json.dumps(openapi_spec))

        source = OpenAPISchema(openapi_file)
        with pytest.raises(
            SchemaNotFoundError,
            match="Schema not found for 'InvalidComponent'",
        ) as exc:
            source.get_schema("InvalidComponent")
        assert isinstance(exc.value.__cause__, PointerToNowhere)

    def test_openapi_schema_spec_caching(
        self,
        tmp_path: Path,
        openapi_spec: dict[str, t.Any],
    ):
        """Test that spec property caches the loaded specification."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text(json.dumps(openapi_spec))

        source = OpenAPISchema(openapi_file)
        # First access should load and cache
        spec1 = source.spec
        # Second access should return cached version
        spec2 = source.spec
        assert spec1 is spec2  # Same object reference
        assert spec1 == openapi_spec

    def test_openapi_schema_with_traversable(
        self,
        resolved_user_schema: dict[str, t.Any],
        openapi_spec: dict[str, t.Any],
        tmp_path: Path,
    ):
        """Test OpenAPISchema with a Traversable object."""
        path = tmp_path / "openapi.json"
        path.write_text(json.dumps(openapi_spec))

        source = OpenAPISchema(path)
        result = source.get_schema("User", key_properties=("id",))
        assert result == resolved_user_schema

    def test_openapi_schema_with_unknown_file_type(
        self,
        tmp_path: Path,
    ):
        """Test OpenAPISchema with an unknown file type."""
        path = tmp_path / "openapi.toml"
        path.write_text('openapi = "3.0.0"\n')

        source = OpenAPISchema(path)
        with pytest.raises(
            UnsupportedOpenAPISpec,
            match=r"Unsupported OpenAPI file type: \.toml",
        ):
            _ = source.spec


class TestCustomOpenAPISchema:
    """Test a custom OpenAPISchema implementation."""

    def test_openapi_schema_from_path(self) -> None:
        """Test getting schema from an endpoint path."""
        # Use OpenAPI 3.0 spec for this specific test
        openapi3_spec = {
            "openapi": "3.0.0",
            "info": {"title": "Test API", "version": "1.0.0"},
            "paths": {
                "/users": {
                    "get": {
                        "summary": "Get a list of users",
                        "responses": {
                            "200": {
                                "description": "A list of users",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "array",
                                            "items": {
                                                "$ref": "#/components/schemas/User",
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
            "components": {
                "schemas": {
                    "Email": {"type": "string", "format": "email"},
                    "User": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "email": {"$ref": "#/components/schemas/Email"},
                        },
                        "required": ["id", "name"],
                    },
                }
            },
        }

        class PathAndMethodKey(t.NamedTuple):
            path: str
            http_method: str

        class OpenAPISchemaFromPath(OpenAPISchema[PathAndMethodKey]):
            @override
            def get_unresolved_schema(self, key: PathAndMethodKey) -> dict[str, t.Any]:
                return get_in(  # type: ignore[no-any-return]
                    [
                        "paths",
                        key.path,
                        key.http_method.lower(),
                        "responses",
                        "200",
                        "content",
                        "application/json",
                        "schema",
                    ],
                    self.spec,
                )

        source = OpenAPISchemaFromPath("dummy_path")
        source.spec = openapi3_spec  # type: ignore[assignment]

        expected_schema = {
            "type": "array",
            "items": {
                "type": ["object", "null"],
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "name": {"type": ["string", "null"]},
                    "email": {"type": ["string", "null"], "format": "email"},
                },
                "required": ["id", "name"],
            },
        }

        assert source.get_schema(PathAndMethodKey("/users", "GET")) == expected_schema


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

        stream_schema = StreamSchema(schema_source, key="bar")
        with pytest.raises(
            SchemaNotFoundError,
            match="Schema not found for 'bar'",
        ) as exc:
            stream_schema.get_stream_schema(stream, type(stream))
        assert isinstance(exc.value.__cause__, FileNotFoundError)

    def test_stream_schema_descriptor(
        self,
        foo_schema: dict[str, t.Any],
        schema_source: SchemaDirectory,
    ):
        """Test the StreamSchema descriptor."""

        class FooStream:
            name = "foo"
            schema: t.ClassVar[StreamSchema] = StreamSchema(schema_source)
            primary_keys = ("id",)

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
            primary_keys = ("id",)

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
            primary_keys = ("id",)

        stream = BarStream()
        with pytest.raises(
            SchemaNotFoundError,
            match="Schema not found for 'bar'",
        ) as exc:
            _ = stream.schema
        assert isinstance(exc.value.__cause__, FileNotFoundError)


class TestOpenAPISchemaNormalization:
    """Test OpenAPI schema normalization functionality."""

    @pytest.fixture
    def openapi(self) -> dict[str, t.Any]:
        """OpenAPI 3.0 spec with nullable attributes."""
        return {
            "openapi": "3.0.0",
            "info": {"title": "Test API", "version": "1.0.0"},
            "components": {
                "schemas": {
                    "User": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string", "nullable": True},
                            "email": {"type": "string", "nullable": True},
                            "age": {"type": "integer", "nullable": True},
                        },
                        "required": ["id"],
                    },
                    "Product": {
                        "type": "object",
                        "properties": {
                            "sku": {"type": "string"},
                            "title": {"type": "string", "nullable": True},
                            "tags": {
                                "type": "array",
                                "items": {"type": "string"},
                                "nullable": True,
                            },
                        },
                    },
                    "SimpleOneOf": {
                        "oneOf": [{"type": "string"}],
                    },
                    "NestedOneOf": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "oneOf": [{"type": "integer", "nullable": True}],
                            },
                        },
                    },
                    "AllOfNoElements": {
                        "allOf": [],
                    },
                    "AllOfSingleElement": {
                        "allOf": [{"type": "string"}],
                    },
                    "AllOfSchemas": {
                        "allOf": [
                            {
                                "properties": {
                                    "references": {
                                        "additionalProperties": {"type": "string"},
                                        "type": "object",
                                    }
                                },
                                "type": "object",
                            },
                            {
                                "properties": {
                                    "created": {"type": "string"},
                                    "name": {"type": "string"},
                                }
                            },
                        ],
                        "required": ["created", "name"],
                        "type": "object",
                    },
                    "Status": {
                        "type": "string",
                        "enum": ["active", "inactive", "pending"],
                    },
                    "Priority": {
                        "type": "integer",
                        "enum": [1, 2, 3, 4, 5],
                    },
                }
            },
        }

    def test_normalize(
        self,
        tmp_path: Path,
        openapi: dict[str, t.Any],
        subtests: pytest.Subtests,
    ):
        """Test that nullable attributes are converted to type arrays."""
        openapi_file = tmp_path / "openapi.json"
        openapi_file.write_text(json.dumps(openapi))

        source = OpenAPISchema(openapi_file)
        schema = source.get_schema("User")

        # Apply normalization without key properties
        normalized = source.preprocess_schema(schema)

        with subtests.test("nullable is handled"):
            # Check that nullable properties have type arrays
            assert normalized["properties"]["name"]["type"] == ["string", "null"]
            assert normalized["properties"]["email"]["type"] == ["string", "null"]
            assert normalized["properties"]["age"]["type"] == ["integer", "null"]

            # nullable attribute is removed
            assert "nullable" not in normalized["properties"]["name"]

        with subtests.test("oneOf is handled"):
            # Test simple oneOf
            schema = source.get_schema("SimpleOneOf")
            normalized = source.preprocess_schema(schema)
            assert normalized == {"type": "string"}
            assert "oneOf" not in normalized

            # Test nested oneOf with nullable
            schema = source.get_schema("NestedOneOf")
            normalized = source.preprocess_schema(schema)
            assert "oneOf" not in normalized["properties"]["value"]
            assert normalized["properties"]["value"]["type"] == ["integer", "null"]

        with subtests.test("allOf with no elements"):
            schema = source.get_schema("AllOfNoElements")
            normalized = source.preprocess_schema(schema)
            assert normalized == {}
            assert "allOf" not in normalized

        with subtests.test("allOf with a single element"):
            schema = source.get_schema("AllOfSingleElement")
            normalized = source.preprocess_schema(schema)
            assert normalized == {"type": "string"}
            assert "allOf" not in normalized

        with subtests.test("simple allOf is handled my merging schemas"):
            schema = source.get_schema("AllOfSchemas")
            normalized = source.preprocess_schema(schema)
            assert normalized == {
                "type": ["object", "null"],
                "properties": {
                    "references": {
                        "type": ["object", "null"],
                        "additionalProperties": {"type": "string"},
                    },
                    "created": {"type": ["string", "null"]},
                    "name": {"type": ["string", "null"]},
                },
            }
            assert "allOf" not in normalized

        with subtests.test("enum is handled"):
            # Test string enum
            schema = source.get_schema("Status")
            normalized = source.preprocess_schema(schema)
            assert "enum" not in normalized
            assert normalized["type"] == "string"

            # Test integer enum
            schema = source.get_schema("Priority")
            normalized = source.preprocess_schema(schema)
            assert "enum" not in normalized
            assert normalized["type"] == "integer"

        with subtests.test("arrays are handled"):
            schema = source.get_schema("Product")
            normalized = source.preprocess_schema(schema)

            # Check that the array itself can be nullable
            assert normalized["properties"]["tags"]["type"] == ["array", "null"]
