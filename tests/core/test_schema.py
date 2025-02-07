"""Test the schema sources."""

from __future__ import annotations

import typing as t

from singer_sdk.schema import LocalSchemaSource, OpenAPISchemaSource

if t.TYPE_CHECKING:
    import pytest


def test_local_schema_source(pytestconfig: pytest.Config):
    schema_dir = pytestconfig.rootpath / "tests/fixtures/schemas"
    schema_source = LocalSchemaSource(schema_dir)
    schema = schema_source("user")
    assert isinstance(schema, dict)
    assert schema["type"] == "object"
    assert "items" not in schema
    assert "properties" in schema
    assert "id" in schema["properties"]


def test_openapi_schema_source(pytestconfig: pytest.Config):
    openapi_path = pytestconfig.rootpath / "tests/fixtures/openapi.json"
    schema_source = OpenAPISchemaSource(openapi_path)
    schema = schema_source("ProjectListItem")
    assert isinstance(schema, dict)
    assert schema["type"] == "object"
    assert "items" not in schema
    assert "properties" in schema
    assert "id" in schema["properties"]
