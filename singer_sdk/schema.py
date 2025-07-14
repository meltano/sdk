"""Schema sources."""

from __future__ import annotations

import functools
import json
import sys
import typing as t
from pathlib import Path

import requests

from singer_sdk.singerlib import resolve_schema_references

if sys.version_info < (3, 12):
    from importlib.abc import Traversable
else:
    from importlib.resources.abc import Traversable


class BaseSchemaSource:
    """Base schema source."""

    def __init__(self) -> None:
        """Initialize the schema source."""
        self._registry: dict[str, dict] = {}

    def get_schema(self, *args: t.Any, **kwargs: t.Any) -> dict:
        """Get schema from reference.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """
        msg = "Subclasses must implement this method."
        raise NotImplementedError(msg)

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> dict:
        """Get schema for the given stream name or reference.

        Returns:
            The schema dictionary.
        """
        return self.get_schema(*args, **kwargs)


class LocalSchemaSource(BaseSchemaSource):
    """Local schema source."""

    def __init__(self, path: Path | Traversable) -> None:
        """Initialize the schema source."""
        super().__init__()
        self.path = path

    def get_schema(self, name: str) -> dict:
        """Get schema from reference.

        Args:
            name: Name of the stream.

        Returns:
            The schema dictionary.
        """
        if name not in self._registry:
            schema_path = self.path / f"{name}.json"
            self._registry[name] = json.loads(schema_path.read_text())

        return self._registry[name]


class OpenAPISchemaSource(BaseSchemaSource):
    """OpenAPI schema source."""

    def __init__(self, path: str | Path | Traversable) -> None:
        """Initialize the schema source."""
        super().__init__()
        self.path = path

    @functools.cached_property
    def spec_dict(self) -> dict:
        """OpenAPI spec dictionary.

        Raises:
            ValueError: If the path type is not supported.
        """
        if isinstance(self.path, (Path, Traversable)):
            return json.loads(self.path.read_text())  # type: ignore[no-any-return]

        if self.path.startswith("http"):
            return requests.get(self.path, timeout=10).json()  # type: ignore[no-any-return]

        msg = f"Unsupported path type: {self.path}"
        raise ValueError(msg)

    def get_schema(self, ref: str) -> dict:
        """Get schema from reference.

        Args:
            ref: Reference to the schema.

        Returns:
            The schema dictionary.
        """
        if ref not in self._registry:
            schema = {"$ref": f"#/components/schemas/{ref}"}
            schema["components"] = self.spec_dict["components"]
            self._registry[ref] = resolve_schema_references(schema)

        return self._registry[ref]
