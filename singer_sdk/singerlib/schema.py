"""Alias for :mod:`singer.schema`."""

from __future__ import annotations

from singer.schema import Schema, resolve_schema_references

__all__ = [
    "Schema",
    "resolve_schema_references",
]
