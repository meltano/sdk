"""Alias for :mod:`singer.schema`."""

from __future__ import annotations

from singer.schema import META_KEYS, STANDARD_KEYS, Schema, resolve_schema_references

__all__ = [
    "META_KEYS",
    "STANDARD_KEYS",
    "Schema",
    "resolve_schema_references",
]
