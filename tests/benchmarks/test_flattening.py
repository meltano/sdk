"""Benchmarks for flattening logic."""

from __future__ import annotations

import pytest

from singer_sdk.helpers._flattening import (
    flatten_key,
    flatten_record,
    flatten_schema,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def short_key_inputs() -> tuple[str, list[str]]:
    return "record_id", ["parent_a", "parent_b"]


@pytest.fixture
def long_key_inputs() -> tuple[str, list[str]]:
    segment = "very_long_key_name_that_contains_many_characters"
    parents = [
        "first_extremely_long_parent_key_name_with_many_words",
        "second_extremely_long_parent_key_name_with_many_words",
        "third_extremely_long_parent_key_name_with_many_words",
        "fourth_extremely_long_parent_key_name_with_many_words",
        "fifth_extremely_long_parent_key_name_with_many_words",
    ]
    return segment, parents


@pytest.fixture
def wide_flat_record() -> dict:
    return {f"field_{i}": i * 1.5 for i in range(50)}


@pytest.fixture
def wide_flat_schema(wide_flat_record: dict) -> dict:
    return {
        "type": "object",
        "properties": {k: {"type": "number"} for k in wide_flat_record},
    }


def _make_nested_record(depth: int) -> dict:
    if depth == 0:
        return {"leaf_a": 1, "leaf_b": "x", "leaf_c": True}
    return {f"level_{depth}_key_{i}": _make_nested_record(depth - 1) for i in range(3)}


def _make_nested_schema(depth: int) -> dict:
    if depth == 0:
        return {
            "type": "object",
            "properties": {
                "leaf_a": {"type": "integer"},
                "leaf_b": {"type": "string"},
                "leaf_c": {"type": "boolean"},
            },
        }
    return {
        "type": "object",
        "properties": {
            f"level_{depth}_key_{i}": _make_nested_schema(depth - 1) for i in range(3)
        },
    }


@pytest.fixture
def deep_nested_record() -> dict:
    return _make_nested_record(4)


@pytest.fixture
def deep_nested_schema() -> dict:
    return _make_nested_schema(4)


@pytest.fixture
def wide_schema() -> dict:
    props: dict = {f"scalar_{i}": {"type": "string"} for i in range(40)}
    for i in range(10):
        props[f"obj_{i}"] = {
            "type": "object",
            "properties": {
                "inner_a": {"type": "string"},
                "inner_b": {"type": "integer"},
            },
        }
    return {"type": "object", "properties": props}


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------


def test_bench_flatten_key_short(
    benchmark,
    short_key_inputs: tuple[str, list[str]],
) -> None:
    """Benchmark flatten_key for a short key (no truncation needed)."""
    key, parents = short_key_inputs
    benchmark(flatten_key, key, parents)


def test_bench_flatten_key_long(
    benchmark,
    long_key_inputs: tuple[str, list[str]],
) -> None:
    """Benchmark flatten_key for a long key (triggers key-shortening while loop)."""
    key, parents = long_key_inputs
    benchmark(flatten_key, key, parents)


def test_bench_flatten_record_wide(
    benchmark,
    wide_flat_record: dict,
    wide_flat_schema: dict,
) -> None:
    """Benchmark flatten_record for a wide flat record (no nesting)."""
    flattened = flatten_schema(wide_flat_schema, max_level=0)
    benchmark(flatten_record, wide_flat_record, flattened, max_level=0)


def test_bench_flatten_record_deep(
    benchmark,
    deep_nested_record: dict,
    deep_nested_schema: dict,
) -> None:
    """Benchmark flatten_record for a deeply nested record (max_level=3)."""
    flattened = flatten_schema(deep_nested_schema, max_level=3)
    benchmark(flatten_record, deep_nested_record, flattened, max_level=3)


def test_bench_flatten_schema_wide(
    benchmark,
    wide_schema: dict,
) -> None:
    """Benchmark flatten_schema for a wide schema (many properties, shallow nesting)."""
    benchmark(flatten_schema, wide_schema, max_level=1)


def test_bench_flatten_schema_deep(
    benchmark,
    deep_nested_schema: dict,
) -> None:
    """Benchmark flatten_schema for a deeply nested schema (max_level=3)."""
    benchmark(flatten_schema, deep_nested_schema, max_level=3)
