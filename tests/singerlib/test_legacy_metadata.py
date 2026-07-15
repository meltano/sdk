"""Test the legacy singer-python functional metadata module."""

from __future__ import annotations

import singer.metadata as metadata_


def test_to_map_and_to_list_round_trip():
    raw = [
        {"breadcrumb": [], "metadata": {"table-key-properties": ["id"]}},
        {
            "breadcrumb": ["properties", "id"],
            "metadata": {"inclusion": "automatic"},
        },
    ]
    compiled = metadata_.to_map(raw)
    assert compiled[()]["table-key-properties"] == ["id"]
    assert compiled["properties", "id"]["inclusion"] == "automatic"

    round_tripped = metadata_.to_list(compiled)
    assert {tuple(m["breadcrumb"]): m["metadata"] for m in round_tripped} == compiled


def test_write_and_get():
    compiled = metadata_.new()
    metadata_.write(compiled, (), "selected", True)
    assert metadata_.get(compiled, (), "selected") is True
    assert metadata_.get(compiled, (), "missing") is None


def test_delete():
    compiled = metadata_.new()
    metadata_.write(compiled, (), "selected", True)
    metadata_.delete(compiled, (), "selected")
    assert metadata_.get(compiled, (), "selected") is None


def test_get_standard_metadata():
    schema = {
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "updated_at": {"type": "string"},
        },
    }
    entries = metadata_.get_standard_metadata(
        schema=schema,
        schema_name="my_stream",
        key_properties=["id"],
        valid_replication_keys=["updated_at"],
        replication_method="INCREMENTAL",
    )
    compiled = metadata_.to_map(entries)

    assert compiled[()]["table-key-properties"] == ["id"]
    assert compiled[()]["forced-replication-method"] == "INCREMENTAL"
    assert compiled[()]["valid-replication-keys"] == ["updated_at"]
    assert compiled[()]["inclusion"] == "available"
    assert compiled[()]["schema-name"] == "my_stream"

    assert compiled["properties", "id"]["inclusion"] == "automatic"
    assert compiled["properties", "updated_at"]["inclusion"] == "automatic"
    assert compiled["properties", "name"]["inclusion"] == "available"


def test_get_standard_metadata_no_schema():
    assert metadata_.get_standard_metadata() == []
