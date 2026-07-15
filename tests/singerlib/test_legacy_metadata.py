"""Test the legacy singer-python functional metadata module."""

from __future__ import annotations

import pytest

import singer.metadata as metadata_
from singer.catalog import CatalogEntry, Metadata, MetadataMapping, StreamMetadata


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


def test_new_does_not_autovivify():
    """`new()` matches upstream `{}`, not a defaultdict.

    A defaultdict would silently create entries on access, e.g. via
    `breadcrumb in compiled` checks or plain indexing, which upstream
    singer-python's `metadata.new()` never did.
    """
    compiled = metadata_.new()
    assert compiled == {}
    with pytest.raises(KeyError):
        compiled[()]
    assert compiled == {}


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

    # Only key_properties are marked automatic, matching upstream
    # singer-python -- valid_replication_keys are not.
    assert compiled["properties", "id"]["inclusion"] == "automatic"
    assert compiled["properties", "updated_at"]["inclusion"] == "available"
    assert compiled["properties", "name"]["inclusion"] == "available"


def test_get_standard_metadata_no_schema():
    assert metadata_.get_standard_metadata() == []


def test_to_map_accepts_metadata_mapping():
    """`CatalogEntry.metadata` is a `MetadataMapping`, not the raw legacy list.

    Legacy taps that call `metadata.to_map(catalog_entry.metadata)` (as
    `pipelinewise-singer-python` callers do) must keep working.
    """
    mapping = MetadataMapping()
    mapping[()] = StreamMetadata(
        table_key_properties=["id"],
        replication_method="INCREMENTAL",
    )
    mapping["properties", "id"] = Metadata(inclusion=Metadata.InclusionType.AUTOMATIC)

    compiled = metadata_.to_map(mapping)

    assert compiled[()]["table-key-properties"] == ["id"]
    assert compiled[()]["replication-method"] == "INCREMENTAL"
    assert compiled["properties", "id"]["inclusion"] == "automatic"


def test_to_map_is_idempotent_on_already_compiled_map():
    compiled_once = metadata_.to_map([
        {"breadcrumb": [], "metadata": {"selected": True}},
    ])
    compiled_twice = metadata_.to_map(compiled_once)
    assert compiled_twice == compiled_once


def test_replication_method_and_view_key_properties_round_trip():
    """These two keys must survive a Catalog.load()-style round trip.

    They previously had no dataclass field on `StreamMetadata`, so
    `Metadata.from_dict`/`to_dict` silently dropped them.
    """
    entry = CatalogEntry.from_dict({
        "tap_stream_id": "my_stream",
        "schema": {"type": "object"},
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "replication-method": "INCREMENTAL",
                    "view-key-properties": ["id"],
                },
            },
        ],
    })

    root = entry.metadata.root
    assert root.replication_method == "INCREMENTAL"
    assert root.view_key_properties == ["id"]

    round_tripped = entry.metadata.to_list()
    compiled = metadata_.to_map(round_tripped)
    assert compiled[()]["replication-method"] == "INCREMENTAL"
    assert compiled[()]["view-key-properties"] == ["id"]
