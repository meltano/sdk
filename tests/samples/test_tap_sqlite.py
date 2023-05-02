from __future__ import annotations

import typing as t

from samples.sample_target_csv.csv_target import SampleTargetCSV
from singer_sdk import SQLStream
from singer_sdk._singerlib import MetadataMapping, StreamMetadata
from singer_sdk.testing import (
    get_standard_tap_tests,
    tap_to_target_sync_test,
)

if t.TYPE_CHECKING:
    from pathlib import Path

    from singer_sdk.tap_base import SQLTap


def _discover_and_select_all(tap: SQLTap) -> None:
    """Discover catalog and auto-select all streams."""
    for catalog_entry in tap.catalog_dict["streams"]:
        md = MetadataMapping.from_iterable(catalog_entry["metadata"])
        md.root.selected = True
        catalog_entry["metadata"] = md.to_list()


def test_sql_metadata(sqlite_sample_tap: SQLTap):
    stream = t.cast(SQLStream, sqlite_sample_tap.streams["main-t1"])
    detected_metadata = stream.catalog_entry["metadata"]
    detected_root_md = [md for md in detected_metadata if md["breadcrumb"] == []][0]
    detected_root_md = detected_root_md["metadata"]
    translated_metadata = StreamMetadata.from_dict(detected_root_md)
    assert detected_root_md["schema-name"] == translated_metadata.schema_name
    assert detected_root_md == translated_metadata.to_dict()
    md_map = MetadataMapping.from_iterable(stream.catalog_entry["metadata"])
    assert md_map[()].schema_name == "main"
    assert md_map[()].table_key_properties == ["c1"]


def test_sqlite_discovery(sqlite_sample_tap: SQLTap):
    _discover_and_select_all(sqlite_sample_tap)
    sqlite_sample_tap.sync_all()
    stream = t.cast(SQLStream, sqlite_sample_tap.streams["main-t1"])
    schema = stream.schema
    assert len(schema["properties"]) == 2
    assert stream.name == stream.tap_stream_id == "main-t1"

    md_map = MetadataMapping.from_iterable(stream.catalog_entry["metadata"])
    assert md_map[()] is not None
    assert md_map[()] is md_map.root
    assert md_map[()].schema_name == "main"

    assert stream.metadata.root.schema_name == "main"
    assert stream.fully_qualified_name == "main.t1"

    assert stream.metadata.root.table_key_properties == ["c1"]
    assert stream.primary_keys == ["c1"]


def test_sqlite_input_catalog(sqlite_sample_tap: SQLTap):
    sqlite_sample_tap.sync_all()
    stream = t.cast(SQLStream, sqlite_sample_tap.streams["main-t1"])
    assert len(stream.schema["properties"]) == 2
    assert len(stream.stream_maps[0].transformed_schema["properties"]) == 2

    for schema in [stream.schema, stream.stream_maps[0].transformed_schema]:
        assert len(schema["properties"]) == 2
        assert schema["properties"]["c1"] == {"type": ["integer", "null"]}
        assert schema["properties"]["c2"] == {"type": ["string", "null"]}
        assert stream.name == stream.tap_stream_id == "main-t1"

    md_map = MetadataMapping.from_iterable(stream.catalog_entry["metadata"])
    assert md_map[()] is not None
    assert md_map[()] is md_map.root
    assert md_map[()].schema_name == "main"

    # Fails here (schema is None):
    assert stream.metadata.root.schema_name == "main"
    assert stream.fully_qualified_name == "main.t1"


def test_sqlite_tap_standard_tests(sqlite_sample_tap: SQLTap):
    """Run standard tap tests against Countries tap."""
    tests = get_standard_tap_tests(
        type(sqlite_sample_tap),
        dict(sqlite_sample_tap.config),
    )
    for test in tests:
        test()


def test_sync_sqlite_to_csv(sqlite_sample_tap: SQLTap, tmp_path: Path):
    _discover_and_select_all(sqlite_sample_tap)
    orig_stdout, _, _, _ = tap_to_target_sync_test(
        sqlite_sample_tap,
        SampleTargetCSV(config={"target_folder": f"{tmp_path}/"}),
    )
