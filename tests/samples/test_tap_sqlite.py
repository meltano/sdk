from __future__ import annotations

import datetime
import json
import typing as t

import pytest
import time_machine
from click.testing import CliRunner

from samples.sample_tap_sqlite import SQLiteTap
from samples.sample_target_csv.csv_target import SampleTargetCSV
from singer_sdk._singerlib import MetadataMapping, StreamMetadata
from singer_sdk.testing import (
    get_standard_tap_tests,
    tap_sync_test,
    tap_to_target_sync_test,
)

if t.TYPE_CHECKING:
    from pathlib import Path

    from singer_sdk import SQLStream
    from singer_sdk.tap_base import SQLTap


def _discover_and_select_all(tap: SQLTap) -> None:
    """Discover catalog and auto-select all streams."""
    for catalog_entry in tap.catalog_dict["streams"]:
        md = MetadataMapping.from_iterable(catalog_entry["metadata"])
        md.root.selected = True
        catalog_entry["metadata"] = md.to_list()


def test_tap_sqlite_cli(sqlite_sample_db_config: dict[str, t.Any], tmp_path: Path):
    runner = CliRunner()
    filepath = tmp_path / "config.json"

    with filepath.open("w") as f:
        json.dump(sqlite_sample_db_config, f)

    result = runner.invoke(
        SQLiteTap.cli,
        ["--discover", "--config", str(filepath)],
    )
    assert result.exit_code == 0

    catalog = json.loads(result.stdout)
    assert "streams" in catalog


def test_sql_metadata(sqlite_sample_tap: SQLTap):
    stream = t.cast("SQLStream", sqlite_sample_tap.streams["main-t1"])
    detected_metadata = stream.catalog_entry["metadata"]
    detected_root_md = next(md for md in detected_metadata if md["breadcrumb"] == [])
    detected_root_md = detected_root_md["metadata"]
    translated_metadata = StreamMetadata.from_dict(detected_root_md)
    assert detected_root_md["schema-name"] == translated_metadata.schema_name
    assert detected_root_md == translated_metadata.to_dict()
    md_map = MetadataMapping.from_iterable(stream.catalog_entry["metadata"])

    stream_metadata = md_map[()]
    assert isinstance(stream_metadata, StreamMetadata)
    assert stream_metadata.schema_name == "main"
    assert stream_metadata.table_key_properties == ["c1"]


def test_sqlite_discovery(sqlite_sample_tap: SQLTap):
    _discover_and_select_all(sqlite_sample_tap)
    sqlite_sample_tap.sync_all()
    stream = t.cast("SQLStream", sqlite_sample_tap.streams["main-t1"])
    schema = stream.schema
    assert len(schema["properties"]) == 3
    assert stream.name == stream.tap_stream_id == "main-t1"

    md_map = MetadataMapping.from_iterable(stream.catalog_entry["metadata"])
    assert md_map[()] is not None
    assert md_map[()] is md_map.root
    assert md_map[()].schema_name == "main"

    assert stream.metadata.root.schema_name == "main"
    assert stream.fully_qualified_name == "main.t1"

    assert stream.metadata.root.table_key_properties == ["c1"]
    assert stream.primary_keys == ["c1"]
    assert stream.schema["properties"]["c1"] == {"type": ["integer"]}
    assert stream.schema["required"] == ["c1"]


def test_sqlite_input_catalog(sqlite_sample_tap: SQLTap):
    sqlite_sample_tap.sync_all()
    stream = t.cast("SQLStream", sqlite_sample_tap.streams["main-t1"])
    assert len(stream.schema["properties"]) == 3
    assert len(stream.stream_maps[0].transformed_schema["properties"]) == 3

    for schema in [stream.schema, stream.stream_maps[0].transformed_schema]:
        assert len(schema["properties"]) == 3
        assert schema["properties"]["c1"] == {"type": ["integer"]}
        assert schema["properties"]["c2"] == {
            "type": ["string", "null"],
            "maxLength": 10,
        }
        assert schema["properties"]["c3"] == {"type": ["string", "null"]}
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
    _, _, _, _ = tap_to_target_sync_test(
        sqlite_sample_tap,
        SampleTargetCSV(config={"target_folder": f"{tmp_path}/"}),
    )


@pytest.fixture
@time_machine.travel(
    datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
    tick=False,
)
def sqlite_sample_tap_state_messages(sqlite_sample_tap: SQLTap) -> list[dict]:
    stdout, _ = tap_sync_test(sqlite_sample_tap)
    state_messages = []
    for line in stdout.readlines():
        message = json.loads(line)
        if message["type"] == "STATE":
            state_messages.append(message)

    return state_messages


def test_sqlite_state(sqlite_sample_tap_state_messages):
    assert all(
        "progress_markers" not in bookmark
        for message in sqlite_sample_tap_state_messages
        for bookmark in message["value"]["bookmarks"].values()
    )
