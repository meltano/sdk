"""Typing tests."""

import json
from copy import deepcopy
from io import StringIO
from pathlib import Path
from typing import Dict, cast
from uuid import uuid4

import pytest

from samples.sample_tap_sqlite import SQLiteConnector, SQLiteTap
from samples.sample_target_csv.csv_target import SampleTargetCSV
from samples.sample_target_sqlite import SQLiteTarget
from singer_sdk import SQLStream
from singer_sdk import typing as th
from singer_sdk.helpers._singer import MetadataMapping, StreamMetadata
from singer_sdk.tap_base import SQLTap
from singer_sdk.target_base import SQLTarget
from singer_sdk.testing import (
    _get_tap_catalog,
    get_standard_tap_tests,
    tap_sync_test,
    tap_to_target_sync_test,
    target_sync_test,
)

# Sample DB Setup and Config


@pytest.fixture
def path_to_sample_data_db(tmp_path: Path) -> Path:
    return tmp_path / Path("foo.db")


@pytest.fixture
def sqlite_sample_db_config(path_to_sample_data_db: str) -> dict:
    """Get configuration dictionary for target-csv."""
    return {"path_to_db": str(path_to_sample_data_db)}


@pytest.fixture
def sqlite_connector(sqlite_sample_db_config) -> SQLiteConnector:
    return SQLiteConnector(config=sqlite_sample_db_config)


@pytest.fixture
def sqlite_sample_db(sqlite_connector):
    """Return a path to a newly constructed sample DB."""
    for t in range(3):
        sqlite_connector.connection.execute(f"DROP TABLE IF EXISTS t{t}")
        sqlite_connector.connection.execute(
            f"CREATE TABLE t{t} (c1 int PRIMARY KEY, c2 varchar(10))"
        )
        for x in range(100):
            sqlite_connector.connection.execute(
                f"INSERT INTO t{t} VALUES ({x}, 'x={x}')"
            )


@pytest.fixture
def sqlite_sample_tap(sqlite_sample_db, sqlite_sample_db_config) -> SQLiteTap:
    _ = sqlite_sample_db
    catalog = _get_tap_catalog(
        SQLiteTap, config=sqlite_sample_db_config, select_all=True
    )
    return SQLiteTap(config=sqlite_sample_db_config, catalog=catalog)


# Target Test DB Setup and Config


@pytest.fixture
def path_to_target_db(tmp_path: Path) -> Path:
    return Path(f"{tmp_path}/target_test.db")


@pytest.fixture
def sqlite_target_test_config(path_to_target_db: str) -> dict:
    """Get configuration dictionary for target-csv."""
    return {"path_to_db": str(path_to_target_db)}


@pytest.fixture
def sqlite_sample_target(sqlite_target_test_config):
    """Get a sample target object."""
    return SQLiteTarget(sqlite_target_test_config)


def _discover_and_select_all(tap: SQLTap) -> None:
    """Discover catalog and auto-select all streams."""
    for catalog_entry in tap.catalog_dict["streams"]:
        md = MetadataMapping.from_iterable(catalog_entry["metadata"])
        md.root.selected = True
        catalog_entry["metadata"] = md.to_list()


# SQLite Tap Tests


def test_sql_metadata(sqlite_sample_tap: SQLTap):
    stream = cast(SQLStream, sqlite_sample_tap.streams["main-t1"])
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
    stream = cast(SQLStream, sqlite_sample_tap.streams["main-t1"])
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
    stream = cast(SQLStream, sqlite_sample_tap.streams["main-t1"])
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
        type(sqlite_sample_tap), dict(sqlite_sample_tap.config)
    )
    for test in tests:
        test()


def test_sync_sqlite_to_csv(sqlite_sample_tap: SQLTap, tmp_path: Path):
    _discover_and_select_all(sqlite_sample_tap)
    orig_stdout, _, _, _ = tap_to_target_sync_test(
        sqlite_sample_tap, SampleTargetCSV(config={"target_folder": f"{tmp_path}/"})
    )


# SQLite Target Tests


def test_sync_sqlite_to_sqlite(
    sqlite_sample_tap: SQLTap, sqlite_sample_target: SQLTarget
):
    """End-to-end-to-end test for SQLite tap and target.

    Test performs the following actions:

    - Extract sample data from SQLite tap.
    - Load data to SQLite target.
    - Extract data again from the target DB using the SQLite tap.
    - Confirm the STDOUT from the original sample DB matches with the
      STDOUT from the re-tapped target DB.
    """
    orig_stdout, _, _, _ = tap_to_target_sync_test(
        sqlite_sample_tap, sqlite_sample_target
    )
    orig_stdout.seek(0)
    tapped_config = dict(sqlite_sample_target.config)
    catalog = _get_tap_catalog(SQLiteTap, config=tapped_config, select_all=True)
    tapped_target = SQLiteTap(config=tapped_config, catalog=catalog)
    new_stdout, _ = tap_sync_test(tapped_target)

    orig_stdout.seek(0)
    orig_lines = orig_stdout.readlines()
    new_lines = new_stdout.readlines()
    assert len(orig_lines) > 0, "Orig tap output should not be empty."
    assert len(new_lines) > 0, "(Re-)tapped target output should not be empty."
    assert len(orig_lines) == len(new_lines)

    line_num = 0
    for line_num, orig_out, new_out in zip(
        range(len(orig_lines)), orig_lines, new_lines
    ):
        try:
            orig_json = json.loads(orig_out)
        except json.JSONDecodeError:
            raise RuntimeError(
                f"Could not parse JSON in orig line {line_num}: {orig_out}"
            )

        try:
            tapped_json = json.loads(new_out)
        except json.JSONDecodeError:
            raise RuntimeError(
                f"Could not parse JSON in new line {line_num}: {new_out}"
            )

        assert (
            tapped_json["type"] == orig_json["type"]
        ), f"Mismatched message type on line {line_num}."
        if tapped_json["type"] == "SCHEMA":
            assert (
                tapped_json["schema"]["properties"].keys()
                == orig_json["schema"]["properties"].keys()
            )
        if tapped_json["type"] == "RECORD":
            assert tapped_json["stream"] == orig_json["stream"]
            assert tapped_json["record"] == orig_json["record"]

    assert line_num > 0, "No lines read."


def test_sqlite_column_addition(sqlite_sample_target: SQLTarget):
    """End-to-end-to-end test for SQLite tap and target.

    Test performs the following actions:

    - Load a dataset with 1 column.
    - Ensure column count is 1.
    - Load a dataset with 2 columns.
    - Ensure column count is 2.
    """
    test_tbl = f"zzz_tmp_{str(uuid4()).split('-')[-1]}"
    props_a: Dict[str, dict] = {"col_a": th.StringType().to_dict()}
    props_b = deepcopy(props_a)
    props_b["col_b"] = th.IntegerType().to_dict()
    schema_msg_a, schema_msg_b = (
        {
            "type": "SCHEMA",
            "stream": test_tbl,
            "schema": {
                "type": "object",
                "properties": props,
            },
        }
        for props in [props_a, props_b]
    )
    tap_output_a = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg_a,
            {"type": "RECORD", "stream": test_tbl, "record": {"col_a": "samplerow1"}},
        ]
    )
    tap_output_b = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg_b,
            {
                "type": "RECORD",
                "stream": test_tbl,
                "record": {"col_a": "samplerow2", "col_b": 2},
            },
        ]
    )
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output_a), finalize=True)
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output_b), finalize=True)


def test_sqlite_column_morph(sqlite_sample_target: SQLTarget):
    """End-to-end-to-end test for SQLite tap and target.

    Test performs the following actions:

    - Load a column as an int.
    - Send a new column definition to redefine as string.
    - Ensure redefinition raises NotImplementedError, since column ALTERs are not
      supported by SQLite.
    """
    test_tbl = f"zzz_tmp_{str(uuid4()).split('-')[-1]}"
    props_a: Dict[str, dict] = {"col_a": th.IntegerType().to_dict()}
    props_b: Dict[str, dict] = {"col_a": th.StringType().to_dict()}
    schema_msg_a, schema_msg_b = (
        {
            "type": "SCHEMA",
            "stream": test_tbl,
            "schema": {
                "type": "object",
                "properties": props,
            },
        }
        for props in [props_a, props_b]
    )
    tap_output_a = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg_a,
            {"type": "RECORD", "stream": test_tbl, "record": {"col_a": 123}},
        ]
    )
    tap_output_b = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg_b,
            {
                "type": "RECORD",
                "stream": test_tbl,
                "record": {"col_a": "row-number-2"},
            },
        ]
    )
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output_a), finalize=True)
    with pytest.raises(NotImplementedError):
        # SQLite does not support altering column types.
        target_sync_test(
            sqlite_sample_target, input=StringIO(tap_output_b), finalize=True
        )


def test_sqlite_column_no_morph(sqlite_sample_target: SQLTarget):
    """End-to-end-to-end test for SQLite tap and target.

    Test performs the following actions:

    - Load a column as a string.
    - Send a new column definition to redefine as int.
    - Ensure int value can still insert.
    """
    test_tbl = f"zzz_tmp_{str(uuid4()).split('-')[-1]}"
    props_a: Dict[str, dict] = {"col_a": th.StringType().to_dict()}
    props_b: Dict[str, dict] = {"col_a": th.IntegerType().to_dict()}
    schema_msg_a, schema_msg_b = (
        {
            "type": "SCHEMA",
            "stream": test_tbl,
            "schema": {
                "type": "object",
                "properties": props,
            },
        }
        for props in [props_a, props_b]
    )
    tap_output_a = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg_a,
            {"type": "RECORD", "stream": test_tbl, "record": {"col_a": "123"}},
        ]
    )
    tap_output_b = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg_b,
            {
                "type": "RECORD",
                "stream": test_tbl,
                "record": {"col_a": 456},
            },
        ]
    )
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output_a), finalize=True)
    # Int should be inserted as string.
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output_b), finalize=True)
