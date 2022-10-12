"""Typing tests."""

import json
import sqlite3
from copy import deepcopy
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import Dict, cast
from uuid import uuid4

import pytest
import sqlalchemy

from samples.sample_tap_hostile import SampleTapHostile
from samples.sample_tap_sqlite import SQLiteConnector, SQLiteTap
from samples.sample_target_csv.csv_target import SampleTargetCSV
from samples.sample_target_sqlite import SQLiteSink, SQLiteTarget
from singer_sdk import SQLStream
from singer_sdk import typing as th
from singer_sdk._singerlib import Catalog, MetadataMapping, StreamMetadata
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
    catalog_obj = Catalog.from_dict(
        _get_tap_catalog(SQLiteTap, config=sqlite_sample_db_config, select_all=True)
    )

    # Set stream `t1` to use incremental replication.
    t0 = catalog_obj.get_stream("main-t0")
    t0.replication_key = "c1"
    t0.replication_method = "INCREMENTAL"
    t1 = catalog_obj.get_stream("main-t1")
    t1.key_properties = ["c1"]
    t1.replication_method = "FULL_TABLE"
    t2 = catalog_obj.get_stream("main-t2")
    t2.key_properties = ["c1"]
    t2.replication_key = "c1"
    t2.replication_method = "INCREMENTAL"
    return SQLiteTap(config=sqlite_sample_db_config, catalog=catalog_obj.to_dict())


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


@pytest.fixture
def sqlite_sample_target_soft_delete(sqlite_target_test_config):
    """Get a sample target object with hard_delete disabled."""
    conf = sqlite_target_test_config
    conf["hard_delete"] = False

    return SQLiteTarget(conf)


@pytest.fixture
def sqlite_sample_target_batch(sqlite_target_test_config):
    """Get a sample target object with hard_delete disabled."""
    conf = sqlite_target_test_config

    return SQLiteTarget(conf)


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


def test_sqlite_schema_addition(
    sqlite_target_test_config: dict, sqlite_sample_target: SQLTarget
):
    """Test that SQL-based targets attempt to create new schema if included in stream name."""
    schema_name = f"test_schema_{str(uuid4()).split('-')[-1]}"
    table_name = f"zzz_tmp_{str(uuid4()).split('-')[-1]}"
    test_stream_name = f"{schema_name}-{table_name}"
    schema_message = {
        "type": "SCHEMA",
        "stream": test_stream_name,
        "schema": {
            "type": "object",
            "properties": {"col_a": th.StringType().to_dict()},
        },
    }
    tap_output = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_message,
            {
                "type": "RECORD",
                "stream": test_stream_name,
                "record": {"col_a": "samplerow1"},
            },
        ]
    )
    # sqlite doesn't support schema creation
    with pytest.raises(sqlalchemy.exc.OperationalError) as excinfo:
        target_sync_test(
            sqlite_sample_target, input=StringIO(tap_output), finalize=True
        )
    # check the target at least tried to create the schema
    assert excinfo.value.statement == f"CREATE SCHEMA {schema_name}"


def test_sqlite_column_addition(sqlite_sample_target: SQLTarget):
    """End-to-end-to-end test for SQLite tap and target.

    Test performs the following actions:

    - Load a dataset with 1 column.
    - Load a dataset with 2 columns.
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


def test_sqlite_activate_version(
    sqlite_sample_target: SQLTarget, sqlite_sample_target_soft_delete: SQLTarget
):
    """Test handling the activate_version message for the SQLite target.

    Test performs the following actions:

    - Sends an activate_version message for a table that doesn't exist (which should
      have no effect)
    """
    test_tbl = f"zzz_tmp_{str(uuid4()).split('-')[-1]}"
    schema_msg = {
        "type": "SCHEMA",
        "stream": test_tbl,
        "schema": th.PropertiesList(th.Property("col_a", th.StringType())).to_dict(),
    }

    tap_output = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg,
            {"type": "ACTIVATE_VERSION", "stream": test_tbl, "version": 12345},
            {
                "type": "RECORD",
                "stream": test_tbl,
                "record": {"col_a": "samplerow1"},
                "version": 12345,
            },
        ]
    )

    target_sync_test(sqlite_sample_target, input=StringIO(tap_output), finalize=True)
    target_sync_test(
        sqlite_sample_target_soft_delete, input=StringIO(tap_output), finalize=True
    )


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


def test_sqlite_process_batch_message(
    sqlite_target_test_config: dict,
    sqlite_sample_target_batch: SQLiteTarget,
):
    """Test handling the batch message for the SQLite target.

    Test performs the following actions:

    - Sends a batch message for a table that doesn't exist (which should
      have no effect)
    """
    schema_message = {
        "type": "SCHEMA",
        "stream": "users",
        "key_properties": ["id"],
        "schema": {
            "required": ["id"],
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": ["null", "string"]},
            },
        },
    }
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl", "compression": "gzip"},
        "manifest": [
            "file://tests/core/resources/batch.1.jsonl.gz",
            "file://tests/core/resources/batch.2.jsonl.gz",
        ],
    }
    tap_output = "\n".join([json.dumps(schema_message), json.dumps(batch_message)])

    target_sync_test(
        sqlite_sample_target_batch,
        input=StringIO(tap_output),
        finalize=True,
    )
    db = sqlite3.connect(sqlite_target_test_config["path_to_db"])
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM users")
    assert cursor.fetchone()[0] == 4


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


@pytest.mark.parametrize(
    "stream_name,schema,key_properties,expected_dml",
    [
        (
            "test_stream",
            {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                },
            },
            [],
            dedent(
                """\
                INSERT INTO test_stream
                (id, name)
                VALUES (:id, :name)"""
            ),
        ),
    ],
    ids=[
        "no_key_properties",
    ],
)
def test_sqlite_generate_insert_statement(
    sqlite_sample_target: SQLiteTarget,
    stream_name: str,
    schema: dict,
    key_properties: list,
    expected_dml: str,
):
    sink = SQLiteSink(
        sqlite_sample_target,
        stream_name=stream_name,
        schema=schema,
        key_properties=key_properties,
    )

    dml = sink.generate_insert_statement(
        sink.full_table_name,
        sink.schema,
    )
    assert dml == expected_dml


def test_hostile_to_sqlite(
    sqlite_sample_target: SQLTarget, sqlite_target_test_config: dict
):
    tap = SampleTapHostile()
    tap_to_target_sync_test(tap, sqlite_sample_target)
    # check if stream table was created
    db = sqlite3.connect(sqlite_target_test_config["path_to_db"])
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [res[0] for res in cursor.fetchall()]
    assert "hostile_property_names_stream" in tables
    # check if columns were conformed
    cursor.execute(
        dedent(
            """
            SELECT
                p.name as columnName
            FROM sqlite_master m
            left outer join pragma_table_info((m.name)) p
                on m.name <> p.name
            where m.name = 'hostile_property_names_stream'
            ;
            """
        )
    )
    columns = {res[0] for res in cursor.fetchall()}
    assert columns == {
        "name_with_spaces",
        "name_is_camel_case",
        "name_with_dashes",
        "name_with_dashes_and_mixed_cases",
        "gname_starts_with_number",
        "fname_starts_with_number",
        "hname_starts_with_number",
        "name_with_emoji",
    }
