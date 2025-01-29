"""Typing tests."""

from __future__ import annotations

import json
import sqlite3
import typing as t
from copy import deepcopy
from io import StringIO
from pathlib import Path
from textwrap import dedent
from uuid import uuid4

import pytest
import sqlalchemy as sa

from samples.sample_tap_hostile import SampleTapHostile
from samples.sample_tap_sqlite import SQLiteTap
from samples.sample_target_sqlite import SQLiteSink, SQLiteTarget
from singer_sdk import typing as th
from singer_sdk.testing import (
    tap_sync_test,
    tap_to_target_sync_test,
    target_sync_test,
)

if t.TYPE_CHECKING:
    from singer_sdk._singerlib import Catalog
    from singer_sdk.tap_base import SQLTap
    from singer_sdk.target_base import SQLTarget


def get_table(config: dict, table_name: str) -> sa.Table:
    """Get SQLAlchemy metadata and table for inspection.

    Args:
        config: Target configuration dictionary containing database path
        table_name: Name of the table to inspect

    Returns:
        Tuple of (metadata, table)
    """
    db_path = config["path_to_db"]
    engine = sa.create_engine(f"sqlite:///{db_path}")
    meta = sa.MetaData()
    meta.reflect(bind=engine)
    return meta.tables[table_name]


@pytest.fixture
def path_to_target_db(tmp_path: Path) -> Path:
    return Path(f"{tmp_path}/target_test.db")


@pytest.fixture
def sqlite_target_test_config(path_to_target_db: Path) -> dict:
    """Get configuration dictionary for target-csv."""
    return {"path_to_db": str(path_to_target_db)}


@pytest.fixture
def sqlite_sample_target(sqlite_target_test_config):
    """Get a sample target object."""
    return SQLiteTarget(config=sqlite_target_test_config)


@pytest.fixture
def sqlite_sample_target_hard_delete(sqlite_target_test_config):
    """Get a sample target object with hard_delete disabled."""
    return SQLiteTarget(
        config={
            **sqlite_target_test_config,
            "hard_delete": True,
            "add_record_metadata": False,
        }
    )


@pytest.fixture
def sqlite_sample_target_no_activate_version(sqlite_target_test_config):
    """Get a sample target object with hard_delete disabled."""
    return SQLiteTarget(
        config={
            **sqlite_target_test_config,
            "process_activate_version_messages": False,
        }
    )


@pytest.fixture
def sqlite_target_add_record_metadata(sqlite_target_test_config):
    """Get a sample target object with add_record_metadata enabled."""
    return SQLiteTarget(
        config={**sqlite_target_test_config, "add_record_metadata": True}
    )


@pytest.fixture
def sqlite_sample_target_batch(sqlite_target_test_config):
    """Get a sample target object with hard_delete disabled."""
    conf = sqlite_target_test_config

    return SQLiteTarget(config=conf)


# SQLite Target Tests


def test_sync_sqlite_to_sqlite(
    sqlite_sample_tap: SQLTap,
    sqlite_sample_target: SQLTarget,
    sqlite_sample_db_catalog: Catalog,
):
    """End-to-end-to-end test for SQLite tap and target.

    Test performs the following actions:

    - Extract sample data from SQLite tap.
    - Load data to SQLite target.
    - Extract data again from the target DB using the SQLite tap.
    - Confirm the STDOUT from the original sample DB matches with the
      STDOUT from the re-tapped target DB.
    """
    initial_state = deepcopy(sqlite_sample_tap.state)
    orig_stdout, _, _, _ = tap_to_target_sync_test(
        sqlite_sample_tap,
        sqlite_sample_target,
    )
    orig_stdout.seek(0)
    tapped_config = dict(sqlite_sample_target.config)
    tapped_target = SQLiteTap(
        config=tapped_config,
        state=initial_state,
        catalog=sqlite_sample_db_catalog.to_dict(),
    )
    new_stdout, _ = tap_sync_test(tapped_target)

    orig_stdout.seek(0)
    orig_lines = orig_stdout.readlines()
    new_lines = new_stdout.readlines()
    assert len(orig_lines) > 0, "Orig tap output should not be empty."
    assert len(new_lines) > 0, "(Re-)tapped target output should not be empty."
    assert orig_lines[0] == new_lines[0]
    assert "STATE" in new_lines[0]
    assert len(orig_lines) == len(new_lines)

    line_num = 0
    for line_num, orig_out, new_out in zip(
        range(len(orig_lines)),
        orig_lines,
        new_lines,
    ):
        try:
            orig_json = json.loads(orig_out)
        except json.JSONDecodeError as e:
            msg = f"Could not parse JSON in orig line {line_num}: {orig_out}"
            raise RuntimeError(msg) from e

        try:
            tapped_json = json.loads(new_out)
        except json.JSONDecodeError as e:
            msg = f"Could not parse JSON in new line {line_num}: {new_out}"
            raise RuntimeError(msg) from e

        assert tapped_json["type"] == orig_json["type"], (
            f"Mismatched message type on line {line_num}."
        )
        if tapped_json["type"] == "SCHEMA":
            assert (
                tapped_json["schema"]["properties"].keys()
                == orig_json["schema"]["properties"].keys()
            )
        if tapped_json["type"] == "RECORD":
            assert tapped_json["stream"] == orig_json["stream"]
            assert tapped_json["record"] == orig_json["record"]

    assert line_num > 0, "No lines read."


def test_sqlite_schema_addition(sqlite_sample_target: SQLTarget):
    """Test that SQL-based targets attempt to create new schema.

    It should attempt to create a schema if one is included in stream name,
    e.g. "schema_name-table_name".
    """
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
    with pytest.raises(sa.exc.OperationalError) as excinfo:
        target_sync_test(
            sqlite_sample_target,
            input=StringIO(tap_output),
            finalize=True,
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
    props_a: dict[str, dict] = {"col_a": th.StringType().to_dict()}
    props_b = deepcopy(props_a)
    props_b["col_b"] = th.IntegerType().to_dict()
    props_c = deepcopy(props_b)
    props_c["_col_c"] = th.IntegerType().to_dict()
    schema_msg_a, schema_msg_b, schema_msg_c = (
        {
            "type": "SCHEMA",
            "stream": test_tbl,
            "schema": {
                "type": "object",
                "properties": props,
            },
        }
        for props in [props_a, props_b, props_c]
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
    tap_output_c = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg_c,
            {
                "type": "RECORD",
                "stream": test_tbl,
                "record": {"col_a": "samplerow2", "col_b": 2, "_col_c": 3},
            },
        ]
    )
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output_a), finalize=True)
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output_b), finalize=True)
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output_c), finalize=True)


def test_sqlite_activate_version(
    sqlite_sample_target: SQLTarget,
    sqlite_sample_target_hard_delete: SQLTarget,
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
        sqlite_sample_target_hard_delete,
        input=StringIO(tap_output),
        finalize=True,
    )

    # Check that the record metadata was added
    table = get_table(sqlite_sample_target_hard_delete.config, test_tbl)

    assert "_sdc_table_version" in table.columns
    assert type(table.columns["_sdc_table_version"].type) is sa.INTEGER

    assert "_sdc_deleted_at" in table.columns
    assert type(table.columns["_sdc_deleted_at"].type) is sa.DATETIME


def test_sqlite_no_activate_version(
    sqlite_sample_target_no_activate_version: SQLTarget,
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

    target_sync_test(
        sqlite_sample_target_no_activate_version,
        input=StringIO(tap_output),
        finalize=True,
    )

    # Check that the record metadata was added
    table = get_table(sqlite_sample_target_no_activate_version.config, test_tbl)

    assert "col_a" in table.columns
    assert "_sdc_table_version" not in table.columns
    assert "_sdc_deleted_at" not in table.columns


def test_sqlite_add_record_metadata(sqlite_target_add_record_metadata: SQLTarget):
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

    target_sync_test(
        sqlite_target_add_record_metadata,
        input=StringIO(tap_output),
        finalize=True,
    )

    # Check that the record metadata was added
    table = get_table(sqlite_target_add_record_metadata.config, test_tbl)

    assert "_sdc_received_at" in table.columns
    assert type(table.columns["_sdc_received_at"].type) is sa.DATETIME

    assert "_sdc_sync_started_at" in table.columns
    assert type(table.columns["_sdc_sync_started_at"].type) is sa.INTEGER

    assert "_sdc_table_version" in table.columns
    assert type(table.columns["_sdc_table_version"].type) is sa.INTEGER


def test_sqlite_column_morph(sqlite_sample_target: SQLTarget):
    """End-to-end-to-end test for SQLite tap and target.

    Test performs the following actions:

    - Load a column as an int.
    - Send a new column definition to redefine as string.
    - Ensure redefinition raises NotImplementedError, since column ALTERs are not
      supported by SQLite.
    """
    test_tbl = f"zzz_tmp_{str(uuid4()).split('-')[-1]}"
    props_a: dict[str, dict] = {"col_a": th.IntegerType().to_dict()}
    props_b: dict[str, dict] = {"col_a": th.StringType().to_dict()}
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
            sqlite_sample_target,
            input=StringIO(tap_output_b),
            finalize=True,
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


def test_sqlite_process_batch_parquet(
    sqlite_target_test_config: dict,
    sqlite_sample_target_batch: SQLiteTarget,
):
    """Test handling a Parquet batch message for the SQLite target."""
    config = {
        **sqlite_target_test_config,
        "batch_config": {
            "encoding": {"format": "parquet", "compression": "gzip"},
            "batch_size": 100,
        },
    }
    schema_message = {
        "type": "SCHEMA",
        "stream": "continents",
        "key_properties": ["id"],
        "schema": {
            "required": ["id"],
            "type": "object",
            "properties": {
                "code": {"type": "string"},
                "name": {"type": "string"},
            },
        },
    }
    batch_message = {
        "type": "BATCH",
        "stream": "continents",
        "encoding": {"format": "parquet", "compression": "gzip"},
        "manifest": [
            "file://tests/core/resources/continents.parquet.gz",
        ],
    }
    tap_output = "\n".join([json.dumps(schema_message), json.dumps(batch_message)])

    target_sync_test(
        sqlite_sample_target_batch,
        input=StringIO(tap_output),
        finalize=True,
    )
    db = sqlite3.connect(config["path_to_db"])
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM continents")
    assert cursor.fetchone()[0] == 7


def test_sqlite_column_no_morph(sqlite_sample_target: SQLTarget):
    """End-to-end-to-end test for SQLite tap and target.

    Test performs the following actions:

    - Load a column as a string.
    - Send a new column definition to redefine as int.
    - Ensure int value can still insert.
    """
    test_tbl = f"zzz_tmp_{str(uuid4()).split('-')[-1]}"
    props_a: dict[str, dict] = {"col_a": th.StringType().to_dict()}
    props_b: dict[str, dict] = {"col_a": th.IntegerType().to_dict()}
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


def test_record_with_missing_properties(
    sqlite_sample_target: SQLTarget,
):
    """Test handling of records with missing properties."""
    tap_output = "\n".join(
        json.dumps(msg)
        for msg in [
            {
                "type": "SCHEMA",
                "stream": "test_stream",
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                    },
                },
                "key_properties": ["id"],
            },
            {
                "type": "RECORD",
                "stream": "test_stream",
                "record": {"id": 1},
            },
        ]
    )
    target_sync_test(sqlite_sample_target, input=StringIO(tap_output), finalize=True)


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
                    "table": {"type": "string"},
                },
            },
            [],
            'INSERT INTO test_stream (id, name, "table") VALUES (:id, :name, :table)',
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
    assert str(dml) == expected_dml


def test_hostile_to_sqlite(
    sqlite_sample_target: SQLTarget,
    sqlite_target_test_config: dict,
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
            """,
        ),
    )
    columns = {res[0] for res in cursor.fetchall()}
    assert columns == {
        "name_with_spaces",
        "nameiscamelcase",
        "name_with_dashes",
        "name_with_dashes_and_mixed_cases",
        "gname_starts_with_number",
        "fname_starts_with_number",
        "hname_starts_with_number",
        "name_with_emoji_",
    }


def test_overwrite_load_method(
    sqlite_target_test_config: dict,
):
    sqlite_target_test_config["load_method"] = "overwrite"
    target = SQLiteTarget(config=sqlite_target_test_config)
    test_tbl = f"zzz_tmp_{str(uuid4()).split('-')[-1]}"
    schema_msg = {
        "type": "SCHEMA",
        "stream": test_tbl,
        "schema": {
            "type": "object",
            "properties": {"col_a": th.StringType().to_dict()},
        },
    }

    tap_output_a = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg,
            {"type": "RECORD", "stream": test_tbl, "record": {"col_a": "123"}},
        ]
    )
    # Assert
    db = sqlite3.connect(sqlite_target_test_config["path_to_db"])
    cursor = db.cursor()

    target_sync_test(target, input=StringIO(tap_output_a), finalize=True)
    cursor.execute(f"SELECT col_a FROM {test_tbl} ;")  # noqa: S608
    records = [res[0] for res in cursor.fetchall()]
    assert records == ["123"]

    tap_output_b = "\n".join(
        json.dumps(msg)
        for msg in [
            schema_msg,
            {"type": "RECORD", "stream": test_tbl, "record": {"col_a": "456"}},
        ]
    )
    target = SQLiteTarget(config=sqlite_target_test_config)
    target_sync_test(target, input=StringIO(tap_output_b), finalize=True)
    cursor.execute(f"SELECT col_a FROM {test_tbl} ;")  # noqa: S608
    records = [res[0] for res in cursor.fetchall()]
    assert records == ["456"]
