"""Typing tests."""

from pathlib import Path
from typing import cast

import pytest

from singer_sdk import SQLStream
from singer_sdk.helpers._singer import MetadataMapping, StreamMetadata
from singer_sdk.samples.sample_tap_sqlite import SQLiteConnector, SQLiteTap
from singer_sdk.sinks.sql import SQLSink
from singer_sdk.tap_base import SQLTap
from singer_sdk.target_base import SQLTarget
from singer_sdk.testing import get_standard_tap_tests


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return Path(f"{tmp_path}/foo.db")


@pytest.fixture
def sqlite_url_local(db_path: Path) -> str:
    return f"sqlite:///{db_path}"


@pytest.fixture
def sqlite_url_inmem():
    return "sqlite://"


@pytest.fixture
def sqlite_config(db_path: Path) -> dict:
    """Get configuration dictionary for target-csv."""
    return {"path_to_db": str(db_path)}


@pytest.fixture
def sqlite_connector(sqlite_config) -> SQLiteConnector:
    return SQLiteConnector(config=sqlite_config)


@pytest.fixture
def sqlite_sample_db(sqlite_connector):
    """Return a path to a newly constructed sample DB."""
    for t in range(3):
        sqlite_connector.connection.execute(f"DROP TABLE IF EXISTS t{t}")
        sqlite_connector.connection.execute(
            f"CREATE TABLE t{t} (c1 int, c2 varchar(10))"
        )
        for x in range(100):
            sqlite_connector.connection.execute(
                f"INSERT INTO t{t} VALUES ({x}, 'x={x}')"
            )


@pytest.fixture
def sqlite_sampletap(sqlite_connector, sqlite_config):
    for t in range(3):
        sqlite_connector.connection.execute(f"DROP TABLE IF EXISTS t{t}")
        sqlite_connector.connection.execute(
            f"CREATE TABLE t{t} (c1 int, c2 varchar(10))"
        )
        for x in range(100):
            sqlite_connector.connection.execute(
                f"INSERT INTO t{t} VALUES ({x}, 'x={x}')"
            )

    return SQLiteTap(config=sqlite_config)


@pytest.fixture
def sqlite_sample_target(sqlite_connector):
    pass

    class SQLiteSink(SQLSink):
        pass

    class SQLiteTarget(SQLTarget):
        connector = sqlite_connector
        name = "target-sqlite-tester"
        default_sink_class = SQLiteSink

    return SQLiteTarget(config={"sqlalchemy_url": sqlite_connector.sqlalchemy_url})


def test_sql_metadata(sqlite_sampletap: SQLTap):
    stream = cast(SQLStream, sqlite_sampletap.streams["main-t1"])
    detected_metadata = stream.catalog_entry["metadata"]
    detected_root_md = [md for md in detected_metadata if md["breadcrumb"] == []][0]
    detected_root_md = detected_root_md["metadata"]
    translated_metadata = StreamMetadata.from_dict(detected_root_md)
    assert detected_root_md["schema-name"] == translated_metadata.schema_name
    assert detected_root_md == translated_metadata.to_dict()
    md_map = MetadataMapping.from_iterable(stream.catalog_entry["metadata"])
    assert md_map[()].schema_name == "main"


def test_sqlite_discovery(sqlite_sampletap: SQLTap):
    sqlite_sampletap.run_discovery()
    sqlite_sampletap.sync_all()
    stream = cast(SQLStream, sqlite_sampletap.streams["main-t1"])
    schema = stream.schema
    assert len(schema["properties"]) == 2
    assert stream.name == stream.tap_stream_id == "main-t1"

    md_map = MetadataMapping.from_iterable(stream.catalog_entry["metadata"])
    assert md_map[()] is not None
    assert md_map[()] is md_map.root
    assert md_map[()].schema_name == "main"

    # Fails here (schema is None):
    assert stream.metadata.root.schema_name == "main"
    assert stream.fully_qualified_name == "main.t1"


def test_sqlite_input_catalog(sqlite_sampletap: SQLTap):
    sqlite_sampletap.run_discovery()
    sqlite_sampletap.sync_all()
    stream = cast(SQLStream, sqlite_sampletap.streams["main-t1"])
    schema = stream.schema
    assert len(schema["properties"]) == 2
    assert stream.name == stream.tap_stream_id == "main-t1"

    md_map = MetadataMapping.from_iterable(stream.catalog_entry["metadata"])
    assert md_map[()] is not None
    assert md_map[()] is md_map.root
    assert md_map[()].schema_name == "main"

    # Fails here (schema is None):
    assert stream.metadata.root.schema_name == "main"
    assert stream.fully_qualified_name == "main.t1"


def test_sqlite_tap_standard_tests(sqlite_sampletap: SQLTap):
    """Run standard tap tests against Countries tap."""
    tests = get_standard_tap_tests(
        type(sqlite_sampletap), dict(sqlite_sampletap.config)
    )
    for test in tests:
        test()


def test_sqlite_target(sqlite_sample_target: SQLTarget):
    pass
