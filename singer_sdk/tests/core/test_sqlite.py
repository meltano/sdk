"""Typing tests."""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import pytest

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers._singer import MetadataMapping, StreamMetadata
from singer_sdk.tap_base import SQLTap


@pytest.fixture
def temp_dir() -> Path:
    return TemporaryDirectory("test-output")


@pytest.fixture
def sqlite_url_local(temp_dir: Path):
    return "sqlite:///foo.db"


@pytest.fixture
def sqlite_url_inmem():
    return "sqlite://"


@pytest.fixture
def sqlite_connector(sqlite_url_local):
    return SQLConnector(config=None, sqlalchemy_url=sqlite_url_local)


@pytest.fixture
def sqlite_sampletap(sqlite_connector):
    for t in range(3):
        sqlite_connector.connection.execute(f"DROP TABLE IF EXISTS t{t}")
        sqlite_connector.connection.execute(
            f"CREATE TABLE t{t} (c1 int, c2 varchar(10))"
        )
        for x in range(100):
            sqlite_connector.connection.execute(
                f"INSERT INTO t{t} VALUES ({x}, 'x={x}')"
            )

    class SQLiteStream(SQLStream):
        pass

    class SQLiteTap(SQLTap):
        connector = sqlite_connector
        name = "tap-sqlite-tester"
        default_stream_class = SQLiteStream

    return SQLiteTap(config={"sqlalchemy_url": sqlite_connector.sqlalchemy_url})


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
