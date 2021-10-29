"""Typing tests."""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import pytest

from singer_sdk import SQLStream, SQLConnector
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


def test_sqlite_discovery(sqlite_sampletap: SQLTap):
    sqlite_sampletap.run_discovery()
    sqlite_sampletap.sync_all()
    stream = cast(SQLStream, sqlite_sampletap.streams["main-t1"])
    schema = stream.schema
    assert len(schema["properties"]) == 2
    assert stream.name == stream.tap_stream_id == "main-t1"

    # Fails here (schema is None):
    assert stream.metadata.root.schema_name == "main"
    assert stream.fully_qualified_name == "main.t1"
