"""Tap, target and stream test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text

from samples.sample_tap_sqlite import SQLiteConnector, SQLiteTap
from singer_sdk._singerlib import Catalog
from singer_sdk.testing import _get_tap_catalog


@pytest.fixture
def csv_config(outdir: str) -> dict:
    """Get configuration dictionary for target-csv."""
    return {"target_folder": outdir}


@pytest.fixture
def _sqlite_sample_db(sqlite_connector):
    """Return a path to a newly constructed sample DB."""
    with sqlite_connector._connect() as conn, conn.begin():
        for t in range(3):
            conn.execute(text(f"DROP TABLE IF EXISTS t{t}"))
            conn.execute(
                text(f"CREATE TABLE t{t} (c1 int PRIMARY KEY, c2 varchar(10))"),
            )
            for x in range(100):
                conn.execute(
                    text(f"INSERT INTO t{t} VALUES ({x}, 'x={x}')"),  # noqa: S608
                )


@pytest.fixture
def sqlite_sample_tap(
    _sqlite_sample_db,
    sqlite_sample_db_config,
    sqlite_sample_db_state,
) -> SQLiteTap:
    _ = _sqlite_sample_db
    catalog_obj = Catalog.from_dict(
        _get_tap_catalog(SQLiteTap, config=sqlite_sample_db_config, select_all=True),
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
    return SQLiteTap(
        config=sqlite_sample_db_config,
        catalog=catalog_obj.to_dict(),
        state=sqlite_sample_db_state,
    )


@pytest.fixture
def sqlite_connector(sqlite_sample_db_config) -> SQLiteConnector:
    return SQLiteConnector(config=sqlite_sample_db_config)


@pytest.fixture
def path_to_sample_data_db(tmp_path: Path) -> Path:
    return tmp_path / Path("foo.db")


@pytest.fixture
def sqlite_sample_db_config(path_to_sample_data_db: str) -> dict:
    """Get configuration dictionary for target-csv."""
    return {
        "path_to_db": str(path_to_sample_data_db),
        "start_date": "2010-01-01T00:00:00Z",
    }


@pytest.fixture
def sqlite_sample_db_state() -> dict:
    """Get configuration dictionary for target-csv."""
    return {
        "bookmarks": {
            "main-t0": {"replication_key": "c1", "replication_key_value": 55},
        },
    }
