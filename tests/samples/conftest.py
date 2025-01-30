"""Tap, target and stream test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa

from samples.sample_tap_sqlite import SQLiteConnector, SQLiteTap
from singer_sdk._singerlib import Catalog
from singer_sdk.testing import _get_tap_catalog


@pytest.fixture
def csv_config(outdir: str) -> dict:
    """Get configuration dictionary for target-csv."""
    return {"target_folder": outdir, "add_record_metadata": False}


@pytest.fixture
def sqlite_sample_db(sqlite_connector: SQLiteConnector):
    """Return a path to a newly constructed sample DB."""
    with sqlite_connector._connect() as conn, conn.begin():
        for t in range(3):
            conn.execute(sa.text(f"DROP TABLE IF EXISTS t{t}"))
            conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE t{t} (
                        c1 int PRIMARY KEY NOT NULL,
                        c2 varchar(10) NOT NULL,
                        c3 text NOT NULL
                    )
                    """
                ),
            )
            for x in range(100):
                conn.execute(
                    sa.text(f"INSERT INTO t{t} VALUES ({x}, 'x={x}', 'y={x}')"),  # noqa: S608
                )


@pytest.fixture
def sqlite_sample_db_catalog(sqlite_sample_db_config) -> Catalog:
    catalog_obj = Catalog.from_dict(
        _get_tap_catalog(SQLiteTap, config=sqlite_sample_db_config, select_all=True),
    )

    # Set stream `t1` to use incremental replication.
    t0 = catalog_obj.get_stream("main-t0")
    assert t0 is not None

    t0.replication_key = "c1"
    t0.replication_method = "INCREMENTAL"

    t1 = catalog_obj.get_stream("main-t1")
    assert t1 is not None

    t1.key_properties = ["c1"]
    t1.replication_method = "FULL_TABLE"

    t2 = catalog_obj.get_stream("main-t2")
    assert t2 is not None

    t2.key_properties = ["c1"]
    t2.replication_key = "c1"
    t2.replication_method = "INCREMENTAL"

    return catalog_obj


@pytest.fixture
def sqlite_sample_tap(
    sqlite_sample_db,
    sqlite_sample_db_config,
    sqlite_sample_db_state,
    sqlite_sample_db_catalog,
) -> SQLiteTap:
    _ = sqlite_sample_db
    return SQLiteTap(
        config=sqlite_sample_db_config,
        catalog=sqlite_sample_db_catalog.to_dict(),
        state=sqlite_sample_db_state,
    )


@pytest.fixture
def sqlite_connector(sqlite_sample_db_config) -> SQLiteConnector:
    return SQLiteConnector(config=sqlite_sample_db_config)


@pytest.fixture
def path_to_sample_data_db(tmp_path: Path) -> Path:
    return tmp_path / Path("foo.db")


@pytest.fixture
def sqlite_sample_db_config(path_to_sample_data_db: Path) -> dict:
    """Get configuration dictionary for target-csv."""
    return {"path_to_db": str(path_to_sample_data_db)}


@pytest.fixture
def sqlite_sample_db_state() -> dict:
    """Get configuration dictionary for target-csv."""
    return {
        "bookmarks": {
            "main-t0": {"replication_key": "c1", "replication_key_value": 55},
        },
    }
