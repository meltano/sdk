"""Test class creation."""

import json
from pathlib import Path

from tap_base.samples.sample_tap_snowflake.snowflake_tap import SampleTapSnowflake

CONFIG_FILE = "tap_base/tests/.secrets/snowflake-config.json"

SAMPLE_CATALOG_FILEPATH = (
    "tap_base/samples/sample_tap_snowflake/snowflake-catalog.sample.json"
)


def test_snowflake_tap_init():
    """Test snowflake tap creation."""
    catalog_dict = json.loads(Path(SAMPLE_CATALOG_FILEPATH).read_text())
    _ = SampleTapSnowflake(config=CONFIG_FILE, state=None, catalog=catalog_dict)


def test_snowflake_sync_one():
    """Test snowflake discovery."""
    tap = SampleTapSnowflake(config=CONFIG_FILE, state=None)
    tap.sync_one(tap._streams[tap._streams.keys()[0]])
    assert True


def test_snowflake_discovery():
    """Test snowflake discovery."""
    tap = SampleTapSnowflake(config=CONFIG_FILE, state=None)
    catalog_json = tap.run_discovery()
    assert catalog_json
