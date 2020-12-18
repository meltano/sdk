"""Test class creation."""

import json
from pathlib import Path

from tap_base.tests.sample_tap_snowflake.snowflake_tap import SampleTapSnowflake

CONFIG_FILE = "tap_base/tests/sample_tap_snowflake/tests/.secrets/tap-snowflake.json"
SAMPLE_CONFIG = json.loads(Path(CONFIG_FILE).read_text())

SAMPLE_CATALOG_FILEPATH = (
    "tap_base/tests/sample_tap_snowflake/tests/catalog.sample.json"
)


def test_snowflake_discovery():
    """Test snowflake discovery."""
    tap = SampleTapSnowflake(config=SAMPLE_CONFIG, state=None)
    tap.sync_one("SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM")
    assert True
