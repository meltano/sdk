"""Test class creation."""

import json
from pathlib import Path

from tap_base.tests.sample_tap_snowflake.snowflake_tap import SampleTapSnowflake

CONFIG_FILE = "tap_base/tests/sample_tap_snowflake/tests/.secrets/tap-snowflake.json"
SAMPLE_CONFIG = json.loads(Path(CONFIG_FILE).read_text())

SAMPLE_CATALOG_FILEPATH = (
    "tap_base/tests/sample_tap_snowflake/tests/catalog.sample.json"
)


def test_tap_class():
    """Test snowflake tap creation."""
    catalog_dict = json.loads(Path(SAMPLE_CATALOG_FILEPATH).read_text())
    _ = SampleTapSnowflake(config=SAMPLE_CONFIG, state=None, catalog=catalog_dict)
