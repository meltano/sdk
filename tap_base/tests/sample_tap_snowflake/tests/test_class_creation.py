"""Test class creation."""

import json
from pathlib import Path

from tap_base.tests.sample_tap_snowflake.snowflake_tap import SampleTapSnowflake

SAMPLE_CATALOG_FILEPATH = (
    "tap_base/tests/sample_tap_snowflake/tests/catalog.sample.json"
)
SAMPLE_CONFIG = {
    "account": "",
    "dbname": "",
    "user": "",
    "password": "",
    "warehouse": "",
    "tables": "",
}


def test_tap_class():
    """Test class creation."""
    catalog_dict = json.loads(Path(SAMPLE_CATALOG_FILEPATH).read_text())
    _ = SampleTapSnowflake(config=SAMPLE_CONFIG, state=None, catalog=catalog_dict)
