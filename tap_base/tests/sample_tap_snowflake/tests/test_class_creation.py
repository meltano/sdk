"""Test class creation."""

from tap_base.tests.sample_tap_snowflake.tap import SampleTapSnowflake

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
    _ = SampleTapSnowflake(config=SAMPLE_CONFIG, state=None)
