"""Tests discovery features for Parquet."""

from tap_base.tests.sample_tap_parquet.tap import SampleTapParquet

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}


def test_parquet_tap_discovery():
    """Test class creation."""
    tap = SampleTapParquet(config=SAMPLE_CONFIG, state=None)
    catalog_json = tap.run_discovery()
    assert catalog_json
