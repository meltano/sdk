"""Tests discovery features for Parquet."""

from singer_sdk.samples.sample_tap_parquet.parquet_tap import SampleTapParquet

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}


def test_parquet_tap_discovery():
    """Test class creation."""
    tap = SampleTapParquet(config=SAMPLE_CONFIG, state=None)
    catalog_json = tap.run_discovery()
    assert catalog_json
