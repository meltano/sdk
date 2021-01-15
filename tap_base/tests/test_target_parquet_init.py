"""Test class creation."""

from tap_base.samples.sample_target_parquet.parquet_target import SampleTargetParquet

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}


def test_target_class():
    """Test class creation."""
    _ = SampleTargetParquet(config=SAMPLE_CONFIG)
