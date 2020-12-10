"""Test class creation."""

from tap_base.tests.sample_target_parquet.target import SampleTargetParquet

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}


def test_target_class():
    """Test class creation."""
    _ = SampleTargetParquet(config=SAMPLE_CONFIG)
