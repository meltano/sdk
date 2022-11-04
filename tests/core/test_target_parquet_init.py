"""Test class creation."""

import pytest

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}


@pytest.mark.skipif("sys.version_info >= (3,11)")
def test_target_class():
    """Test class creation."""
    from samples.sample_target_parquet.parquet_target import SampleTargetParquet

    _ = SampleTargetParquet(config=SAMPLE_CONFIG)
