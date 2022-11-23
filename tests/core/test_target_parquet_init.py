"""Test class creation."""

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}


def test_target_class():
    """Test class creation."""
    from samples.sample_target_parquet.parquet_target import SampleTargetParquet

    _ = SampleTargetParquet(config=SAMPLE_CONFIG)
