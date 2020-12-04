"""Test class creation."""


from tap_base.tests.sample_tap_parquet.tap import SampleTapParquet

SAMPLE_FILENAME = "/tmp/testfile.parquet"
SAMPLE_CONFIG = {"filepath": SAMPLE_FILENAME}


def test_tap_class():
    """Test class creation."""
    _ = SampleTapParquet(config=SAMPLE_CONFIG, state=None)
