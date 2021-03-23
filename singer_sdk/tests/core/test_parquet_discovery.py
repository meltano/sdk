"""Tests discovery features for Parquet."""

from pathlib import Path

from singer_sdk.samples.sample_tap_parquet.parquet_tap import SampleTapParquet

PARQUET_SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/testfile.parquet")
PARQUET_TEST_CONFIG = {"filepath": str(PARQUET_SAMPLE_FILENAME)}


def test_parquet_tap_discovery():
    """Test class creation."""
    tap = SampleTapParquet(config=PARQUET_TEST_CONFIG, state=None)
    catalog_json = tap.run_discovery()
    assert catalog_json
