"""Test the generic tests from `singer_sdk.helpers.testing`."""

from pathlib import Path

from singer_sdk.helpers.testing import get_standard_tap_tests
from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.samples.sample_tap_parquet.parquet_tap import SampleTapParquet


PARQUET_SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/testfile.parquet")
PARQUET_TEST_CONFIG = {"filepath": str(PARQUET_SAMPLE_FILENAME)}


def test_countries_tap_standard_tests():
    """Run standard tap tests against Countries tap."""
    tests = get_standard_tap_tests(SampleTapCountries)
    for test in tests:
        test()


def test_parquet_tap_standard_tests():
    """Run standard tap tests against Parquet tap."""
    tests = get_standard_tap_tests(SampleTapParquet, tap_config=PARQUET_TEST_CONFIG)
    for test in tests:
        test()
