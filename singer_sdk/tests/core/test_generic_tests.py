"""Test the generic tests from `singer_sdk.testing`."""

from pathlib import Path

from singer_sdk.testing import get_standard_tap_tests
from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries


PARQUET_SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/testfile.parquet")
PARQUET_TEST_CONFIG = {"filepath": str(PARQUET_SAMPLE_FILENAME)}


def test_countries_tap_standard_tests():
    """Run standard tap tests against Countries tap."""
    tests = get_standard_tap_tests(SampleTapCountries)
    for test in tests:
        test()
