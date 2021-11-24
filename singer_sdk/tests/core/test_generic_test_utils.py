"""Test the generic tests from `singer_sdk.testing.utils`."""

from pathlib import Path

import pytest

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.testing import (
    get_standard_tap_pytest_parameters,
    get_standard_tap_tests,
)

PARQUET_SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/testfile.parquet")
PARQUET_TEST_CONFIG = {"filepath": str(PARQUET_SAMPLE_FILENAME)}


pytest_tests = get_standard_tap_pytest_parameters(
    SampleTapCountries,
    {},
    include_tap_tests=True,
    include_stream_tests=True,
    include_attribute_tests=True,
)


@pytest.mark.parametrize("test_object", **pytest_tests)
def test_countries_standard_pytest_tap_tests(test_object):
    test_object.run_test()


def test_countries_tap_standard_tests():
    """Run standard tap tests against Countries tap."""
    tests = get_standard_tap_tests(SampleTapCountries)
    for test in tests:
        test()
