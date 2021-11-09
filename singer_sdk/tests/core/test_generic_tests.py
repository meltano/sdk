"""Test the generic tests from `singer_sdk.testing`."""

import pytest

from pathlib import Path

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.testing import get_standard_tap_tests, TapTestRunner, AttributeTests

PARQUET_SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/testfile.parquet")
PARQUET_TEST_CONFIG = {"filepath": str(PARQUET_SAMPLE_FILENAME)}


@pytest.fixture(scope="session")
def tap_test_runner():
    runner = TapTestRunner(SampleTapCountries, {})
    runner.run_discovery()
    runner.run_sync()

    yield runner


def test_attribute_unique_test(tap_test_runner):
    t = AttributeTests.unique.value(
        test_runner=tap_test_runner, stream_name="countries", attribute_name="currency"
    )
    t.run_test()


def test_attribute_is_number_expectation(tap_test_runner):
    t = AttributeTests.is_number.value(
        test_runner=tap_test_runner, stream_name="countries", attribute_name="currency"
    )
    t.run_test()


def test_attribute_is_boolean_expectation(tap_test_runner):
    t = AttributeTests.is_boolean.value(
        test_runner=tap_test_runner, stream_name="countries", attribute_name="currency"
    )
    t.run_test()


def test_attribute_is_datetime_expectation(tap_test_runner):
    t = AttributeTests.is_number.value(
        test_runner=tap_test_runner, stream_name="countries", attribute_name="currency"
    )
    t.run_test()


def test_countries_tap_standard_tests():
    """Run standard tap tests against Countries tap."""
    tests = get_standard_tap_tests(SampleTapCountries)
    for test in tests:
        test()
