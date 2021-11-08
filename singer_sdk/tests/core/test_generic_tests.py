"""Test the generic tests from `singer_sdk.testing`."""

import pytest

from pathlib import Path

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.testing import get_standard_tap_tests, TapTestRunner, AttributeTests

PARQUET_SAMPLE_FILENAME = Path(__file__).parent / Path("./resources/testfile.parquet")
PARQUET_TEST_CONFIG = {"filepath": str(PARQUET_SAMPLE_FILENAME)}


runner = TapTestRunner(SampleTapCountries, {})
runner.run_discovery()
runner.run_sync()

# pytest_params = test_runner.generate_built_in_tests()


@pytest.fixture(scope="session")
def tap_test_runner():
    yield runner

def test_is_number_expectation(tap_test_runner):
    test_class = AttributeTests.is_number.value
    params = {"test_runner": tap_test_runner, "stream_name": "countries", "attribute_name": "currency"}
    e = test_class(**params)
    e.run_test()

# @pytest.mark.parametrize("test_config", **pytest_params)
# def test_builtin_tap_tests(test_util, test_config):
#     test_name, params = test_config
#     test_func = test_util.available_tests[test_name]
#     test_func(**params)


def test_countries_tap_standard_tests():
    """Run standard tap tests against Countries tap."""
    tests = get_standard_tap_tests(SampleTapCountries)
    for test in tests:
        test()

