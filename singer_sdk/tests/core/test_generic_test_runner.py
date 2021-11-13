"""Test the generic tests from `singer_sdk.testing.utils`."""

import pytest

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.testing import TapTestRunner


runner = TapTestRunner(SampleTapCountries, {})
runner.run_discovery()
runner.run_sync()

pytest_tests = runner.generate_pytest_tests()

@pytest.fixture(scope="session")
def test_runner():
    yield runner


@pytest.mark.parametrize("test_object", **pytest_tests)
def test_builtin_tap_tests(test_object):
    test_object.run_test()
