"""Test the generic tests from `singer_sdk.testing.utils`."""

import pytest

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.testing import get_standard_tap_tests_for_pytest


pytest_tests = get_standard_tap_tests_for_pytest(SampleTapCountries, {})


@pytest.mark.parametrize("test_object", **pytest_tests)
def test_builtin_tap_tests(test_object):
    test_object.run_test()
