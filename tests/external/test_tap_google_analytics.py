"""Tests standard tap features using the built-in SDK tests library."""

from samples.sample_tap_google_analytics.ga_tap import SampleTapGoogleAnalytics
from singer_sdk.testing import TapTestRunner, get_test_class, pytest_generate_tests
from singer_sdk.testing.suites import (
    tap_stream_attribute_tests,
    tap_stream_tests,
    tap_tests,
)

from .conftest import ga_config

TestSampleTapGoogleAnalytics = get_test_class(
    test_runner=TapTestRunner(tap_class=SampleTapGoogleAnalytics, config=ga_config()),
    test_suites=[tap_tests, tap_stream_tests, tap_stream_attribute_tests],
)
