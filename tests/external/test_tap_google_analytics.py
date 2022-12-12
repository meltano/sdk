"""Tests standard tap features using the built-in SDK tests library."""

import warnings

from samples.sample_tap_google_analytics.ga_tap import SampleTapGoogleAnalytics
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.testing import TapTestRunner, get_test_class
from singer_sdk.testing.suites import (
    tap_stream_attribute_tests,
    tap_stream_tests,
    tap_tests,
)

from .conftest import ga_config

try:
    TestSampleTapGoogleAnalytics = get_test_class(
        test_runner=TapTestRunner(
            tap_class=SampleTapGoogleAnalytics,
            config=ga_config(),
            parse_env_config=True,
        ),
        test_suites=[tap_tests, tap_stream_tests, tap_stream_attribute_tests],
    )
except ConfigValidationError as e:
    warnings.warn(
        UserWarning(
            "Could not configure external gitlab tests. "
            f"Config in CI is expected via env vars.\n{e}"
        )
    )
