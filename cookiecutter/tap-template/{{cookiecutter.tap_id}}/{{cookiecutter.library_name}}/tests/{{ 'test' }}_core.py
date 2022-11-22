"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import (
    TapTestRunner,
    get_test_class,
    pytest_generate_tests # pytest hook function, required for standard tests
)
from singer_sdk.testing.suites import (
    tap_stream_attribute_tests,
    tap_stream_tests,
    tap_tests,
)

from {{ cookiecutter.library_name }}.tap import Tap{{ cookiecutter.source_name }}

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
TestTap{{ cookiecutter.source_name }} = get_test_class(
    test_runner=TapTestRunner(tap_class=Tap{{ cookiecutter.source_name }}, config=SAMPLE_CONFIG),
    test_suites=[tap_tests, tap_stream_tests, tap_stream_attribute_tests],
)


# TODO: Create additional tests as appropriate for your tap.
