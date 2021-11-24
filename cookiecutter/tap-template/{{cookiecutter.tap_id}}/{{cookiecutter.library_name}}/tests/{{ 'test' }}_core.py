"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import pytest

from singer_sdk.testing import get_standard_tap_pytest_parameters

from {{ cookiecutter.library_name }}.tap import Tap{{ cookiecutter.source_name }}

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}

test_params = get_standard_tap_pytest_parameters(
    tap_class=Tap{{ cookiecutter.source_name }},
    tap_config=SAMPLE_CONFIG,
    include_tap_tests=True,
    include_stream_tests=True,
    include_attribute_tests=True
)

# Run standard built-in tap tests from the SDK:
@pytest.parametrize("test", **test_params)
def test_standard_tap_tests(test):
    """Run standard tap tests from the SDK."""
    test.run_test()

# TODO: Create additional tests as appropriate for your tap.
