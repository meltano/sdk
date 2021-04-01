"""Tests init and discovery features for {{ cookiecutter.tap_id }}."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from {{ cookiecutter.library_name }}.tap import Tap{{ cookiecutter.source_name }}

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config and/or register env vars in test harness
}


# Get built-in 'generic' tap tester from SDK:
def test_standard_tap_tests():
    """Run standard tap tests against {{ cookiecutter.source_name }}) tap."""
    tests = get_standard_tap_tests(
        Tap{{ cookiecutter.source_name }},
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
