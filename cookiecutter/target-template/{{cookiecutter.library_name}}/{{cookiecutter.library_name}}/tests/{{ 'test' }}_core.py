"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from typing import Dict, Any

from singer_sdk.testing import get_standard_target_tests

from {{ cookiecutter.library_name }}.target import Target{{ cookiecutter.destination_name }}

SAMPLE_CONFIG: Dict[str, Any] = {
    # TODO: Initialize minimal target config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        Target{{ cookiecutter.destination_name }},
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
