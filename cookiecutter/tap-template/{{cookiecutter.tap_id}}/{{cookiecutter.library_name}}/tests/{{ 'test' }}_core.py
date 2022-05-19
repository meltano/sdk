"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from dotenv import load_dotenv

from singer_sdk.testing import get_standard_tap_tests

from {{ cookiecutter.library_name }}.tap import Tap{{ cookiecutter.source_name }}

load_dotenv()  # Import any environment variables from local `.env` file.

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        Tap{{ cookiecutter.source_name }},
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
