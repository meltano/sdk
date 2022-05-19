"""Tests standard target features using the built-in SDK tests library."""

from typing import Dict, Any

from dotenv import load_dotenv

from singer_sdk.testing import get_standard_target_tests

from {{ cookiecutter.library_name }}.target import Target{{ cookiecutter.destination_name }}

load_dotenv()  # Import any environment variables from local `.env` file.

SAMPLE_CONFIG: Dict[str, Any] = {
    # TODO: Initialize minimal target config
}


# Run standard built-in target tests from the SDK:
def test_standard_target_tests():
    """Run standard target tests from the SDK."""
    tests = get_standard_target_tests(
        Target{{ cookiecutter.destination_name }},
        config=SAMPLE_CONFIG,
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your target.
