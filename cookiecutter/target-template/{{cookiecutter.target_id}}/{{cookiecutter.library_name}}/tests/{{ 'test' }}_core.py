"""Tests standard target features using the built-in SDK tests library."""

import pytest
from typing import Dict, Any

from singer_sdk.testing import get_target_test_class

from {{ cookiecutter.library_name }}.target import Target{{ cookiecutter.destination_name }}


SAMPLE_CONFIG: Dict[str, Any] = {
    # TODO: Initialize minimal target config
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=Target{{ cookiecutter.destination_name }},
    config=SAMPLE_CONFIG
)


class TestTarget{{ cookiecutter.destination_name }}(StandardTargetTests):
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        yield "resource"


# TODO: Create additional tests as appropriate for your target.
