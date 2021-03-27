"""Tests init and discovery features for {{ cookiecutter.tap_id }}."""

from singer_sdk.helpers.util import utc_now
from singer_sdk.helpers.testing import get_basic_tap_test

from {{ cookiecutter.library_name }}.tap import Tap{{ cookiecutter.source_name }}

SAMPLE_CONFIG = {
    "start_date": utc_now()
    # TODO: Initialize minimal tap config and/or register env vars in test harness
}


# Get built-in 'generic' tap tester from SDK:
test_using_generic_test_suite = get_basic_tap_test(Tap{{ cookiecutter.source_name }})


# TODO: Expand additional tests as appropriate for your tap.
def test_catalog_discovery():
    """Test stream catalog discovery."""
    tap = Tap{{ cookiecutter.source_name }}(
        config=SAMPLE_CONFIG, state=None, parse_env_config=True
    )
    catalog_json = tap.run_discovery()
    assert catalog_json
