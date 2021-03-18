"""Tests init and discovery features for {{ cookiecutter.tap_id }}."""

from singer_sdk.samples.sample_tap_parquet.parquet_tap import SampleTapParquet
from singer_sdk.helpers.util import utc_now

SAMPLE_CONFIG = {
    "start_date": utc_now()
    # TODO: Initialize minimal tap config and/or register env vars in test harness
}

# TODO: Expand tests as appropriate for your tap.


def test_catalog_discovery():
    """Test stream catalog discovery."""
    tap = SampleTapParquet(config=SAMPLE_CONFIG, state=None, parse_env_config=True)
    catalog_json = tap.run_discovery()
    assert catalog_json
