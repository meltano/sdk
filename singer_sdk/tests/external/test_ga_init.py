"""Test class creation."""

from typing import Optional

from singer_sdk.samples.sample_tap_google_analytics.ga_tap import (
    SampleTapGoogleAnalytics,
)

CONFIG_FILE = "singer_sdk/tests/external/.secrets/google-analytics-config.json"


def test_tap_class(ga_config: Optional[dict]):
    """Test class creation."""
    _ = SampleTapGoogleAnalytics(config=ga_config, parse_env_config=True)
