"""Test class creation."""

from typing import Optional

from singer_sdk.samples.sample_tap_google_analytics.ga_tap import (
    SampleTapGoogleAnalytics,
)


def test_ga_sync_sample(ga_config: Optional[dict]):
    """Test class creation."""
    tap = SampleTapGoogleAnalytics(config=ga_config, parse_env_config=True)
    tap.sync_all()
