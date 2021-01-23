"""Test class creation."""


from singer_sdk.samples.sample_tap_google_analytics.ga_tap import (
    SampleTapGoogleAnalytics,
)

CONFIG_FILE = "singer_sdk/tests/.secrets/google-analytics-config.json"


def test_ga_sync_sample():
    """Test class creation."""
    tap = SampleTapGoogleAnalytics(config=CONFIG_FILE)
    tap.sync_all()
