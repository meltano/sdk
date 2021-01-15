"""Test class creation."""


from tap_base.samples.sample_tap_google_analytics.ga_tap import SampleTapGoogleAnalytics

CONFIG_FILE = "tap_base/tests/.secrets/google-analytics-config.json"


def test_ga_sync_sample():
    """Test class creation."""
    tap = SampleTapGoogleAnalytics(config=CONFIG_FILE)
    tap.sync_all()
