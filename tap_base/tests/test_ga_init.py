"""Test class creation."""


from tap_base.samples.sample_tap_google_analytics.ga_tap import SampleTapGoogleAnalytics

CONFIG_FILE = "tap_base/tests/.secrets/google-analytics-config.json"


def test_tap_class():
    """Test class creation."""
    _ = SampleTapGoogleAnalytics(config=CONFIG_FILE)
