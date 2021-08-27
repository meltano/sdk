"""Run the generic tests from `singer_sdk.testing`."""

from pathlib import Path
from typing import Optional

from singer_sdk.testing import get_standard_tap_tests
from singer_sdk.samples.sample_tap_gitlab.gitlab_tap import SampleTapGitlab
from singer_sdk.samples.sample_tap_google_analytics.ga_tap import (
    SampleTapGoogleAnalytics,
)

GA_CONFIG_FILE = Path("singer_sdk/tests/external/.secrets/google-analytics-config.json")


def test_gitlab_tap_standard_tests(gitlab_config: Optional[dict]):
    """Run standard tap tests against Gitlab tap."""
    tests = get_standard_tap_tests(SampleTapGitlab, config=gitlab_config)
    for test in tests:
        test()


def test_ga_tap_standard_tests(ga_config: Optional[dict]):
    """Run standard tap tests against Google Analytics tap."""
    tests = get_standard_tap_tests(SampleTapGoogleAnalytics, config=ga_config)
    for test in tests:
        test()
