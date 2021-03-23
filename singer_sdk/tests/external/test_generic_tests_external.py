"""Test the generic tests from `singer_sdk.helpers.testing`."""

from pathlib import Path

from singer_sdk.helpers.testing import get_basic_tap_test
from singer_sdk.samples.sample_tap_gitlab.gitlab_tap import SampleTapGitlab
from singer_sdk.samples.sample_tap_google_analytics.ga_tap import (
    SampleTapGoogleAnalytics,
)

GA_CONFIG_FILE = Path("singer_sdk/tests/external/.secrets/google-analytics-config.json")
GITLAB_CONFIG_FILE = Path("singer_sdk/tests/external/.secrets/gitlab-config.json")

test_tap_gitlab_basic_test = get_basic_tap_test(
    SampleTapGitlab,
    tap_config=GITLAB_CONFIG_FILE if GITLAB_CONFIG_FILE.exists() else None,
)
test_tap_ga_basic_test = get_basic_tap_test(
    SampleTapGoogleAnalytics,
    tap_config=GA_CONFIG_FILE if GA_CONFIG_FILE.exists() else None,
)
