"""Tests discovery features for Parquet."""

from tap_base.tests.sample_tap_gitlab.gitlab_tap import SampleTapGitlab

CONFIG_FILE = "tap_base/tests/sample_tap_gitlab/tests/.secrets/tap-gitlab.json"


def test_gitlab_tap_discovery():
    """Test class creation."""
    tap = SampleTapGitlab(config=CONFIG_FILE, state=None)
    catalog_json = tap.run_discovery()
    assert catalog_json
