"""Tests discovery features for Parquet."""

import json
from pathlib import Path

from tap_base.tests.sample_tap_gitlab.gitlab_tap import SampleTapGitlab

CONFIG_FILE = "tap_base/tests/sample_tap_gitlab/tests/.secrets/tap-gitlab.json"
SAMPLE_CONFIG = json.loads(Path(CONFIG_FILE).read_text())


def test_gitlab_tap_discovery():
    """Test class creation."""
    tap = SampleTapGitlab(config=SAMPLE_CONFIG, state=None)
    catalog_json = tap.run_discovery()
    assert catalog_json
