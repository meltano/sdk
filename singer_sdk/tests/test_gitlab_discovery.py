"""Tests discovery features for Parquet."""

import json
from pathlib import Path
from typing import Optional

from singer_sdk.samples.sample_tap_gitlab.gitlab_tap import SampleTapGitlab

CONFIG_FILE = "singer_sdk/tests/.secrets/gitlab-config.json"


def test_gitlab_tap_discovery():
    """Test class creation."""
    config: Optional[dict] = None
    if Path(CONFIG_FILE).exists():
        config = json.loads(Path(CONFIG_FILE).read_text())
    tap = SampleTapGitlab(config=config, state=None)
    catalog_json = tap.run_discovery()
    assert catalog_json
