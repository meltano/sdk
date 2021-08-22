"""Test sample sync."""

import json
from pathlib import Path
from typing import Optional

from singer_sdk.helpers import _catalog

from singer_sdk.samples.sample_tap_gitlab.gitlab_tap import SampleTapGitlab

COUNTER = 0


CONFIG_FILE = "singer_sdk/tests/external/.secrets/gitlab-config.json"
SAMPLE_CONFIG_BAD = {"not": "correct"}

config: Optional[dict] = None
if Path(CONFIG_FILE).exists():
    config = json.loads(Path(CONFIG_FILE).read_text())


def test_gitlab_sync_all():
    """Test sync_all() for gitlab sample."""
    tap = SampleTapGitlab(config=config, parse_env_config=True)
    tap.sync_all()


def test_gitlab_sync_epic_issues():
    """Test sync for just the 'epic_issues' child stream."""
    # Initialize with basic config
    stream_name = "epic_issues"
    tap1 = SampleTapGitlab(config=config, parse_env_config=True)
    # Test discovery
    tap1.run_discovery()
    catalog1 = tap1.catalog_dict
    # Reset and re-initialize with an input catalog
    _catalog.deselect_all_streams(catalog=catalog1)
    _catalog.set_catalog_stream_selected(
        catalog=catalog1, stream_name=stream_name, selected=True
    )
    tap1 = None
    tap2 = SampleTapGitlab(config=config, parse_env_config=True, catalog=catalog1)
    tap2.sync_all()
