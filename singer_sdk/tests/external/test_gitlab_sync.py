"""Test sample sync."""
from typing import Optional
from singer_sdk.helpers import _catalog
from singer_sdk.samples.sample_tap_gitlab.gitlab_tap import SampleTapGitlab

COUNTER = 0
SAMPLE_CONFIG_BAD = {"not": "correct"}


def test_gitlab_sync_all(gitlab_config: Optional[dict]):
    """Test sync_all() for gitlab sample."""
    tap = SampleTapGitlab(config=gitlab_config, parse_env_config=True)
    tap.sync_all()


def test_gitlab_sync_epic_issues(gitlab_config: Optional[dict]):
    """Test sync for just the 'epic_issues' child stream."""
    # Initialize with basic config
    stream_name = "epic_issues"
    tap1 = SampleTapGitlab(config=gitlab_config, parse_env_config=True)
    # Test discovery
    tap1.run_discovery()
    catalog1 = tap1.catalog_dict
    # Reset and re-initialize with an input catalog
    _catalog.deselect_all_streams(catalog=catalog1)
    _catalog.set_catalog_stream_selected(
        catalog=catalog1, stream_name=stream_name, selected=True
    )
    tap1 = None
    tap2 = SampleTapGitlab(
        config=gitlab_config, parse_env_config=True, catalog=catalog1
    )
    tap2.sync_all()
