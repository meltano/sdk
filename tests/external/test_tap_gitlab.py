from __future__ import annotations

import warnings

from samples.sample_tap_gitlab.gitlab_tap import SampleTapGitlab
from singer_sdk._singerlib import Catalog
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers import _catalog
from singer_sdk.testing import get_tap_test_class

from .conftest import gitlab_config

try:
    config = gitlab_config()
    TestSampleTapGitlab = get_tap_test_class(
        tap_class=SampleTapGitlab,
        config=config,
        parse_env_config=True,
    )
except ConfigValidationError as e:
    warnings.warn(
        UserWarning(
            "Could not configure external gitlab tests. "
            f"Config in CI is expected via env vars.\n{e}",
        ),
        stacklevel=2,
    )

COUNTER = 0
SAMPLE_CONFIG_BAD = {"not": "correct"}


def test_gitlab_replication_keys(gitlab_config: dict | None):
    stream_name = "issues"
    expected_replication_key = "updated_at"
    tap = SampleTapGitlab(config=gitlab_config, state=None, parse_env_config=True)

    catalog = tap._singer_catalog
    catalog_entry = catalog.get_stream(stream_name)
    metadata_root = catalog_entry.metadata.root

    key_props_1 = metadata_root.valid_replication_keys[0]
    key_props_2 = catalog_entry.replication_key
    assert key_props_1 == expected_replication_key, (
        f"Incorrect 'valid-replication-keys' in catalog: ({key_props_1})\n\n"
        f"Root metadata was: {metadata_root}\n\nCatalog entry was: {catalog_entry}"
    )
    assert key_props_2 == expected_replication_key, (
        f"Incorrect 'replication_key' in catalog: ({key_props_2})\n\n"
        f"Catalog entry was: {catalog_entry}"
    )
    assert tap.streams[stream_name].is_timestamp_replication_key, (
        "Failed to detect `is_timestamp_replication_key`"
    )

    assert tap.streams["commits"].is_timestamp_replication_key, (
        "Failed to detect `is_timestamp_replication_key`"
    )


def test_gitlab_sync_epic_issues(gitlab_config: dict | None):
    """Test sync for just the 'epic_issues' child stream."""
    # Initialize with basic config
    stream_name = "epic_issues"
    tap1 = SampleTapGitlab(config=gitlab_config, parse_env_config=True)
    # Test discovery
    tap1.run_discovery()
    catalog1 = Catalog.from_dict(tap1.catalog_dict)
    # Reset and re-initialize with an input catalog
    _catalog.deselect_all_streams(catalog=catalog1)
    _catalog.set_catalog_stream_selected(
        catalog=catalog1,
        stream_name=stream_name,
        selected=True,
    )
    tap1 = None
    tap2 = SampleTapGitlab(
        config=gitlab_config,
        parse_env_config=True,
        catalog=catalog1.to_dict(),
    )
    tap2.sync_all()
