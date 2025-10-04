from __future__ import annotations

from tap_gitlab.tap import TapGitlab

from singer_sdk.helpers import _catalog
from singer_sdk.singerlib import Catalog
from singer_sdk.testing import get_tap_test_class

TestSampleTapGitlab = get_tap_test_class(TapGitlab, validate_config=False)


def test_gitlab_replication_keys():
    stream_name = "issues"
    expected_replication_key = "updated_at"
    tap = TapGitlab(state=None, parse_env_config=True)

    catalog = tap._singer_catalog
    catalog_entry = catalog.get_stream(stream_name)
    assert catalog_entry is not None

    metadata_root = catalog_entry.metadata.root
    assert metadata_root is not None

    assert metadata_root.valid_replication_keys is not None
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


def test_gitlab_sync_epic_issues():
    """Test sync for just the 'epic_issues' child stream."""
    # Initialize with basic config
    stream_name = "epic_issues"
    tap1 = TapGitlab(parse_env_config=True)
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
    tap2 = TapGitlab(parse_env_config=True, catalog=catalog1.to_dict())
    tap2.sync_all()
