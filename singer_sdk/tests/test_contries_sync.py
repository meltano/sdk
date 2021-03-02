"""Test sample sync."""

import json
from pathlib import Path

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries


SAMPLE_CONFIG_BAD = {"not": "correct"}


def test_countries_sync_all():
    """Test sync_all() for countries sample."""
    tap = SampleTapCountries(config=None)
    tap.sync_all()

def test_countries_primary_key():
    tap = SampleTapCountries(config=None)
    catalog = json.loads(tap.get_catalog_json())
    catalog_entries = catalog["streams"]
    for countries_entry in [c for c in catalog_entries if c["stream"] == "countries"]:
        metadata_root = [md for md in countries_entry["metadata"] if md["breadcrumb"] == []][0]
        key_props_1 = metadata_root["metadata"].get("table-key-properties")
        key_props_2 = countries_entry.get("key_properties")
        assert key_props_1 == ["code"], (
            f"Incorrect 'table-key-properties' in catalog: ({key_props_1})\n\nRoot metadata was: {metadata_root}\n\nCatalog entry was: {countries_entry}"
            )
        assert key_props_2 == ["code"], (
            f"Incorrect 'key_properties' in catalog: ({key_props_2})\n\nCatalog entry was: {countries_entry}"
            )
