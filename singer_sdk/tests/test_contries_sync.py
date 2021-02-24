"""Test sample sync."""

import json
from pathlib import Path

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries


SAMPLE_CONFIG_BAD = {"not": "correct"}


def test_countries_sync_all():
    """Test sync_all() for countries sample."""
    tap = SampleTapCountries(config=None)
    tap.sync_all()
