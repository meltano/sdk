"""Test sample sync."""

import json
from pathlib import Path

from tap_base.tests.sample_tap_countries.countries_tap import SampleTapCountries

COUNTER = 0


CONFIG_FILE = "tap_base/tests/sample_tap_countries/tests/.secrets/tap-countries.json"
SAMPLE_CONFIG = json.loads(Path(CONFIG_FILE).read_text())
SAMPLE_CONFIG_BAD = {"not": "correct"}

def test_countries_sync_all():
    """Test sync_all() for countries sample."""
    tap = SampleTapCountries(config=SAMPLE_CONFIG)
    tap.sync_all()
