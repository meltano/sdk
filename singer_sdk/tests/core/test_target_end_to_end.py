"""Test tap-to-target sync."""

import io
from contextlib import redirect_stdout

from typing import Dict, Any

from singer_sdk.samples.sample_tap_countries.countries_tap import SampleTapCountries
from singer_sdk.samples.sample_target_csv.csv_target import SampleTargetCSV

SAMPLE_FILENAME = "/tmp/testfile.countries"
SAMPLE_TAP_CONFIG: Dict[str, Any] = {}
SAMPLE_TARGET_CSV_CONFIG = {"target_folder": "./.output"}


def sync_end_to_end(tap, target):
    """Test and end-to-end sink from the tap to the target."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()
    buf.seek(0)
    target._process_lines(buf)


def test_countries_to_csv():
    tap = SampleTapCountries(config=SAMPLE_TAP_CONFIG, state=None)
    target = SampleTargetCSV(config=SAMPLE_TARGET_CSV_CONFIG)
    sync_end_to_end(tap, target)
