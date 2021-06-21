"""Test class creation."""

from singer_sdk.samples.sample_target_csv.csv_target import SampleTargetCSV

SAMPLE_CONFIG = {"target_folder": "./.output"}


def test_target_class():
    """Test class creation."""
    _ = SampleTargetCSV(config=SAMPLE_CONFIG)
