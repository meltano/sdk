"""Test class creation."""

from singer_sdk.samples.sample_target_csv.csv_target import SampleTargetCSV


def test_target_class(csv_config: dict):
    """Test class creation."""
    _ = SampleTargetCSV(config=csv_config)
