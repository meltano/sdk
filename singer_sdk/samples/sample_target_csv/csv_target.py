"""Sample target test for target-csv."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from singer_sdk.samples.sample_target_csv.csv_target_sink import SampleCSVTargetSink


class SampleTargetCSV(Target):
    """Sample target for CSV."""

    name = "target-csv"
    config_jsonschema = th.PropertiesList(
        th.Property("target_folder", th.StringType, required=True),
        th.Property("file_naming_scheme", th.StringType),
    ).to_dict()
    default_sink_class = SampleCSVTargetSink
