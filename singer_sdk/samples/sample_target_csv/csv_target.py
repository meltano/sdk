"""Sample target test for target-csv."""

from singer_sdk.target_base import TargetBase
from singer_sdk import typehelpers as th

from singer_sdk.samples.sample_target_csv.csv_target_sink import SampleCSVTargetSink


class SampleTargetCSV(TargetBase):
    """Sample target for CSV."""

    name = "target-csv"
    config_jsonschema = th.PropertiesList(
        th.StringType("target_folder", optional=True),
        th.StringType("file_naming_scheme", optional=True),
    ).to_dict()
    default_sink_class = SampleCSVTargetSink
