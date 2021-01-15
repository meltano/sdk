"""Sample target test for target-csv."""

from tap_base.target_base import TargetBase
from tap_base import typehelpers as th

from tap_base.samples.sample_target_csv.csv_target_sink import SampleCSVTargetSink


class SampleTargetCSV(TargetBase):
    """Sample target for CSV."""

    name = "target-csv"
    config_jsonschema = th.PropertiesList(
        th.StringType("filepath", optional=True),
        th.StringType("file_naming_scheme", optional=True),
    ).to_dict()
    default_sink_class = SampleCSVTargetSink
