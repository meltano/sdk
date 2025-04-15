"""Sample target test for target-csv."""

from __future__ import annotations

from samples.sample_target_csv.csv_target_sink import SampleCSVTargetSink
from singer_sdk import typing as th
from singer_sdk.target_base import Target


class SampleTargetCSV(Target):
    """Sample target for CSV."""

    name = "target-csv"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "target_folder", th.StringType, required=True, title="Target Folder"
        ),
        th.Property("file_naming_scheme", th.StringType, title="File Naming Scheme"),
    ).to_dict()
    default_sink_class = SampleCSVTargetSink
