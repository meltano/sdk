"""Sample target test for target-parquet."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_parquet.sink import ParquetSink


class TargetParquet(Target):
    """Sample target for Parquet."""

    name = "target-parquet"
    config_jsonschema = th.PropertiesList(
        th.Property("filepath", th.StringType, title="Output File Path"),
        th.Property("file_naming_scheme", th.StringType, title="File Naming Scheme"),
    ).to_dict()
    default_sink_class = ParquetSink


if __name__ == "__main__":
    TargetParquet.cli()
