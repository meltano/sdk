"""Module test for target-parquet functionality."""


# Reuse the tap connection rather than create a new target connection:
from singer_sdk.samples.sample_target_parquet.parquet_target import SampleTargetParquet
from singer_sdk.samples.sample_target_parquet.parquet_target_sink import (
    SampleParquetTargetSink,
)

__all__ = [
    "SampleTargetParquet",
    "SampleParquetTargetSink",
]
