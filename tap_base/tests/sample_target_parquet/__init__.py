"""Module test for target-parquet functionality."""


# Reuse the tap connection rather than create a new target connection:
from tap_base.tests.sample_target_parquet.parquet_target import SampleTargetParquet

__all__ = [
    "SampleTargetParquet",
    "SampleTargetParquetStream",
    "SampleParquetConnection",
]
