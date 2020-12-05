"""Module test for target-parquet functionality."""


# Reuse the tap connection rather than create a new target connection:
from tap_base.tests.sample_tap_parquet.connection import SampleParquetConnection

from tap_base.tests.sample_target_parquet.target import SampleTargetParquet

__all__ = [
    "SampleTargetParquet",
    "SampleTargetParquetStream",
    "SampleParquetConnection",
]
