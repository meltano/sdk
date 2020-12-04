"""Module test for tap-parquet functionality."""

from tap_base.tests.sample_tap_parquet.tap import SampleTapParquet
from tap_base.tests.sample_tap_parquet.stream import SampleTapParquetStream
from tap_base.tests.sample_tap_parquet.connection import SampleParquetConnection

__all__ = [
    "SampleTapParquet",
    "SampleTapParquetStream",
    "SampleParquetConnection",
]
