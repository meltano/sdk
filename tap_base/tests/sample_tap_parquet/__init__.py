"""Module test for tap-parquet functionality."""

from tap_base.tests.sample_tap_parquet.tap import SampleTapParquet
from tap_base.tests.sample_tap_parquet.tap_stream import SampleTapParquetStream

__all__ = [
    "SampleTapParquet",
    "SampleTapParquetStream",
]
