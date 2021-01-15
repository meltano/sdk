"""Module test for tap-parquet functionality."""

from tap_base.samples.sample_tap_parquet.parquet_tap import SampleTapParquet
from tap_base.samples.sample_tap_parquet.parquet_tap_stream import (
    SampleTapParquetStream,
)

__all__ = [
    "SampleTapParquet",
    "SampleTapParquetStream",
]
