"""Module test for tap-parquet functionality."""

from singer_sdk.samples.sample_tap_parquet.parquet_tap import SampleTapParquet
from singer_sdk.samples.sample_tap_parquet.parquet_tap_stream import (
    SampleTapParquetStream,
)

__all__ = [
    "SampleTapParquet",
    "SampleTapParquetStream",
]
