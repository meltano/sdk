"""Main package module for target-parquet functionality."""

from __future__ import annotations

from target_parquet.sink import ParquetSink
from target_parquet.target import TargetParquet

__all__ = [
    "ParquetSink",
    "TargetParquet",
]
