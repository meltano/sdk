"""A sample implementation for BigQuery."""

from __future__ import annotations

from .tap import BigQueryConnector, BigQueryStream, TapBigQuery

__all__ = ["BigQueryConnector", "BigQueryStream", "TapBigQuery"]
