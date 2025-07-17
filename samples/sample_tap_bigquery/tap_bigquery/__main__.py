"""Entry point for the tap."""

from __future__ import annotations

from tap_bigquery.tap import TapBigQuery

TapBigQuery.cli()
