"""Entry point for the tap."""

from __future__ import annotations

from tap_csv.tap import TapCSV

TapCSV.cli()
