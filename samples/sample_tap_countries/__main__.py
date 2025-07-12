"""Entry point for the tap."""

from __future__ import annotations

from tap_countries.tap import TapCountries

TapCountries.cli()
