"""Tap, target and stream test fixtures."""

import pytest


@pytest.fixture
def csv_config(outdir: str) -> dict:
    """Get configuration dictionary for target-csv."""
    return {"target_folder": outdir}
