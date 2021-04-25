"""Test class creation."""

import json
from pathlib import Path
from typing import Optional

from singer_sdk.samples.sample_tap_google_analytics.ga_tap import (
    SampleTapGoogleAnalytics,
)

CONFIG_FILE = "singer_sdk/tests/external/.secrets/google-analytics-config.json"


def test_ga_sync_sample():
    """Test class creation."""
    config: Optional[dict] = None
    if Path(CONFIG_FILE).exists():
        config = json.loads(Path(CONFIG_FILE).read_text())
    tap = SampleTapGoogleAnalytics(config=config, parse_env_config=True)
    tap.sync_all()
