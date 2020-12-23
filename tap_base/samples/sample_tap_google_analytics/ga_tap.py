"""Sample tap test for tap-google-analytics."""

import json
from pathlib import Path
from typing import List

from singer.schema import Schema

from tap_base.tap_base import TapBase
from tap_base.samples.sample_tap_google_analytics.ga_tap_stream import (
    GASimpleSampleStream,
    SampleGoogleAnalyticsStream,
)

ACCEPTED_CONFIG_OPTIONS = ["client_id", "client_secret"]
REPORT_DEFS_FILE = "tap_base/samples/sample_tap_google_analytics/resources/default_report_definitions.json"
REQUIRED_CONFIG_SETS = None

REPORT_DEFS = json.loads(Path(REPORT_DEFS_FILE).read_text())


class SampleTapGoogleAnalytics(TapBase):
    """Sample tap for GoogleAnalytics."""

    name: str = "sample-tap-google_analytics"
    accepted_config_keys = ACCEPTED_CONFIG_OPTIONS
    required_config_options = REQUIRED_CONFIG_SETS
    default_stream_class = SampleGoogleAnalyticsStream

    def discover_streams(self) -> List[SampleGoogleAnalyticsStream]:
        """Return a list of all streams."""
        return [GASimpleSampleStream(config=self._config, state=self._state)]


cli = SampleTapGoogleAnalytics.build_cli()
