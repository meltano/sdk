"""Sample tap stream test for tap-google-analytics."""

from typing import Iterable

from tap_base.streams.core import TapStreamBase

PLUGIN_NAME = "sample-tap-google-analytics"


class SampleTapGoogleAnalyticsStream(TapStreamBase):
    """Sample tap test for google-analytics."""

    tap_name = PLUGIN_NAME

    def get_row_generator(self) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        for row in zip(*batch.columns):
            yield row
