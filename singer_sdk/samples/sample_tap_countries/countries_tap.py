"""Sample tap test for tap-countries.

This uses a free "Countries API" which does not require authentication.

See the online explorer and query builder here:
  - https://countries.trevorblades.com/
"""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.samples.sample_tap_countries.countries_streams import (
    CountriesStream,
    ContinentsStream,
)

PLUGIN_NAME = "sample-tap-countries"


class SampleTapCountries(Tap):
    """Sample tap for Countries GraphQL API."""

    name: str = PLUGIN_NAME
    accepted_config_keys: List[str] = []
    required_config_options = None

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [
            CountriesStream(tap=self),
            ContinentsStream(tap=self),
        ]


# CLI Execution:

cli = SampleTapCountries.cli
