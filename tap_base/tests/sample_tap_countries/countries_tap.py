"""Sample tap test for tap-countries.

This uses a free "Countries API" which does not require authentication.

See the online explorer and query builder here:
  - https://countries.trevorblades.com/
"""

from typing import List

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_countries.countries_streams import (
    CountriesStream,
    ContinentsStream,
)


class SampleTapCountries(TapBase):
    """Sample tap for Countries GraphQL API."""

    name: str = "sample-tap-countries"
    accepted_config_keys: List[str] = []
    required_config_options = None

    def discover_catalog_streams(self) -> None:
        """Initialize self._streams with a dictionary of all streams."""
        self.logger.info("Loading streams types...")
        self._streams = {
            "countries": CountriesStream(config=self._config, state=self._state,),
            "continents": ContinentsStream(config=self._config, state=self._state,),
        }


# CLI Execution:

cli = SampleTapCountries.build_cli()
