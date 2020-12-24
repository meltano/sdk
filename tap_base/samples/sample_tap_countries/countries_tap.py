"""Sample tap test for tap-countries.

This uses a free "Countries API" which does not require authentication.

See the online explorer and query builder here:
  - https://countries.trevorblades.com/
"""

from typing import List

from tap_base import TapBase, TapStreamBase
from tap_base.samples.sample_tap_countries.countries_streams import (
    CountriesStream,
    ContinentsStream,
)
from tap_base.samples.sample_tap_countries.countries_globals import PLUGIN_NAME


class SampleTapCountries(TapBase):
    """Sample tap for Countries GraphQL API."""

    name: str = PLUGIN_NAME
    accepted_config_keys: List[str] = []
    required_config_options = None

    def discover_streams(self) -> List[TapStreamBase]:
        """Return a list of discovered streams."""
        return [
            CountriesStream(config=self._config, state=self._state),
            ContinentsStream(config=self._config, state=self._state),
        ]


# CLI Execution:

cli = SampleTapCountries.build_cli()
