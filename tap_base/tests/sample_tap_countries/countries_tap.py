"""Sample tap test for tap-countries.

This uses a free "Countries API" which does not require authentication.

See the online explorer and query builder here:
  - https://countries.trevorblades.com/
"""

from logging import Logger
from typing import List

import click

from tap_base.helpers import classproperty
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
            "countries": CountriesStream(
                config=self._config, state=self._state,
            ),
            "continents": ContinentsStream(
                config=self._config, state=self._state,
            ),
        }


# CLI Execution:


@click.option("--version", is_flag=True)
@click.option("--discover", is_flag=True)
@click.option("--config")
@click.option("--catalog")
@click.command()
def cli(
    discover: bool = False,
    config: str = None,
    catalog: str = None,
    version: bool = False,
):
    SampleTapCountries.cli(
        version=version, discover=discover, config=config, catalog=catalog
    )
