"""{{ cookiecutter.source_name }} tap class."""

from pathlib import Path
from typing import List

import click

from tap_base import TapBase
from {{ cookiecutter.library_name }}.stream import Tap{{ cookiecutter.source_name }}Stream


PLUGIN_NAME = "{{ cookiecutter.tap_id }}"


class Tap{{ cookiecutter.source_name }}(TapBase):
    """{{ cookiecutter.source_name }} tap class."""

    name = "{{ cookiecutter.tap_id }}"
    accepted_config_keys: ACCEPTED_CONFIG_KEYS
    required_config_options = REQUIRED_CONFIG_OPTIONS

    def discover_catalog_streams(self) -> None:
        """Initialize self._streams with a dictionary of all streams."""
        self.logger.info("Loading streams types...")
        self._streams = {
            "stream_a": Tap{{ cookiecutter.source_name }}Stream(
                config=self._config, state=self._state,
            ),
            # TODO: Add any additional streams here:
            # "stream_b": Tap{{ cookiecutter.source_name }}Stream(
            #     config=self._config, state=self._state,
            # ),
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
    """Handle CLI Execution."""
    Tap{{ cookiecutter.source_name }}.cli(
        version=version, discover=discover, config=config, catalog=catalog
    )
