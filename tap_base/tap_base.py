"""TapBase abstract class."""

import abc
import json
from logging import Logger

from singer.catalog import Catalog
from tap_base.helpers import classproperty

from typing import Any, List, Optional, Type, Dict
from pathlib import Path

import click

from tap_base.plugin_base import PluginBase
from tap_base.tap_stream_base import TapStreamBase


class TapBase(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    _streams: Dict[str, TapStreamBase] = {}

    # Constructor

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        catalog: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the tap."""
        self._state = state or {}
        super().__init__(config=config)
        if catalog:
            self.load_catalog_streams(
                catalog=catalog,
                config=self._config,
                state=self._state,
                logger=self.logger,
            )
        else:
            self.discover_catalog_streams(
                config=self._config, state=self._state, logger=self.logger
            )

    @classproperty
    def stream_class(cls) -> Type[TapStreamBase]:
        """Return the stream class."""
        return TapStreamBase

    @property
    def streams(self) -> Dict[str, TapStreamBase]:
        return self._streams

    @property
    def capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        result = ["sync", "catalog", "state"]
        if self.stream_class.discoverable:
            result.append("discover")
        return result

    # Abstract stream detection methods:

    def load_catalog_streams(
        self, catalog: dict, state: dict, config: dict, logger: Logger
    ) -> None:
        streams: List[Dict] = catalog["streams"]
        for stream in streams:
            stream_name = stream["tap_stream_id"]
            new_stream = self.stream_class.from_stream_dict(
                stream_dict=stream, state=state, config=config, logger=logger
            )
            self._streams[stream_name] = new_stream

    def discover_catalog_streams(
        self, state: dict, config: dict, logger: Logger
    ) -> None:
        raise NotImplementedError(
            f"Tap '{self.plugin_name}' does not support discovery. "
            "Please set the '--catalog' command line argument and try again."
        )

    def run_discovery(self) -> str:
        """Write the catalog json to STDOUT and return the same as a string."""
        catalog_json = self.get_catalog_json()
        print(catalog_json)
        return catalog_json

    def get_singer_catalog(self) -> Catalog:
        """Return a Catalog object."""
        catalog_entries = [
            stream.singer_catalog_entry for stream in self.streams.values()
        ]
        return Catalog(catalog_entries)

    def get_catalog_json(self) -> str:
        return json.dumps(self.get_singer_catalog().to_dict(), indent=2)

    # Sync methods

    def sync_one(self, tap_stream_id: str):
        """Sync a single stream."""
        stream = self.streams[tap_stream_id]
        stream.sync()

    def sync_all(self):
        """Sync all streams."""
        for stream in self.streams.values():
            stream.sync()

    # Command Line Execution

    @classmethod
    def cli(
        cls,
        discover: bool = False,
        config: str = None,
        state: str = None,
        catalog: str = None,
    ):
        """Handle command line execution."""

        def read_optional_json(path: Optional[str]) -> Optional[Dict[str, Any]]:
            if not path:
                return None
            return json.loads(Path(path).read_text())

        config_dict = read_optional_json(config)
        state_dict = read_optional_json(state)
        catalog_dict = read_optional_json(catalog)
        tap = cls(config=config_dict, state=state_dict, catalog=catalog_dict)
        if discover:
            tap.run_discovery()
        else:
            tap.sync_all()


@click.option("--discover", is_flag=True)
@click.option("--config")
@click.option("--catalog")
@click.command()
def cli(discover: bool = False, config: str = None, catalog: str = None):
    TapBase.cli(discover=discover, config=config, catalog=catalog)
