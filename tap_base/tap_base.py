"""Tap abstract class."""

import abc
import json
from pathlib import PurePath
from tap_base.helpers import classproperty
from typing import List, Optional, Type, Dict, Union

import click
from singer.catalog import Catalog

from tap_base.plugin_base import PluginBase
from tap_base.streams.core import Stream


class Tap(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    default_stream_class: Optional[Type[Stream]] = None

    # Constructor

    def __init__(
        self,
        config: Union[PurePath, str, dict, None] = None,
        catalog: Union[PurePath, str, dict, None] = None,
        state: Union[PurePath, str, dict, None] = None,
    ) -> None:
        """Initialize the tap."""
        if isinstance(state, dict):
            state_dict = state
        else:
            state_dict = self.read_optional_json_file(state) or {}
        self._custom_catalog: Optional[dict] = None
        if isinstance(catalog, dict):
            self._custom_catalog = catalog
        elif catalog is not None:
            self._custom_catalog = self.read_optional_json_file(catalog)
        self._state = state_dict or {}
        self._streams: Optional[Dict[str, Stream]] = None
        super().__init__(config=config)

    # Class properties

    @property
    def streams(self) -> Dict[str, Stream]:
        """Return a list of streams, using discovery or a provided catalog.

        Results will be cached after first execution.
        """
        if self._streams is None:
            self._streams = {}
            for stream in self.discover_streams():
                self._streams[stream.name] = stream
            if self._custom_catalog:
                self.apply_catalog(self._custom_catalog)
        return self._streams

    @property
    def capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        return ["sync", "catalog", "state", "discover"]

    # Stream detection:

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

    def sync_one(self, stream_name: str):
        """Sync a single stream."""
        if stream_name not in self.streams:
            raise ValueError(
                f"Could not find stream '{stream_name}' in streams list: "
                f"{sorted(self.streams.keys())}"
            )
        stream = self.streams[stream_name]
        stream.sync()

    def sync_all(self):
        """Sync all streams."""
        for stream in self.streams.values():
            stream.sync()

    # Command Line Execution

    @classproperty
    def cli(cls):
        @click.option("--version", is_flag=True)
        @click.option("--discover", is_flag=True)
        @click.option("--config")
        @click.option("--catalog")
        @click.command()
        def cli(
            version: bool = False,
            discover: bool = False,
            config: str = None,
            state: str = None,
            catalog: str = None,
        ):
            """Handle command line execution."""
            if version:
                cls.print_version()
                return
            tap = cls(config=config, state=state, catalog=catalog)
            if discover:
                tap.run_discovery()
            else:
                tap.sync_all()

        return cli

    # Abstract stream detection methods:

    def apply_catalog(self, catalog: dict) -> None:
        """Update internal streams using provided catalog."""
        for stream in self.streams.values():
            stream.apply_catalog(catalog)

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        raise NotImplementedError(
            f"Tap '{self.name}' does not support discovery. "
            "Please set the '--catalog' command line argument and try again."
        )


cli = Tap.cli
