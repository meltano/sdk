"""TapBase abstract class."""

import abc
import json
from typing import List, Type
from pathlib import Path

from singer import Catalog

from tap_base.plugin_base import PluginBase
from tap_base.tap_stream_base import TapStreamBase
from tap_base.connection_base import GenericConnectionBase


class TapBase(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    _streams: List[TapStreamBase]

    # Constructor

    def __init__(
        self,
        plugin_name: str,
        version: str,
        capabilities: List[str],
        accepted_options: List[str],
        option_set_requirements: List[List[str]],
        connection_class: Type[GenericConnectionBase],
        config: dict,
        state: dict = None,
    ) -> None:
        """Initialize the tap."""
        super().__init__(
            plugin_name=plugin_name,
            version=version,
            capabilities=capabilities,
            accepted_options=accepted_options,
            option_set_requirements=option_set_requirements,
            connection_class=connection_class,
            config=config,
        )

    # Abstract stream detection methods:

    @abc.abstractmethod
    def create_stream(self, stream_id: str) -> TapStreamBase:
        """Return a tap stream object."""
        pass

    def get_catalog(self) -> Catalog:
        """Return a catalog object."""
        return

    def write_catalog_file(self, outfile: str) -> str:
        """Write out catalog file."""
        Path(outfile).write_text(json.dumps(self.get_catalog().to_dict()))
        return outfile

    # Standard CLI Functions:
    def handle_cli_args(self, args, cwd, environ) -> None:
        """Take necessary action in response to a CLI command."""
        pass
