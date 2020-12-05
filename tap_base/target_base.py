"""TargetBase abstract class."""

import abc

from typing import List, Type

from tap_base.plugin_base import PluginBase
from tap_base.connection_base import GenericConnectionBase


class TargetBase(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for targets."""

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

    def handle_cli_args(self, args, cwd, environ) -> None:
        """Take necessary action in response to a CLI command."""
        pass
