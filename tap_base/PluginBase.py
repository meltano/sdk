"""Shared parent class for TapBase, TargetBase, and TransformBase"""

import abc
from typing import List

class PluginBase(metaclass=abc.ABCMeta):
    """Abstract base class for taps"""

    # Core plugin metadata:

    @abc.abstractmethod
    def get_plugin_name(self) -> str:
        """Returns the plugin name"""
        pass

    @abc.abstractmethod
    def get_capabilities() -> List[str]:
        """Return a list of supported capabilities."""
        pass

    # CLI Functions:

    def print_help(self) -> None:
        print(self.get_usage_str())

    def get_usage_str(self) -> str:
        """Get usage string (used in --help)"""
        result = "\n".join([
            f"Usage for {plugin}:", ""
        ])
        capabilities = self.get_capabilities()
        plugin_name = self.get_plugin_name()
        if "sync" in capabilities:
            result += f"  {plugin} sync [--config CONFIG] [--state STATE]"
            if "state" in capabilities:
                 result += "[--state STATE]"
            if "catalog" in capabilities:
                 result += "[--catalog CATALOG]"
            result += "\n"
        return result

    @abc.abstractmethod
    def handle_cli_args(args, cwd, environ) -> None:
        """Take necessary action in response to a CLI command."""
        pass

