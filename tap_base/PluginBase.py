"""Shared parent class for TapBase, TargetBase, and TransformBase"""

import abc
from typing import List


class PluginBase(metaclass=abc.ABCMeta):
    """Abstract base class for taps"""

    # Core plugin metadata:

    @abc.abstractmethod
    def get_plugin_name(self) -> str:
        """Return the plugin name."""
        pass

    @abc.abstractmethod
    def get_plugin_version(self) -> str:
        """Return the plugin version string."""
        pass

    @abc.abstractmethod
    def get_capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        pass

    # Standard CLI Functions:

    def print_help(self) -> None:
        """Print help text for the tap."""
        self.print_version()
        print(self.get_usage_str())

    def print_version(self) -> None:
        """Print help text for the tap."""
        print(self.get_usage_str())

    def get_usage_str(self) -> str:
        """Get usage string (used in --help)."""
        capabilities = self.get_capabilities()
        plugin = self.get_plugin_name()
        result = "\n".join([f"Usage for {plugin}:", ""])
        if "sync" in capabilities:
            result += f"  {plugin} sync [--config CONFIG]"
            if "state" in capabilities:
                result += " [--state STATE]"
            if "catalog" in capabilities:
                result += " [--catalog CATALOG]"
            result += "\n"
        return result

    @abc.abstractmethod
    def handle_cli_args(self, args, cwd, environ) -> None:
        """Take necessary action in response to a CLI command."""
        pass
