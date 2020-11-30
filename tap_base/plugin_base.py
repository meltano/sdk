"""Shared parent class for TapBase, TargetBase (future), and TransformBase (future)."""

import abc
from typing import List, Type, Tuple, Any

from tap_base.connection_base import GenericConnectionBase


class PluginBase(metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    _name: str
    _vers: str
    _capabilities: List[str]
    _accepted_options: List[str]
    _option_set_requirements: List[List[str]]
    _config: dict
    _conn_class: Type[GenericConnectionBase]
    _conn: GenericConnectionBase

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
        self._name = plugin_name
        self._vers = version
        self._capabilities = capabilities
        self._accepted_options = accepted_options
        self._option_set_requirements = option_set_requirements
        self._config = config
        self._conn_class = connection_class
        self._conn = None
        self._conn = self.get_connection()

    # Core plugin metadata:

    def get_plugin_name(self) -> str:
        """Return the plugin name."""
        return self._name

    def get_plugin_version(self) -> str:
        """Return the plugin version string."""
        return self._vers

    def get_capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        return self._capabilities

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return self._config.get(config_key, default)

    # Core plugin metadata:

    def validate_config(self) -> Tuple[List[str], List[str]]:
        """Return a tuple: (warnings: List[str], errors: List[str])."""
        warnings: List[str] = []
        errors: List[str] = []
        for k in self._config:
            if k not in self._accepted_options:
                warnings.append(f"Unexpected config option found: {k}.")
        if self._option_set_requirements:
            required_sets = self._option_set_requirements
            matched_any = False
            missing: List[List[str]] = []
            for required_set in required_sets:
                if all([x in self._config.keys() for x in required_set]):
                    matched_any = True
                else:
                    missing.append(
                        [x for x in required_set if x not in self._config.keys()]
                    )
            if not matched_any:
                errors.append(
                    "One or more required config options are missing. "
                    "Please complete one or more of the following sets: "
                    f"{str(missing)}"
                )
        return warnings, errors

    # Connection management:

    def get_connection(self) -> GenericConnectionBase:
        """Get or create tap connection."""
        if not self._conn:
            self._conn = self._conn_class(config=self._config)
        return self._conn

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
