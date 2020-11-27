"""TapBase abstract class."""

import abc
from typing import Any, List, Tuple

from tap_base.PluginBase import PluginBase
from tap_base.TapStreamBase import TapStreamBase


class TapBase(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    _name: str
    _vers: str
    _capabilities: List[str]
    _accepted_options: List[str]
    _option_set_requirements: List[List[str]]
    _config: dict
    _conn: Any = None

    # Constructor

    def __init__(
        self,
        plugin_name: str,
        version: str,
        capabilities: List[str],
        accepted_options: List[str],
        option_set_requirements: List[List[str]],
        config: dict,
        state: dict = None,
    ) -> None:
        """Initialize the tap."""
        self._name = plugin_name
        self._vers = version
        self._capabilities = capabilities
        self._accepted_options = accepted_options
        self._option_set_requirements = option_set_requirements
        self._config = config
        self._conn = self.get_connection()

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

    # Connection management:

    def is_connected(self) -> bool:
        """Return True if connected."""
        return self._conn is not None

    @abc.abstractmethod
    def open_connection(self) -> Any:
        """Initialize the tap connection."""
        pass

    def get_connection(self) -> Any:
        """Get or create tap connection."""
        if self.is_connected():
            return self._conn
        conn = self.open_connection()
        return conn

    # Abstract stream detection methods:

    @abc.abstractmethod
    def get_available_stream_ids(self) -> List[str]:
        """Return a list of all streams (tables)."""
        pass

    @abc.abstractmethod
    def create_stream(self, stream_id: str) -> TapStreamBase:
        """Return a tap stream object."""
        pass
