"""Shared parent class for TapBase, TargetBase (future), and TransformBase (future)."""

import abc
import logging
from tap_base.helpers import classproperty
from typing import Dict, List, Optional, Type, Tuple, Any

import click

from tap_base.stream_base import GenericStreamBase


class PluginBase(metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    _config: dict
    _logger: Optional[logging.Logger] = None

    @property
    def logger(self) -> logging.Logger:
        if not self._logger:
            self._logger = logging.getLogger(self.plugin_name)
        return self._logger

    # Constructor

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the tap."""
        self._config = config or {}
        self.validate_config()

    @classproperty
    def plugin_name(cls) -> str:
        """Return the plugin name."""
        return "sample-plugin-name"

    @classproperty
    def stream_class(cls) -> Type[GenericStreamBase]:
        """Return the stream class."""
        return GenericStreamBase

    @property
    def capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        return []

    @abc.abstractproperty
    @classproperty
    def accepted_config_options(cls) -> List[str]:
        return ["start_date"]

    @abc.abstractproperty
    @classproperty
    def required_config_sets(cls) -> List[List[str]]:
        return [[]]

    # Core plugin metadata:

    @classproperty
    def plugin_version(cls) -> str:
        """Return the package version number."""
        try:
            from importlib import metadata
        except ImportError:
            # Running on pre-3.8 Python; use importlib-metadata package
            import importlib_metadata as metadata
        try:
            version = metadata.version(cls.plugin_name)
        except metadata.PackageNotFoundError:
            version = "[could not be detected]"
        return version

    # Core plugin config:

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return self._config.get(config_key, default)

    def validate_config(
        self, raise_errors: bool = True, warnings_as_errors: bool = False
    ) -> Tuple[List[str], List[str]]:
        """Return a tuple: (warnings: List[str], errors: List[str])."""
        warnings: List[str] = []
        errors: List[str] = []
        for k in self._config:
            if k not in self.accepted_config_options:
                warnings.append(f"Unexpected config option found: {k}.")
        if self.required_config_sets:
            required_sets = self.required_config_sets
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
        if raise_errors and errors:
            raise RuntimeError(
                f"One or more errors ocurred during validation: {errors}"
            )
        if warnings_as_errors and raise_errors and warnings:
            raise RuntimeError(
                f"One or more warnings ocurred during validation: {warnings}"
            )
        return warnings, errors

    def print_version(self) -> None:
        """Print help text for the tap."""
        print(f"{self.plugin_name} v{self.plugin_version}")

    @classmethod
    @click.command()
    def cli(cls):
        """Handle command line execution."""
        pass
