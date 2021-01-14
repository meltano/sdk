"""Shared parent class for Tap, Target (future), and Transform (future)."""

import abc
from functools import lru_cache
import json
import logging
import os
from copy import deepcopy
from types import MappingProxyType
from jsonschema import ValidationError, SchemaError
from jsonschema import Draft4Validator as JSONSchemaValidator
from pathlib import Path, PurePath

from tap_base.helpers import classproperty, is_common_secret_key, SecretString
from typing import Dict, List, Mapping, Optional, Tuple, Any, Union

import click


class PluginBase(metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    name: str = "sample-plugin-name"
    accepted_config_keys: List[str] = []
    protected_config_keys: List[str] = []
    required_config_options: Optional[List[List[str]]] = [[]]
    config_jsonschema: Optional[dict] = None

    _config: dict

    @classproperty
    def logger(self) -> logging.Logger:
        """Get logger."""
        return logging.getLogger(self.name)

    # Constructor

    def __init__(self, config: Union[PurePath, str, dict, None] = None) -> None:
        """Initialize the tap."""
        if not config:
            config_dict = {}
        elif isinstance(config, str) or isinstance(config, PurePath):
            config_dict = (
                self.read_optional_json_file(str(config), warn_missing=True) or {}
            )
        else:
            config_dict = config
        config_dict.update(self.get_env_var_config())
        for k, v in config_dict.items():
            if self.is_secret_config(k):
                config_dict[k] = SecretString(v)
        self._config = config_dict
        self.validate_config()

    @property
    def capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        return []

    # Read input files and parse env vars:

    @classmethod
    def read_optional_json_file(
        cls, path: Optional[Union[PurePath, str]], warn_missing: bool = False
    ) -> Optional[Dict[str, Any]]:
        """If json filepath is specified, read it from disk."""
        if not path:
            return None
        if Path(path).exists():
            return json.loads(Path(path).read_text())
        elif warn_missing:
            cls.logger.warning(f"File at '{path}' was not found.")
            return None
        else:
            raise FileExistsError(f"File at '{path}' was not found.")

    @classmethod
    def get_env_var_config(cls) -> Dict[str, Any]:
        """Return any config specified in environment variables.

        Variables must match the convention "PLUGIN_NAME_setting_name",
        with dashes converted to underscores, the plugin name converted to all
        caps, and the setting name in same-case as specified in settings config.
        """
        result: Dict[str, Any] = {}
        for k, v in os.environ.items():
            for key in cls.accepted_config_keys:
                if k == f"{cls.name.upper()}_{key}".replace("-", "_"):
                    cls.logger.info(f"Parsing '{key}' config from env variable '{k}'.")
                    result[key] = v
        return result

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
            version = metadata.version(cls.name)
        except metadata.PackageNotFoundError:
            version = "[could not be detected]"
        return version

    # Abstract methods:

    @property
    def state(self) -> dict:
        """Return the state dict for the plugin."""
        raise NotImplementedError()

    # Core plugin config:

    @property
    @lru_cache()
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return MappingProxyType(self._config)

    def is_secret_config(self, config_key: str) -> bool:
        """Return true if a config value should be treated as a secret.

        This avoids accidental printing to logs, and it prevents rendering the secrets
        in jinja templating functions.
        """
        return (
            is_common_secret_key(config_key) or config_key in self.protected_config_keys
        )

    def validate_config(
        self, raise_errors: bool = True, warnings_as_errors: bool = False
    ) -> Tuple[List[str], List[str]]:
        """Return a tuple: (warnings: List[str], errors: List[str])."""
        warnings: List[str] = []
        errors: List[str] = []
        for k in self.config:
            if k not in self.accepted_config_keys:
                warnings.append(f"Unexpected config option found: {k}.")
        if self.required_config_options:
            required_set_options = self.required_config_options
            matched_any = False
            missing: List[List[str]] = []
            for required_set in required_set_options:
                if all([x in self.config.keys() for x in required_set]):
                    matched_any = True
                else:
                    missing.append(
                        [x for x in required_set if x not in self.config.keys()]
                    )
            if not matched_any:
                errors.append(
                    "One or more required config options are missing. "
                    "Please complete one or more of the following sets: "
                    f"{str(missing)}"
                )
        if self.config_jsonschema:
            try:
                validator = JSONSchemaValidator(self.config_jsonschema)
                validator.validate(dict(self.config))
            except (ValidationError, SchemaError) as ex:
                errors.append(str(ex))
        if raise_errors and errors:
            raise RuntimeError(
                f"Config validation failed: {f'; '.join(errors)}\n"
                f"JSONSchema was: {self.config_jsonschema}"
            )
        if warnings_as_errors and raise_errors and warnings:
            raise RuntimeError(
                f"One or more warnings ocurred during validation: {warnings}"
            )
        return warnings, errors

    @classmethod
    def print_version(cls) -> None:
        """Print help text for the tap."""
        print(f"{cls.name} v{cls.plugin_version}")

    @classmethod
    @click.command()
    def cli(cls):
        """Handle command line execution."""
        pass
