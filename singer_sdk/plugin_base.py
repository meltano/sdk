"""Shared parent class for Tap, Target (future), and Transform (future)."""

import abc
from collections import OrderedDict
import json
import logging
import os
from singer_sdk.mapper import PluginMapper
from types import MappingProxyType
from typing import Dict, List, Mapping, Optional, Tuple, Any, Union, cast


from jsonschema import (
    ValidationError,
    SchemaError,
    Draft4Validator,
)
from pathlib import PurePath

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import metadata
from singer_sdk.helpers._util import read_json_file
from singer_sdk.helpers._secrets import is_common_secret_key, SecretString
from singer_sdk.helpers._typing import is_string_array_type
from singer_sdk.typing import extend_validator_with_defaults

import click

SDK_PACKAGE_NAME = "singer_sdk"


JSONSchemaValidator = extend_validator_with_defaults(Draft4Validator)


class PluginBase(metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    name: str  # The executable name of the tap or target plugin.

    config_jsonschema: dict = {}
    # A JSON Schema object defining the config options that this tap will accept.

    _config: dict

    @classproperty
    def logger(cls) -> logging.Logger:
        """Get logger."""
        return logging.getLogger(cls.name)

    # Constructor

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
    ) -> None:
        """Initialize the tap or target.

        - `config` may be one or more paths, either as str or PurePath objects, or
        it can be a predetermined config dict.
        - `parse_env_config` - True to parse settings from env vars.
        """
        if not config:
            config_dict = {}
        elif isinstance(config, str) or isinstance(config, PurePath):
            config_dict = read_json_file(config)
        elif isinstance(config, list):
            config_dict = {}
            for config_path in config:
                # Read each config file sequentially. Settings from files later in the
                # list will override those of earlier ones.
                config_dict.update(read_json_file(config_path))
        elif isinstance(config, dict):
            config_dict = config
        else:
            raise ValueError(f"Error parsing config of type '{type(config).__name__}'.")
        if parse_env_config:
            self.logger.info("Parsing env var for settings config...")
            config_dict.update(self._env_var_config)
        else:
            self.logger.info("Skipping parse of env var settings...")
        for k, v in config_dict.items():
            if self._is_secret_config(k):
                config_dict[k] = SecretString(v)
        self._config = config_dict
        self._validate_config()
        self.mapper: PluginMapper

    @classproperty
    def capabilities(self) -> List[str]:
        """Return a list of supported capabilities."""
        return []

    @classproperty
    def _env_var_config(cls) -> Dict[str, Any]:
        """Return any config specified in environment variables.

        Variables must match the convention "<PLUGIN_NAME>_<SETTING_NAME>",
        all uppercase with dashes converted to underscores.
        """
        result: Dict[str, Any] = {}
        plugin_env_prefix = f"{cls.name.upper().replace('-', '_')}_"
        for config_key in cls.config_jsonschema["properties"].keys():
            env_var_name = plugin_env_prefix + config_key.upper().replace("-", "_")
            if env_var_name in os.environ:
                env_var_value = os.environ[env_var_name]
                cls.logger.info(
                    f"Parsing '{config_key}' config from env variable '{env_var_name}'."
                )
                if is_string_array_type(
                    cls.config_jsonschema["properties"][config_key]
                ):
                    if env_var_value[0] == "[" and env_var_value[-1] == "]":
                        raise ValueError(
                            "A bracketed list was detected in the environment variable "
                            f"'{env_var_name}'. This syntax is no longer supported. "
                            "Please remove the brackets and try again."
                        )
                    result[config_key] = env_var_value.split(",")
                else:
                    result[config_key] = env_var_value
        return result

    # Core plugin metadata:

    @classproperty
    def plugin_version(cls) -> str:
        """Return the package version number."""
        try:
            version = metadata.version(cls.name)
        except metadata.PackageNotFoundError:
            version = "[could not be detected]"
        return version

    @classproperty
    def sdk_version(cls) -> str:
        """Return the package version number."""
        try:
            version = metadata.version(SDK_PACKAGE_NAME)
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
    def config(self) -> Mapping[str, Any]:
        """Return a frozen (read-only) config dictionary map."""
        return cast(Dict, MappingProxyType(self._config))

    @staticmethod
    def _is_secret_config(config_key: str) -> bool:
        """Return true if a config value should be treated as a secret.

        This prevents accidental printing to logs.
        """
        return is_common_secret_key(config_key)

    def _validate_config(
        self, raise_errors: bool = True, warnings_as_errors: bool = False
    ) -> Tuple[List[str], List[str]]:
        """Return a tuple: (warnings: List[str], errors: List[str])."""
        warnings: List[str] = []
        errors: List[str] = []
        if self.config_jsonschema:
            try:
                self.logger.debug(
                    f"Validating config using jsonschema: {self.config_jsonschema}"
                )
                validator = JSONSchemaValidator(self.config_jsonschema)
                validator.validate(self._config)
            except (ValidationError, SchemaError) as ex:
                errors.append(str(ex))
        if errors:
            summary = (
                f"Config validation failed: {f'; '.join(errors)}\n"
                f"JSONSchema was: {self.config_jsonschema}"
            )
            if raise_errors:
                raise RuntimeError(summary)
        else:
            summary = (
                f"Config validation passed with 0 errors and {len(warnings)} warnings."
            )
            for warning in warnings:
                summary += f"\n{warning}"
        if warnings_as_errors and raise_errors and warnings:
            raise RuntimeError(
                f"One or more warnings ocurred during validation: {warnings}"
            )
        self.logger.info(summary)
        return warnings, errors

    @classmethod
    def print_version(cls, print_fn=print) -> None:
        """Print help text for the tap."""
        print_fn(f"{cls.name} v{cls.plugin_version}, Meltano SDK v{cls.sdk_version})")

    @classmethod
    def print_about(cls, format: Optional[str] = None) -> None:
        """Print capabilities and other tap metadata."""
        info: Dict[str, Any] = OrderedDict({})
        info["name"] = cls.name
        info["version"] = cls.plugin_version
        info["sdk_version"] = cls.sdk_version
        info["capabilities"] = cls.capabilities
        info["settings"] = cls.config_jsonschema
        if format == "json":
            print(json.dumps(info, indent=2))
        else:
            formatted = "\n".join([f"{k.title()}: {v}" for k, v in info.items()])
            print(formatted)

    @classmethod
    @click.command()
    def cli(cls):
        """Handle command line execution."""
        pass
