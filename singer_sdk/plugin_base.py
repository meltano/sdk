"""Shared parent class for Tap, Target (future), and Transform (future)."""

import abc
import json
import logging
import os
from collections import OrderedDict
from pathlib import PurePath
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import click
from jsonschema import Draft7Validator, SchemaError, ValidationError

from singer_sdk import metrics
from singer_sdk.configuration._dict_config import parse_environment_config
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import metadata
from singer_sdk.helpers._secrets import SecretString, is_common_secret_key
from singer_sdk.helpers._util import read_json_file
from singer_sdk.helpers.capabilities import (
    FLATTENING_CONFIG,
    STREAM_MAPS_CONFIG,
    CapabilitiesEnum,
    PluginCapabilities,
)
from singer_sdk.mapper import PluginMapper
from singer_sdk.typing import extend_validator_with_defaults

SDK_PACKAGE_NAME = "singer_sdk"


JSONSchemaValidator = extend_validator_with_defaults(Draft7Validator)


class PluginBase(metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    name: str  # The executable name of the tap or target plugin.

    config_jsonschema: dict = {}
    # A JSON Schema object defining the config options that this tap will accept.

    _config: dict

    @classproperty
    def logger(cls) -> logging.Logger:
        """Get logger.

        Returns:
            Plugin logger.
        """
        # Get the level from <PLUGIN_NAME>_LOGLEVEL or LOGLEVEL environment variables
        LOGLEVEL = (
            os.environ.get(f"{cls.name.upper()}_LOGLEVEL")
            or os.environ.get("LOGLEVEL")
            or "INFO"
        ).upper()

        assert (
            LOGLEVEL in logging._levelToName.values()
        ), f"Invalid LOGLEVEL configuration: {LOGLEVEL}"
        logger = logging.getLogger(cls.name)
        logger.setLevel(LOGLEVEL)
        return logger

    # Constructor

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Create the tap or target.

        Args:
            config: May be one or more paths, either as str or PurePath objects, or
                it can be a predetermined config dict.
            parse_env_config: True to parse settings from env vars.
            validate_config: True to require validation of config settings.

        Raises:
            ValueError: If config is not a dict or path string.
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
        self._validate_config(raise_errors=validate_config)
        self.mapper: PluginMapper

        metrics._setup_logging(self.config)
        self.metrics_logger = metrics.get_metrics_logger()

    @classproperty
    def capabilities(self) -> List[CapabilitiesEnum]:
        """Get capabilities.

        Developers may override this property in oder to add or remove
        advertised capabilities for this plugin.

        Returns:
            A list of plugin capabilities.
        """
        return [
            PluginCapabilities.STREAM_MAPS,
            PluginCapabilities.FLATTENING,
        ]

    @classproperty
    def _env_var_config(cls) -> Dict[str, Any]:
        """Return any config specified in environment variables.

        Variables must match the convention "<PLUGIN_NAME>_<SETTING_NAME>",
        all uppercase with dashes converted to underscores.

        Returns:
            Dictionary of configuration parsed from the environment.
        """
        plugin_env_prefix = f"{cls.name.upper().replace('-', '_')}_"
        config_jsonschema = cls.config_jsonschema
        cls.append_builtin_config(config_jsonschema)

        return parse_environment_config(config_jsonschema, plugin_env_prefix)

    # Core plugin metadata:

    @classproperty
    def plugin_version(cls) -> str:
        """Get version.

        Returns:
            The package version number.
        """
        try:
            version = metadata.version(cls.name)
        except metadata.PackageNotFoundError:
            version = "[could not be detected]"
        return version

    @classproperty
    def sdk_version(cls) -> str:
        """Return the package version number.

        Returns:
            Meltano Singer SDK version number.
        """
        try:
            version = metadata.version(SDK_PACKAGE_NAME)
        except metadata.PackageNotFoundError:
            version = "[could not be detected]"
        return version

    # Abstract methods:

    @property
    def state(self) -> dict:
        """Get state.

        Raises:
            NotImplementedError: If the derived plugin doesn't override this method.
        """
        raise NotImplementedError()

    # Core plugin config:

    @property
    def config(self) -> Mapping[str, Any]:
        """Get config.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        return cast(Dict, MappingProxyType(self._config))

    @staticmethod
    def _is_secret_config(config_key: str) -> bool:
        """Check if config key is secret.

        This prevents accidental printing to logs.

        Args:
            config_key: Configuration key name to match against common secret names.

        Returns:
            True if a config value should be treated as a secret.
        """
        return is_common_secret_key(config_key)

    def _validate_config(
        self, raise_errors: bool = True, warnings_as_errors: bool = False
    ) -> Tuple[List[str], List[str]]:
        """Validate configuration input against the plugin configuration JSON schema.

        Args:
            raise_errors: Flag to throw an exception if any validation errors are found.
            warnings_as_errors: Flag to throw an exception if any warnings were emitted.

        Returns:
            A tuple of configuration validation warnings and errors.

        Raises:
            ConfigValidationError: If raise_errors is True and validation fails.
        """
        warnings: List[str] = []
        errors: List[str] = []
        log_fn = self.logger.info
        config_jsonschema = self.config_jsonschema
        if config_jsonschema:
            self.append_builtin_config(config_jsonschema)
            try:
                self.logger.debug(
                    f"Validating config using jsonschema: {config_jsonschema}"
                )
                validator = JSONSchemaValidator(config_jsonschema)
                validator.validate(self._config)
            except (ValidationError, SchemaError) as ex:
                errors.append(str(ex.message))
        if errors:
            summary = (
                f"Config validation failed: {f'; '.join(errors)}\n"
                f"JSONSchema was: {config_jsonschema}"
            )
            if raise_errors:
                raise ConfigValidationError(summary)

            log_fn = self.logger.warning
        else:
            summary = f"Config validation passed with {len(warnings)} warnings."
            for warning in warnings:
                summary += f"\n{warning}"
        if warnings_as_errors and raise_errors and warnings:
            raise ConfigValidationError(
                f"One or more warnings ocurred during validation: {warnings}"
            )
        log_fn(summary)
        return warnings, errors

    @classmethod
    def print_version(
        cls: Type["PluginBase"],
        print_fn: Callable[[Any], None] = print,
    ) -> None:
        """Print help text for the tap.

        Args:
            print_fn: A function to use to display the plugin version.
                Defaults to :function:`print`.
        """
        print_fn(f"{cls.name} v{cls.plugin_version}, Meltano SDK v{cls.sdk_version}")

    @classmethod
    def _get_about_info(cls: Type["PluginBase"]) -> Dict[str, Any]:
        """Returns capabilities and other tap metadata.

        Returns:
            A dictionary containing the relevant 'about' information.
        """
        info: Dict[str, Any] = OrderedDict({})
        info["name"] = cls.name
        info["description"] = cls.__doc__
        info["version"] = cls.plugin_version
        info["sdk_version"] = cls.sdk_version
        info["capabilities"] = cls.capabilities

        config_jsonschema = cls.config_jsonschema
        cls.append_builtin_config(config_jsonschema)
        info["settings"] = config_jsonschema
        return info

    @classmethod
    def append_builtin_config(cls: Type["PluginBase"], config_jsonschema: dict) -> None:
        """Appends built-in config to `config_jsonschema` if not already set.

        To customize or disable this behavior, developers may either override this class
        method or override the `capabilities` property to disabled any unwanted
        built-in capabilities.

        For all except very advanced use cases, we recommend leaving these
        implementations "as-is", since this provides the most choice to users and is
        the most "future proof" in terms of taking advantage of built-in capabilities
        which may be added in the future.

        Args:
            config_jsonschema: [description]
        """

        def _merge_missing(source_jsonschema: dict, target_jsonschema: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in source_jsonschema["properties"].items():
                if k not in target_jsonschema["properties"]:
                    target_jsonschema["properties"][k] = v

        capabilities = cls.capabilities
        if PluginCapabilities.STREAM_MAPS in capabilities:
            _merge_missing(STREAM_MAPS_CONFIG, config_jsonschema)

        if PluginCapabilities.FLATTENING in capabilities:
            _merge_missing(FLATTENING_CONFIG, config_jsonschema)

    @classmethod
    def print_about(cls: Type["PluginBase"], format: Optional[str] = None) -> None:
        """Print capabilities and other tap metadata.

        Args:
            format: Render option for the plugin information.
        """
        info = cls._get_about_info()

        if format == "json":
            print(json.dumps(info, indent=2, default=str))

        elif format == "markdown":
            max_setting_len = cast(
                int, max(len(k) for k in info["settings"]["properties"].keys())
            )

            # Set table base for markdown
            table_base = (
                f"| {'Setting':{max_setting_len}}| Required | Default | Description |\n"
                f"|:{'-' * max_setting_len}|:--------:|:-------:|:------------|\n"
            )

            # Empty list for string parts
            md_list = []
            # Get required settings for table
            required_settings = info["settings"].get("required", [])

            # Iterate over Dict to set md
            md_list.append(
                f"# `{info['name']}`\n\n"
                f"{info['description']}\n\n"
                f"Built with the [Meltano Singer SDK](https://sdk.meltano.com).\n\n"
            )
            for key, value in info.items():

                if key == "capabilities":
                    capabilities = f"## {key.title()}\n\n"
                    capabilities += "\n".join([f"* `{v}`" for v in value])
                    capabilities += "\n\n"
                    md_list.append(capabilities)

                if key == "settings":
                    setting = f"## {key.title()}\n\n"
                    for k, v in info["settings"].get("properties", {}).items():
                        md_description = v.get("description", "").replace("\n", "<BR/>")
                        table_base += (
                            f"| {k}{' ' * (max_setting_len - len(k))}"
                            f"| {'True' if k in required_settings else 'False':8} | "
                            f"{v.get('default', 'None'):7} | "
                            f"{md_description:11} |\n"
                        )
                    setting += table_base
                    setting += (
                        "\n"
                        + "\n".join(
                            [
                                "A full list of supported settings and capabilities "
                                f"is available by running: `{info['name']} --about`"
                            ]
                        )
                        + "\n"
                    )
                    md_list.append(setting)

            print("".join(md_list))
        else:
            formatted = "\n".join([f"{k.title()}: {v}" for k, v in info.items()])
            print(formatted)

    @classproperty
    def cli(cls) -> Callable:
        """Handle command line execution.

        Returns:
            A callable CLI object.
        """

        @click.command()
        def cli() -> None:
            pass

        return cli
