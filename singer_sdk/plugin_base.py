"""Shared parent class for Tap, Target (future), and Transform (future)."""

from __future__ import annotations

import abc
import logging
import os
import sys
import time
import typing as t
from pathlib import Path, PurePath
from types import MappingProxyType

import click
from jsonschema import Draft7Validator
from packaging.specifiers import SpecifierSet

from singer_sdk import about, metrics
from singer_sdk.cli import plugin_cli
from singer_sdk.configuration._dict_config import (
    merge_missing_config_jsonschema,
    parse_environment_config,
)
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
CHECK_SUPPORTED_PYTHON_VERSIONS = (
    # unsupported versions
    "2.7",
    "3.0",
    "3.1",
    "3.2",
    "3.3",
    "3.4",
    "3.5",
    "3.6",
    "3.7",
    # current supported versions
    "3.8",
    "3.9",
    "3.10",
    "3.11",
    # future supported versions
    "3.12",
    "3.13",
    "3.14",
    "3.15",
    "3.16",
)


JSONSchemaValidator = extend_validator_with_defaults(Draft7Validator)


class MapperNotInitialized(Exception):
    """Raised when the mapper is not initialized."""

    def __init__(self) -> None:
        """Initialize the exception."""
        super().__init__("Mapper not initialized. Please call setup_mapper() first.")


class PluginBase(metaclass=abc.ABCMeta):
    """Abstract base class for taps."""

    #: The executable name of the tap or target plugin. e.g. tap-foo
    name: str

    #: The package name of the plugin. e.g meltanolabs-tap-foo
    package_name: str | None = None

    config_jsonschema: t.ClassVar[dict] = {}
    # A JSON Schema object defining the config options that this tap will accept.

    _config: dict

    @classproperty
    def logger(cls) -> logging.Logger:  # noqa: N805
        """Get logger.

        Returns:
            Plugin logger.
        """
        # Get the level from <PLUGIN_NAME>_LOGLEVEL or LOGLEVEL environment variables
        plugin_env_prefix = f"{cls.name.upper().replace('-', '_')}_"
        log_level = os.environ.get(f"{plugin_env_prefix}LOGLEVEL") or os.environ.get(
            "LOGLEVEL",
        )

        logger = logging.getLogger(cls.name)

        if log_level is not None and log_level.upper() in logging._levelToName.values():
            logger.setLevel(log_level.upper())

        return logger

    # Constructor

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
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
        elif isinstance(config, (str, PurePath)):
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
            msg = f"Error parsing config of type '{type(config).__name__}'."
            raise ValueError(msg)
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
        self._mapper: PluginMapper | None = None

        metrics._setup_logging(self.config)
        self.metrics_logger = metrics.get_metrics_logger()

        # Initialization timestamp
        self.__initialized_at = int(time.time() * 1000)

    def setup_mapper(self) -> None:
        """Initialize the plugin mapper for this tap."""
        self._mapper = PluginMapper(
            plugin_config=dict(self.config),
            logger=self.logger,
        )

    @property
    def mapper(self) -> PluginMapper:
        """Plugin mapper for this tap.

        Returns:
            A PluginMapper object.

        Raises:
            MapperNotInitialized: If the mapper has not been initialized.
        """
        if self._mapper is None:
            raise MapperNotInitialized
        return self._mapper

    @mapper.setter
    def mapper(self, mapper: PluginMapper) -> None:
        """Set the plugin mapper for this plugin.

        Args:
            mapper: A PluginMapper object.
        """
        self._mapper = mapper

    @property
    def initialized_at(self) -> int:
        """Start time of the plugin.

        Returns:
            The start time of the plugin.
        """
        return self.__initialized_at

    @classproperty
    def capabilities(self) -> list[CapabilitiesEnum]:
        """Get capabilities.

        Developers may override this property in oder to add or remove
        advertised capabilities for this plugin.

        Returns:
            A list of plugin capabilities.
        """
        return [
            PluginCapabilities.STREAM_MAPS,
            PluginCapabilities.FLATTENING,
            PluginCapabilities.BATCH,
        ]

    @classproperty
    def _env_var_config(cls) -> dict[str, t.Any]:  # noqa: N805
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

    @staticmethod
    def _get_package_version(package: str) -> str:
        """Return the package version number.

        Args:
            package: The package name.

        Returns:
            The package version number.
        """
        try:
            version = metadata.version(package)
        except metadata.PackageNotFoundError:
            version = "[could not be detected]"
        return version

    @staticmethod
    def _get_supported_python_versions(package: str) -> list[str] | None:
        """Return the supported Python versions.

        Args:
            package: The package name.

        Returns:
            A list of supported Python versions.
        """
        try:
            package_metadata = metadata.metadata(package)
        except metadata.PackageNotFoundError:
            return None

        reported_python_versions = SpecifierSet(package_metadata["Requires-Python"])
        return [
            version
            for version in CHECK_SUPPORTED_PYTHON_VERSIONS
            if version in reported_python_versions
        ]

    @classmethod
    def get_plugin_version(cls) -> str:
        """Return the package version number.

        Returns:
            The package version number.
        """
        return cls._get_package_version(cls.package_name or cls.name)

    @classmethod
    def get_sdk_version(cls) -> str:
        """Return the package version number.

        Returns:
            The package version number.
        """
        return cls._get_package_version(SDK_PACKAGE_NAME)

    @classmethod
    def get_supported_python_versions(cls) -> list[str] | None:
        """Return the supported Python versions.

        Returns:
            A list of supported Python versions.
        """
        return cls._get_supported_python_versions(cls.package_name or cls.name)

    @classproperty
    def plugin_version(cls) -> str:  # noqa: N805
        """Get version.

        Returns:
            The package version number.
        """
        return cls.get_plugin_version()

    @classproperty
    def sdk_version(cls) -> str:  # noqa: N805
        """Return the package version number.

        Returns:
            Meltano Singer SDK version number.
        """
        return cls.get_sdk_version()

    # Abstract methods:

    @property
    def state(self) -> dict:
        """Get state.

        Raises:
            NotImplementedError: If the derived plugin doesn't override this method.
        """
        raise NotImplementedError

    # Core plugin config:

    @property
    def config(self) -> t.Mapping[str, t.Any]:
        """Get config.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        return t.cast(dict, MappingProxyType(self._config))

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
        self,
        *,
        raise_errors: bool = True,
        warnings_as_errors: bool = False,
    ) -> tuple[list[str], list[str]]:
        """Validate configuration input against the plugin configuration JSON schema.

        Args:
            raise_errors: Flag to throw an exception if any validation errors are found.
            warnings_as_errors: Flag to throw an exception if any warnings were emitted.

        Returns:
            A tuple of configuration validation warnings and errors.

        Raises:
            ConfigValidationError: If raise_errors is True and validation fails.
        """
        warnings: list[str] = []
        errors: list[str] = []
        log_fn = self.logger.info
        config_jsonschema = self.config_jsonschema

        if config_jsonschema:
            self.append_builtin_config(config_jsonschema)
            self.logger.debug(
                "Validating config using jsonschema: %s",
                config_jsonschema,
            )
            validator = JSONSchemaValidator(config_jsonschema)
            errors = [e.message for e in validator.iter_errors(self._config)]

        if errors:
            summary = (
                f"Config validation failed: {'; '.join(errors)}\n"
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
            msg = f"One or more warnings ocurred during validation: {warnings}"
            raise ConfigValidationError(msg)
        log_fn(summary)
        return warnings, errors

    @classmethod
    def print_version(
        cls: type[PluginBase],
        print_fn: t.Callable[[t.Any], None] = print,
    ) -> None:
        """Print help text for the tap.

        Args:
            print_fn: A function to use to display the plugin version.
                Defaults to `print`_.

        .. _print: https://docs.python.org/3/library/functions.html#print
        """
        print_fn(f"{cls.name} v{cls.plugin_version}, Meltano SDK v{cls.sdk_version}")

    @classmethod
    def _get_about_info(cls: type[PluginBase]) -> about.AboutInfo:
        """Returns capabilities and other tap metadata.

        Returns:
            A dictionary containing the relevant 'about' information.
        """
        config_jsonschema = cls.config_jsonschema
        cls.append_builtin_config(config_jsonschema)

        return about.AboutInfo(
            name=cls.name,
            description=cls.__doc__,
            version=cls.get_plugin_version(),
            sdk_version=cls.get_sdk_version(),
            supported_python_versions=cls.get_supported_python_versions(),
            capabilities=cls.capabilities,
            settings=config_jsonschema,
        )

    @classmethod
    def append_builtin_config(cls: type[PluginBase], config_jsonschema: dict) -> None:
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
        capabilities = cls.capabilities
        if PluginCapabilities.STREAM_MAPS in capabilities:
            merge_missing_config_jsonschema(STREAM_MAPS_CONFIG, config_jsonschema)

        if PluginCapabilities.FLATTENING in capabilities:
            merge_missing_config_jsonschema(FLATTENING_CONFIG, config_jsonschema)

    @classmethod
    def print_about(
        cls: type[PluginBase],
        output_format: str | None = None,
    ) -> None:
        """Print capabilities and other tap metadata.

        Args:
            output_format: Render option for the plugin information.
        """
        info = cls._get_about_info()
        formatter = about.AboutFormatter.get_formatter(output_format or "text")
        print(formatter.format_about(info))  # noqa: T201

    @staticmethod
    def config_from_cli_args(*args: str) -> tuple[list[Path], bool]:
        """Parse CLI arguments into a config dictionary.

        Args:
            args: CLI arguments.

        Raises:
            FileNotFoundError: If the config file does not exist.

        Returns:
            A tuple containing the config dictionary and a boolean indicating whether
            the config file was found.
        """
        config_files = []
        parse_env_config = False

        for config_path in args:
            if config_path == "ENV":
                # Allow parse from env vars:
                parse_env_config = True
                continue

            # Validate config file paths before adding to list
            if not Path(config_path).is_file():
                msg = (
                    f"Could not locate config file at '{config_path}'.Please check "
                    "that the file exists."
                )
                raise FileNotFoundError(msg)

            config_files.append(Path(config_path))

        return config_files, parse_env_config

    @classmethod
    def invoke(
        cls,
        *,
        about: bool = False,
        about_format: str | None = None,
        **kwargs: t.Any,  # noqa: ARG003
    ) -> None:
        """Invoke the plugin.

        Args:
            about: Display package metadata and settings.
            about_format: Specify output style for `--about`.
            kwargs: Plugin keyword arguments.
        """
        if about:
            cls.print_about(about_format)
            sys.exit(0)

    @classmethod
    def cb_version(
        cls: type[PluginBase],
        ctx: click.Context,
        param: click.Option,  # noqa: ARG003
        value: bool,  # noqa: FBT001
    ) -> None:
        """CLI callback to print the plugin version and exit.

        Args:
            ctx: Click context.
            param: Click parameter.
            value: Boolean indicating whether to print the version.
        """
        if not value:
            return
        cls.print_version(print_fn=click.echo)
        ctx.exit()

    @classmethod
    def get_singer_command(cls: type[PluginBase]) -> click.Command:
        """Handle command line execution.

        Returns:
            A callable CLI object.
        """
        return click.Command(
            name=cls.name,
            callback=cls.invoke,
            context_settings={"help_option_names": ["--help"]},
            params=[
                click.Option(
                    ["--version"],
                    is_flag=True,
                    help="Display the package version.",
                    is_eager=True,
                    expose_value=False,
                    callback=cls.cb_version,
                ),
                click.Option(
                    ["--about"],
                    help="Display package metadata and settings.",
                    is_flag=True,
                    is_eager=False,
                    expose_value=True,
                ),
                click.Option(
                    ["--format", "about_format"],
                    help="Specify output style for --about",
                    type=click.Choice(
                        ["json", "markdown"],
                        case_sensitive=False,
                    ),
                    default=None,
                ),
                click.Option(
                    ["--config"],
                    multiple=True,
                    help=(
                        "Configuration file location or 'ENV' to use environment "
                        "variables."
                    ),
                    type=click.STRING,
                    default=(),
                    is_eager=True,
                ),
            ],
        )

    @plugin_cli
    def cli(cls) -> click.Command:
        """Handle command line execution.

        Returns:
            A callable CLI object.
        """
        return cls.get_singer_command()
