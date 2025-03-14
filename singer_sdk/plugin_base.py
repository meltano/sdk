"""Shared parent class for Tap, Target (future), and Transform (future)."""

from __future__ import annotations

import abc
import logging
import os
import sys
import time
import typing as t
import warnings
from importlib import metadata
from pathlib import Path, PurePath
from types import MappingProxyType

import click

from singer_sdk import about, metrics
from singer_sdk.cli import plugin_cli
from singer_sdk.configuration._dict_config import (
    merge_missing_config_jsonschema,
    parse_environment_config,
)
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.helpers._secrets import SecretString, is_common_secret_key
from singer_sdk.helpers._util import read_json_file
from singer_sdk.helpers.capabilities import (
    FLATTENING_CONFIG,
    STREAM_MAPS_CONFIG,
    CapabilitiesEnum,
    PluginCapabilities,
)
from singer_sdk.io_base import SingerMessageType, SingerReader, SingerWriter
from singer_sdk.mapper import PluginMapper
from singer_sdk.typing import (
    DEFAULT_JSONSCHEMA_VALIDATOR,
    extend_validator_with_defaults,
)

if sys.version_info >= (3, 11):
    _LOG_LEVELS_MAPPING = logging.getLevelNamesMapping()
else:
    _LOG_LEVELS_MAPPING = logging._nameToLevel  # noqa: SLF001

if t.TYPE_CHECKING:
    from jsonschema import ValidationError

    from singer_sdk.singerlib.encoding.base import (
        GenericSingerReader,
        GenericSingerWriter,
    )

SDK_PACKAGE_NAME = "singer_sdk"

JSONSchemaValidator = extend_validator_with_defaults(DEFAULT_JSONSCHEMA_VALIDATOR)


class MapperNotInitialized(Exception):
    """Raised when the mapper is not initialized."""

    def __init__(self) -> None:
        """Initialize the exception."""
        super().__init__("Mapper not initialized. Please call setup_mapper() first.")


class SingerCommand(click.Command):
    """Custom click command class for Singer packages."""

    def __init__(
        self,
        *args: t.Any,
        logger: logging.Logger,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the command.

        Args:
            *args: Positional `click.Command` arguments.
            logger: A logger instance.
            **kwargs: Keyword `click.Command` arguments.
        """
        super().__init__(*args, **kwargs)
        self.logger = logger

    def invoke(self, ctx: click.Context) -> t.Any:  # noqa: ANN401
        """Invoke the command, capturing warnings and logging them.

        Args:
            ctx: The `click` context.

        Returns:
            The result of the command invocation.
        """
        logging.captureWarnings(capture=True)
        try:
            return super().invoke(ctx)
        except ConfigValidationError as exc:
            for error in exc.errors:
                self.logger.error("Config validation error: %s", error)  # noqa: TRY400
            sys.exit(1)


def _format_validation_error(error: ValidationError) -> str:
    """Format a JSON Schema validation error.

    Args:
        error: A JSON Schema validation error.

    Returns:
        A formatted error message.
    """
    result = f"{error.message}"

    if error.path:
        result += f" in config[{']['.join(repr(index) for index in error.path)}]"

    return result


class PluginBase(metaclass=abc.ABCMeta):  # noqa: PLR0904
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

        if log_level is not None and log_level.upper() in _LOG_LEVELS_MAPPING:
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
            TypeError: If config is not a dict or path string.
        """
        config = config or {}
        if isinstance(config, (str, PurePath)):
            config_dict = read_json_file(config)
            warnings.warn(
                "Passing a config file path is deprecated. Please pass the config "
                "as a dictionary instead.",
                SingerSDKDeprecationWarning,
                stacklevel=2,
            )
        elif isinstance(config, list):
            config_dict = {}
            for config_path in config:
                # Read each config file sequentially. Settings from files later in the
                # list will override those of earlier ones.
                config_dict.update(read_json_file(config_path))
            warnings.warn(
                "Passing a list of config file paths is deprecated. Please pass the "
                "config as a dictionary instead.",
                SingerSDKDeprecationWarning,
                stacklevel=2,
            )
        elif isinstance(config, dict):
            config_dict = config
        else:
            msg = f"Error parsing config of type '{type(config).__name__}'."  # type: ignore[unreachable]
            raise TypeError(msg)
        if parse_env_config:
            self.logger.info("Parsing env var for settings config...")
            config_dict.update(self._env_var_config)
        else:
            self.logger.info("Skipping parse of env var settings...")
        for k, v in config_dict.items():
            if self._is_secret_config(k):
                config_dict[k] = SecretString(v)
        self._config = config_dict
        metrics._setup_logging(  # noqa: SLF001
            self.config,
            package=self.__module__.split(".", maxsplit=1)[0],
        )
        self.metrics_logger = metrics.get_metrics_logger()

        self._validate_config(raise_errors=validate_config)
        self._mapper: PluginMapper | None = None

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
    def capabilities(self) -> list[CapabilitiesEnum]:  # noqa: PLR6301
        """Get capabilities.

        Developers may override this property in order to add or remove
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
    def _env_var_prefix(cls) -> str:  # noqa: N805
        return f"{cls.name.upper().replace('-', '_')}_"

    @classproperty
    def _env_var_config(cls) -> dict[str, t.Any]:  # noqa: N805
        """Return any config specified in environment variables.

        Variables must match the convention "<PLUGIN_NAME>_<SETTING_NAME>",
        all uppercase with dashes converted to underscores.

        Returns:
            Dictionary of configuration parsed from the environment.
        """
        config_jsonschema = cls.config_jsonschema
        cls.append_builtin_config(config_jsonschema)

        return parse_environment_config(config_jsonschema, cls._env_var_prefix)

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

        return list(about.get_supported_pythons(package_metadata["Requires-Python"]))

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
        return t.cast("dict", MappingProxyType(self._config))

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

    def _validate_config(self, *, raise_errors: bool = True) -> list[str]:
        """Validate configuration input against the plugin configuration JSON schema.

        Args:
            raise_errors: Flag to throw an exception if any validation errors are found.

        Returns:
            A list of validation errors.

        Raises:
            ConfigValidationError: If raise_errors is True and validation fails.
        """
        errors: list[str] = []
        config_jsonschema = self.config_jsonschema

        if config_jsonschema:
            self.append_builtin_config(config_jsonschema)
            self.logger.debug(
                "Validating config using jsonschema: %s",
                config_jsonschema,
            )
            validator = JSONSchemaValidator(config_jsonschema)
            errors = [
                _format_validation_error(e) for e in validator.iter_errors(self._config)
            ]

        if errors:
            summary = (
                f"Config validation failed: {'; '.join(errors)}\n"
                f"JSONSchema was: {config_jsonschema}"
            )
            if raise_errors:
                raise ConfigValidationError(summary, errors=errors)

            self.logger.warning(summary)

        return errors

    @classmethod
    def print_version(
        cls: type[PluginBase],
        print_fn: t.Callable[[t.Any], None] = print,
    ) -> None:
        """Print help text for the tap.

        Args:
            print_fn: A function to use to display the plugin version.
                Defaults to :py:func:`print`.
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
            env_var_prefix=cls._env_var_prefix,
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
        return SingerCommand(
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
            logger=cls.logger,
        )

    @plugin_cli
    def cli(cls) -> click.Command:
        """Handle command line execution.

        Returns:
            A callable CLI object.
        """
        return cls.get_singer_command()


_T = t.TypeVar("_T")


class BaseSingerIO(PluginBase):
    """Base class for Singer taps and targets."""

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ):
        """Initialize the Singer tap or target.

        Args:
            config: May be one or more paths, either as str or PurePath objects, or
                it can be a predetermined config dict.
            parse_env_config: True to parse settings from env vars.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

    @classmethod
    def instantiate_processor(cls, instance: _T | None, default_cls: type[_T]) -> _T:
        """Instantiate a processor instance.

        Args:
            instance: The instance to instantiate.
            default_cls: The default class to instantiate.

        Returns:
            The instantiated processor instance.
        """
        return instance or default_cls()


class BaseSingerReader(BaseSingerIO, metaclass=abc.ABCMeta):
    """Base class for Singer readers."""

    message_reader_class: type[GenericSingerReader] = SingerReader
    """The message writer class to use for writing messages."""

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        message_reader: GenericSingerReader | None = None,
    ) -> None:
        """Initialize the Singer reader.

        Args:
            config: May be one or more paths, either as str or PurePath objects, or
                it can be a predetermined config dict.
            parse_env_config: True to parse settings from env vars.
            validate_config: True to require validation of config settings.
            message_reader: A message reader object.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        self.message_reader = self.instantiate_processor(
            message_reader,
            self.message_reader_class,
        )

    @t.final
    def listen(self, file_input: t.IO[str] | None = None) -> None:
        """Read from input until all messages are processed.

        Args:
            file_input: Readable stream of messages. Defaults to standard in.
        """
        counter = self.process_lines(file_input)
        line_count = sum(counter.values())

        self.logger.info(
            "Target '%s' completed reading %d lines of input "
            "(%d schemas, %d records, %d batch manifests, %d state messages).",
            self.name,
            line_count,
            counter[SingerMessageType.SCHEMA],
            counter[SingerMessageType.RECORD],
            counter[SingerMessageType.BATCH],
            counter[SingerMessageType.STATE],
        )
        self.process_endofpipe()

    @t.final
    def process_lines(self, file_input: t.IO[str] | None) -> t.Counter[str]:
        """Internal method to process jsonl lines from a Singer tap.

        Args:
            file_input: Readable stream of messages, each on a separate line.

        Returns:
            A counter object for the processed lines.
        """
        return self.message_reader.process_lines(
            file_input,
            callbacks={
                SingerMessageType.SCHEMA: self._process_schema_message,
                SingerMessageType.RECORD: self._process_record_message,
                SingerMessageType.STATE: self._process_state_message,
                SingerMessageType.ACTIVATE_VERSION: self._process_activate_version_message,  # noqa: E501
                SingerMessageType.BATCH: self._process_batch_message,
            },
        )

    def process_endofpipe(self) -> None:
        """Process end of pipe."""

    def _assert_line_requires(self, message_dict: dict, requires: set[str]) -> None:
        """Assert that a message contains required fields.

        Args:
            message_dict: The message to check.
            requires: The required fields.
        """
        self.message_reader.assert_line_requires(message_dict, requires)

    @abc.abstractmethod
    def _process_schema_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_record_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_state_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_activate_version_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_batch_message(self, message_dict: dict) -> None: ...


class BaseSingerWriter(BaseSingerIO):
    """Base class for Singer writers."""

    message_writer_class: type[GenericSingerWriter] = SingerWriter
    """The message writer class to use for writing messages."""

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        message_writer: GenericSingerWriter | None = None,
    ) -> None:
        """Initialize the Singer writer.

        Args:
            config: May be one or more paths, either as str or PurePath objects, or
                it can be a predetermined config dict.
            parse_env_config: True to parse settings from env vars.
            validate_config: True to require validation of config settings.
            message_writer: A message writer object.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        self.message_writer = self.instantiate_processor(
            message_writer,
            self.message_writer_class,
        )

    @t.final
    def write_message(self, message: t.Any) -> None:  # noqa: ANN401
        """Write a message to the tap's message writer."""
        self.message_writer.write_message(message)
