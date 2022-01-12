"""Abstract base class for stream mapper plugins."""

import abc
from io import FileIO
from typing import Callable, Tuple

import click

from singer_sdk.cli import common_options
from singer_sdk.configuration._dict_config import merge_config_sources
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.io_base import SingerReader
from singer_sdk.plugin_base import PluginBase


class InlineMapper(PluginBase, SingerReader, metaclass=abc.ABCMeta):
    """TODO."""

    @classproperty
    def _env_prefix(cls) -> str:
        return f"{cls.name.upper().replace('-', '_')}_"

    @classproperty
    def cli(cls) -> Callable:
        """Execute standard CLI handler for inline mappers.

        Returns:
            A callable CLI object.
        """

        @common_options.PLUGIN_VERSION
        @common_options.PLUGIN_ABOUT
        @common_options.PLUGIN_ABOUT_FORMAT
        @common_options.PLUGIN_CONFIG
        @common_options.PLUGIN_FILE_INPUT
        @click.command(
            help="Execute the Singer mapper.",
            context_settings={"help_option_names": ["--help"]},
        )
        def cli(
            version: bool = False,
            about: bool = False,
            config: Tuple[str, ...] = (),
            format: str = None,
            file_input: FileIO = None,
        ) -> None:
            """Handle command line execution.

            Args:
                version: Display the package version.
                about: Display package metadata and settings.
                format: Specify output style for `--about`.
                config: Configuration file location or 'ENV' to use environment
                    variables. Accepts multiple inputs as a tuple.
                file_input: Specify a path to an input file to read messages from.
                    Defaults to standard in if unspecified.
            """
            if version:
                cls.print_version()
                return

            if not about:
                cls.print_version(print_fn=cls.logger.info)

            validate_config: bool = True
            if about:
                validate_config = False

            cls.print_version(print_fn=cls.logger.info)

            config_dict = merge_config_sources(
                config,
                cls.config_jsonschema,
                cls._env_prefix,
            )

            mapper = cls(  # type: ignore  # Ignore 'type not callable'
                config=config_dict,
                validate_config=validate_config,
            )

            if about:
                mapper.print_about(format)
            else:
                mapper.listen(file_input)

        return cli
