"""Abstract base class for stream mapper plugins."""

from __future__ import annotations

import abc
import typing as t

import click

import singer_sdk._singerlib as singer
from singer_sdk.cli import common_options
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import CapabilitiesEnum, PluginCapabilities
from singer_sdk.io_base import SingerReader
from singer_sdk.plugin_base import PluginBase

if t.TYPE_CHECKING:
    from io import FileIO


class InlineMapper(PluginBase, SingerReader, metaclass=abc.ABCMeta):
    """Abstract base class for inline mappers."""

    @classproperty
    def capabilities(self) -> list[CapabilitiesEnum]:
        """Get capabilities.

        Returns:
            A list of plugin capabilities.
        """
        return [
            PluginCapabilities.STREAM_MAPS,
        ]

    @staticmethod
    def _write_messages(messages: t.Iterable[singer.Message]) -> None:
        for message in messages:
            singer.write_message(message)

    def _process_schema_message(self, message_dict: dict) -> None:
        self._write_messages(self.map_schema_message(message_dict))

    def _process_record_message(self, message_dict: dict) -> None:
        self._write_messages(self.map_record_message(message_dict))

    def _process_state_message(self, message_dict: dict) -> None:
        self._write_messages(self.map_state_message(message_dict))

    def _process_activate_version_message(self, message_dict: dict) -> None:
        self._write_messages(self.map_activate_version_message(message_dict))

    def _process_batch_message(self, message_dict: dict) -> None:
        self._write_messages(self.map_batch_message(message_dict))

    @abc.abstractmethod
    def map_schema_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a schema message to zero or more new messages.

        Args:
            message_dict: A SCHEMA message JSON dictionary.
        """
        ...

    @abc.abstractmethod
    def map_record_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a record message to zero or more new messages.

        Args:
            message_dict: A RECORD message JSON dictionary.
        """
        ...

    @abc.abstractmethod
    def map_state_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a state message to zero or more new messages.

        Args:
            message_dict: A STATE message JSON dictionary.
        """
        ...

    @abc.abstractmethod
    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> t.Iterable[singer.Message]:
        """Map a version message to zero or more new messages.

        Args:
            message_dict: An ACTIVATE_VERSION message JSON dictionary.
        """
        ...

    def map_batch_message(
        self,
        message_dict: dict,
    ) -> t.Iterable[singer.Message]:
        """Map a batch message to zero or more new messages.

        Args:
            message_dict: A BATCH message JSON dictionary.

        Raises:
            NotImplementedError: if not implemented by subclass.
        """
        raise NotImplementedError("BATCH messages are not supported by mappers.")

    @classproperty
    def cli(cls) -> t.Callable:  # noqa: N805
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
            *,
            version: bool = False,
            about: bool = False,
            config: tuple[str, ...] = (),
            about_format: str | None = None,
            file_input: FileIO | None = None,
        ) -> None:
            """Handle command line execution.

            Args:
                version: Display the package version.
                about: Display package metadata and settings.
                about_format: Specify output style for `--about`.
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

            config_files, parse_env_config = cls.config_from_cli_args(*config)
            mapper = cls(  # type: ignore[operator]
                config=config_files or None,
                validate_config=validate_config,
                parse_env_config=parse_env_config,
            )

            if about:
                mapper.print_about(about_format)
            else:
                mapper.listen(file_input)

        return cli
