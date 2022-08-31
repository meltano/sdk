"""Abstract base class for stream mapper plugins."""

import abc
from io import FileIO
from typing import Iterable, List, Tuple, Type

import click
import singer

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import CapabilitiesEnum, PluginCapabilities
from singer_sdk.io_base import SingerReader
from singer_sdk.plugin_base import PluginBase


class InlineMapper(PluginBase, SingerReader, metaclass=abc.ABCMeta):
    """Abstract base class for inline mappers."""

    @classproperty
    def _env_prefix(cls) -> str:
        return f"{cls.name.upper().replace('-', '_')}_"

    @classproperty
    def capabilities(self) -> List[CapabilitiesEnum]:
        """Get capabilities.

        Returns:
            A list of plugin capabilities.
        """
        return [
            PluginCapabilities.STREAM_MAPS,
        ]

    @staticmethod
    def _write_messages(messages: Iterable[singer.Message]) -> None:
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

    @abc.abstractmethod
    def map_schema_message(self, message_dict: dict) -> Iterable[singer.Message]:
        """Map a schema message to zero or more new messages.

        Args:
            message_dict: A SCHEMA message JSON dictionary.
        """
        ...

    @abc.abstractmethod
    def map_record_message(self, message_dict: dict) -> Iterable[singer.Message]:
        """Map a record message to zero or more new messages.

        Args:
            message_dict: A RECORD message JSON dictionary.
        """
        ...

    @abc.abstractmethod
    def map_state_message(self, message_dict: dict) -> Iterable[singer.Message]:
        """Map a state message to zero or more new messages.

        Args:
            message_dict: A STATE message JSON dictionary.
        """
        ...

    @abc.abstractmethod
    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> Iterable[singer.Message]:
        """Map a version message to zero or more new messages.

        Args:
            message_dict: An ACTIVATE_VERSION message JSON dictionary.
        """
        ...

    # CLI handler

    @classmethod
    def invoke(
        cls: Type["InlineMapper"],
        config: Tuple[str, ...] = (),
        file_input: FileIO = None,
    ) -> None:
        """Invoke the mapper.

        Args:
            config: Configuration file location or 'ENV' to use environment
                variables. Accepts multiple inputs as a tuple.
            file_input: Optional file to read input from.
        """
        cls.print_version(print_fn=cls.logger.info)
        config_files, parse_env_config = cls.config_from_cli_args(*config)

        mapper = cls(
            config=config_files,
            validate_config=True,
            parse_env_config=parse_env_config,
        )
        mapper.listen(file_input)

    @classproperty
    def cli(cls) -> click.Command:
        """Execute standard CLI handler for inline mappers.

        Returns:
            A click.Command object.
        """
        command = super().cli
        command.help = "Execute the Singer mapper."
        command.params.extend(
            [
                click.Option(
                    ["--input", "file_input"],
                    help="A path to read messages from instead of from standard in.",
                    type=click.File("r"),
                ),
            ],
        )

        return command
