"""Abstract base class for stream mapper plugins."""

from __future__ import annotations

import abc
import typing as t

import click

from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import CapabilitiesEnum, PluginCapabilities
from singer_sdk.io_base import SingerReader, SingerWriter
from singer_sdk.plugin_base import PluginBase

if t.TYPE_CHECKING:
    import singer_sdk._singerlib as singer


class InlineMapper(PluginBase, SingerReader, SingerWriter, metaclass=abc.ABCMeta):
    """Abstract base class for inline mappers."""

    @classproperty
    def capabilities(self) -> list[CapabilitiesEnum]:  # noqa: PLR6301
        """Get capabilities.

        Returns:
            A list of plugin capabilities.
        """
        return [
            PluginCapabilities.STREAM_MAPS,
        ]

    def _write_messages(self, messages: t.Iterable[singer.Message]) -> None:
        for message in messages:
            self.write_message(message)

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

    def map_batch_message(  # noqa: PLR6301
        self,
        message_dict: dict,  # noqa: ARG002
    ) -> t.Iterable[singer.Message]:
        """Map a batch message to zero or more new messages.

        Args:
            message_dict: A BATCH message JSON dictionary.

        Raises:
            NotImplementedError: if not implemented by subclass.
        """
        msg = "BATCH messages are not supported by mappers."
        raise NotImplementedError(msg)

    # CLI handler

    @classmethod
    def invoke(  # type: ignore[override]
        cls: type[InlineMapper],
        *,
        about: bool = False,
        about_format: str | None = None,
        config: tuple[str, ...] = (),
        file_input: t.IO[str] | None = None,
    ) -> None:
        """Invoke the mapper.

        Args:
            about: Display package metadata and settings.
            about_format: Specify output style for `--about`.
            config: Configuration file location or 'ENV' to use environment
                variables. Accepts multiple inputs as a tuple.
            file_input: Optional file to read input from.
        """
        super().invoke(about=about, about_format=about_format)
        cls.print_version(print_fn=cls.logger.info)
        config_files, parse_env_config = cls.config_from_cli_args(*config)

        mapper = cls(
            config=config_files,  # type: ignore[arg-type]
            validate_config=True,
            parse_env_config=parse_env_config,
        )
        mapper.listen(file_input)

    @classmethod
    def get_singer_command(cls: type[InlineMapper]) -> click.Command:
        """Execute standard CLI handler for inline mappers.

        Returns:
            A click.Command object.
        """
        command = super().get_singer_command()
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
