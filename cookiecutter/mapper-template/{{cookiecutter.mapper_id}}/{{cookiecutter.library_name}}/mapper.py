"""{{ cookiecutter.name }} mapper class."""

from __future__ import annotations

import typing as t

import singer_sdk.typing as th
from singer_sdk import _singerlib as singer
from singer_sdk.mapper import PluginMapper
from singer_sdk.mapper_base import InlineMapper

if t.TYPE_CHECKING:
    from pathlib import PurePath


class {{ cookiecutter.name }}Mapper(InlineMapper):
    """Sample mapper for {{ cookiecutter.name }}."""

    name = "{{ cookiecutter.mapper_id }}"

    config_jsonschema = th.PropertiesList(
        # TODO: Replace or remove this example config based on your needs
        th.Property(
            "example_config",
            th.StringType,
            title="Example Configuration",
            description="An example config, replace or remove based on your needs.",
        ),
    ).to_dict()

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Create a new inline mapper.

        Args:
            config: Mapper configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        self.mapper = PluginMapper(plugin_config=dict(self.config), logger=self.logger)

    def map_schema_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a schema message to zero or more new messages.

        Args:
            message_dict: A SCHEMA message JSON dictionary.
        """
        yield singer.SchemaMessage.from_dict(message_dict)

    def map_record_message(
        self,
        message_dict: dict,
    ) -> t.Iterable[singer.RecordMessage]:
        """Map a record message to zero or more new messages.

        Args:
            message_dict: A RECORD message JSON dictionary.
        """
        yield singer.RecordMessage.from_dict(message_dict)

    def map_state_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a state message to zero or more new messages.

        Args:
            message_dict: A STATE message JSON dictionary.
        """
        yield singer.StateMessage.from_dict(message_dict)

    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> t.Iterable[singer.Message]:
        """Map a version message to zero or more new messages.

        Args:
            message_dict: An ACTIVATE_VERSION message JSON dictionary.
        """
        yield singer.ActivateVersionMessage.from_dict(message_dict)


if __name__ == "__main__":
    {{ cookiecutter.name }}Mapper.cli()
