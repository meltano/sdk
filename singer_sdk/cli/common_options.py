"""Common CLI options for plugins."""

from __future__ import annotations

import typing as t

import click

PLUGIN_VERSION: t.Callable[..., t.Any] = click.option(
    "--version",
    is_flag=True,
    help="Display the package version.",
)

PLUGIN_ABOUT: t.Callable[..., t.Any] = click.option(
    "--about",
    is_flag=True,
    help="Display package metadata and settings.",
)

PLUGIN_ABOUT_FORMAT: t.Callable[..., t.Any] = click.option(
    "--format",
    "about_format",
    help="Specify output style for --about",
    type=click.Choice(["json", "markdown"], case_sensitive=False),
    default=None,
)

PLUGIN_CONFIG: t.Callable[..., t.Any] = click.option(
    "--config",
    multiple=True,
    help="Configuration file location or 'ENV' to use environment variables.",
    type=click.STRING,
    default=(),
)

PLUGIN_FILE_INPUT: t.Callable[..., t.Any] = click.option(
    "--input",
    "file_input",
    help="A path to read messages from instead of from standard in.",
    type=click.File("r"),
)
