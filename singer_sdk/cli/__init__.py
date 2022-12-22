"""Helpers for the tap, target and mapper CLIs."""

from singer_sdk.cli.command import SingerCommand
from singer_sdk.cli.common_options import (
    PLUGIN_ABOUT,
    PLUGIN_ABOUT_FORMAT,
    PLUGIN_CONFIG,
    PLUGIN_FILE_INPUT,
    PLUGIN_VERSION,
)

__all__ = [
    "SingerCommand",
    "PLUGIN_VERSION",
    "PLUGIN_ABOUT",
    "PLUGIN_ABOUT_FORMAT",
    "PLUGIN_CONFIG",
    "PLUGIN_FILE_INPUT",
]
