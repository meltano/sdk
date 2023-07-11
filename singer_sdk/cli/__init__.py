"""Helpers for the tap, target and mapper CLIs."""

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    import click

_T = t.TypeVar("_T")


class plugin_cli:  # noqa: N801
    """Decorator to create a plugin CLI."""

    def __init__(self, method: t.Callable[..., click.Command]) -> None:
        """Create a new plugin CLI.

        Args:
            method: The method to call to get the command.
        """
        self.method = method
        self.name: str | None = None

    def __get__(self, instance: _T, owner: type[_T]) -> click.Command:
        """Get the command.

        Args:
            instance: The instance of the plugin.
            owner: The plugin class.

        Returns:
            The CLI entrypoint.
        """
        return self.method(owner)
