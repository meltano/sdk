"""Custom click commands for Singer packages."""

import logging
from typing import Any

import click

__all__ = ["SingerCommand"]

logger = logging.getLogger(__name__)


class SingerCommand(click.Command):
    """Custom click command class for Singer packages."""

    def invoke(self, ctx: click.Context) -> Any:  # noqa: ANN401
        """Invoke the command, capturing warnings and logging them.

        Args:
            ctx: The `click` context.

        Returns:
            The result of the command invocation.
        """
        logging.captureWarnings(True)
        return super().invoke(ctx)
