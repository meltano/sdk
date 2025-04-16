"""Base class for all connectors."""

from __future__ import annotations

import abc
import typing as t
from contextlib import contextmanager

if t.TYPE_CHECKING:
    from collections.abc import Mapping

_T = t.TypeVar("_T")


# class BaseConnector(abc.ABC, t.Generic[_T_co]):
class BaseConnector(abc.ABC, t.Generic[_T]):
    """Base class for all connectors."""

    def __init__(self, config: Mapping[str, t.Any] | None = None) -> None:
        """Initialize the connector.

        Args:
            config: Plugin configuration parameters.
        """
        self._config = config or {}

    @property
    def config(self) -> Mapping[str, t.Any]:
        """Return the connector configuration.

        Returns:
            A mapping of configuration parameters.
        """
        return self._config

    @config.setter
    def config(self, config: Mapping[str, t.Any]) -> None:
        """Set the connector configuration.

        Args:
            config: Plugin configuration parameters.
        """
        self._config = config

    @contextmanager
    def connect(self, *args: t.Any, **kwargs: t.Any) -> t.Generator[_T, None, None]:
        """Connect to the destination.

        Args:
            args: Positional arguments to pass to the connection method.
            kwargs: Keyword arguments to pass to the connection method.

        Yields:
            A connection object.
        """
        yield self.get_connection(*args, **kwargs)

    @abc.abstractmethod
    def get_connection(self, *args: t.Any, **kwargs: t.Any) -> _T:
        """Connect to the destination.

        Args:
            args: Positional arguments to pass to the connection method.
            kwargs: Keyword arguments to pass to the connection method.
        """
        ...
