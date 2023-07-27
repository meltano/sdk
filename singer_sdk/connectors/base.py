"""Base class for all connectors."""

from __future__ import annotations

import abc
import typing as t
from contextlib import contextmanager

from singer_sdk.helpers._compat import Protocol

_T_co = t.TypeVar("_T_co", covariant=True)


class ContextManagerProtocol(Protocol[_T_co]):
    """Protocol for context manager enter/exit."""

    def __enter__(self) -> _T_co:  # noqa: D105
        ...  # pragma: no cover

    def __exit__(self, *args: t.Any) -> None:  # noqa: D105
        ...  # pragma: no cover


_C = t.TypeVar("_C", bound=ContextManagerProtocol)


class BaseConnector(abc.ABC, t.Generic[_C]):
    """Base class for all connectors."""

    def __init__(self, config: t.Mapping[str, t.Any] | None) -> None:
        """Initialize the connector.

        Args:
            config: Plugin configuration parameters.
        """
        self._config = config or {}

    @property
    def config(self) -> t.Mapping:
        """Return the connector configuration.

        Returns:
            A mapping of configuration parameters.
        """
        return self._config

    @contextmanager
    def connect(self, *args: t.Any, **kwargs: t.Any) -> t.Generator[_C, None, None]:
        """Connect to the destination.

        Args:
            args: Positional arguments to pass to the connection method.
            kwargs: Keyword arguments to pass to the connection method.

        Yields:
            A connection object.
        """
        with self.get_connection(*args, **kwargs) as connection:
            yield connection

    @abc.abstractmethod
    def get_connection(self, *args: t.Any, **kwargs: t.Any) -> _C:
        """Connect to the destination.

        Args:
            args: Positional arguments to pass to the connection method.
            kwargs: Keyword arguments to pass to the connection method.
        """
        ...
