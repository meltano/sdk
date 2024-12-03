from __future__ import annotations

import typing as t

from ._config_property import ConfigProperty

if t.TYPE_CHECKING:
    from ._enum import CapabilitiesEnum

_T = t.TypeVar("_T")


class Builtin:
    """Use this class to define built-in setting(s) for a plugin."""

    def __init__(
        self,
        schema: dict[str, t.Any],
        *,
        capability: CapabilitiesEnum | None = None,
        **kwargs: t.Any,
    ):
        """Initialize the descriptor.

        Args:
            schema: The JSON schema for the setting.
            capability: The capability that the setting is associated with.
            kwargs: Additional keyword arguments.
        """
        self.schema = schema
        self.capability = capability
        self.kwargs = kwargs

    def attribute(  # noqa: PLR6301
        self,
        custom_key: str | None = None,
        *,
        default: _T | None = None,
    ) -> ConfigProperty[_T]:
        """Generate a class attribute for the setting.

        Args:
            custom_key: Custom key to use in the config.
            default: Default value for the setting.

        Returns:
            Class attribute for the setting.
        """
        return ConfigProperty(custom_key=custom_key, default=default)
