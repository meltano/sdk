from __future__ import annotations

import typing as t

T = t.TypeVar("T")


class ConfigProperty(t.Generic[T]):
    """A descriptor that gets a value from a named key of the config attribute."""

    def __init__(self, custom_key: str | None = None, *, default: T | None = None):
        """Initialize the descriptor.

        Args:
            custom_key: The key to get from the config attribute instead of the
                attribute name.
            default: The default value if the key is not found.
        """
        self.key = custom_key
        self.default = default

    def __set_name__(self, owner, name: str) -> None:  # noqa: ANN001
        """Set the name of the attribute.

        Args:
            owner: The class of the object.
            name: The name of the attribute.
        """
        self.key = self.key or name

    def __get__(self, instance, owner) -> T | None:  # noqa: ANN001
        """Get the value from the instance's config attribute.

        Args:
            instance: The instance of the object.
            owner: The class of the object.

        Returns:
            The value from the config attribute.
        """
        return instance.config.get(self.key, self.default)  # type: ignore[no-any-return]
