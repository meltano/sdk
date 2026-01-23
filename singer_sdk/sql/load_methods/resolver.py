"""Load method strategy resolution from configuration."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

from singer_sdk.helpers.capabilities import TargetLoadMethods
from singer_sdk.sql.load_methods.handlers import (
    HardDeleteHandler,
    NullHandler,
    SoftDeleteHandler,
)
from singer_sdk.sql.load_methods.strategies import (
    AppendStrategy,
    OverwriteStrategy,
    UpsertStrategy,
)

if t.TYPE_CHECKING:
    from collections.abc import Mapping

    from singer_sdk.sql.load_methods.protocols import (
        ActivateVersionHandler,
        LoadMethodStrategy,
    )


@dataclass
class LoadMethodComposite:
    """Composes a strategy with an ACTIVATE_VERSION handler.

    This class combines a load method strategy with an appropriate
    handler for ACTIVATE_VERSION messages, providing a unified
    interface for SQLSink.

    The strategy handles regular batch operations while the handler
    manages version activation for full sync scenarios.

    Example:
        >>> composite = LoadMethodComposite(
        ...     strategy=UpsertStrategy(),
        ...     activate_version_handler=SoftDeleteHandler(),
        ... )
        >>> # Use composite.strategy.process_batch(...) for batches
        >>> # Use composite.activate_version(...) for ACTIVATE_VERSION
    """

    strategy: LoadMethodStrategy
    """The load method strategy for batch operations."""

    activate_version_handler: ActivateVersionHandler | None = None
    """Optional handler for ACTIVATE_VERSION messages."""

    def supports_activate_version(self) -> bool:
        """Check if this composite supports ACTIVATE_VERSION.

        Returns:
            True if both strategy supports it and handler is configured.
        """
        return (
            self.strategy.supports_activate_version()
            and self.activate_version_handler is not None
        )


class LoadMethodResolver:
    """Factory that resolves configuration to strategy composites.

    This class creates the appropriate LoadMethodComposite based on
    target configuration options including:

    - load_method: "append-only", "upsert", or "overwrite"
    - hard_delete: Whether to use hard or soft deletes
    - process_activate_version_messages: Whether to handle ACTIVATE_VERSION

    The resolver also provides an extension point for registering custom
    strategies.

    Example:
        >>> config = {"load_method": "upsert", "hard_delete": True}
        >>> composite = LoadMethodResolver.resolve(config)
        >>> print(composite.strategy.name)  # "upsert"
    """

    # Registry for custom strategies
    _strategy_registry: t.ClassVar[dict[str, type[LoadMethodStrategy]]] = {
        TargetLoadMethods.APPEND_ONLY.value: AppendStrategy,
        TargetLoadMethods.UPSERT.value: UpsertStrategy,
        TargetLoadMethods.OVERWRITE.value: OverwriteStrategy,
    }

    @classmethod
    def resolve(cls, config: Mapping[str, t.Any]) -> LoadMethodComposite:
        """Resolve configuration to a LoadMethodComposite.

        This method examines the target configuration and creates the
        appropriate strategy and handler combination.

        Args:
            config: Target configuration dictionary. Expected keys:
                - load_method: "append-only", "upsert", or "overwrite"
                - hard_delete: bool (default False)
                - process_activate_version_messages: bool (default True)

        Returns:
            A LoadMethodComposite with the appropriate strategy and handler.

        Raises:
            ValueError: If the load_method is not recognized.
        """
        # Get load method from config, default to append-only
        load_method = config.get("load_method", TargetLoadMethods.APPEND_ONLY.value)

        # Normalize enum to string value
        if isinstance(load_method, TargetLoadMethods):
            load_method = load_method.value

        # Get the strategy class from registry
        strategy_class = cls._strategy_registry.get(load_method)
        if strategy_class is None:
            available = ", ".join(cls._strategy_registry.keys())
            msg = (
                f"Unknown load_method: '{load_method}'. Available methods: {available}"
            )
            raise ValueError(msg)

        # Instantiate the strategy
        strategy = strategy_class()

        # Determine the ACTIVATE_VERSION handler
        handler = cls._resolve_activate_version_handler(config, strategy)

        return LoadMethodComposite(
            strategy=strategy,
            activate_version_handler=handler,
        )

    @classmethod
    def _resolve_activate_version_handler(
        cls,
        config: Mapping[str, t.Any],
        strategy: LoadMethodStrategy,
    ) -> ActivateVersionHandler | None:
        """Resolve the appropriate ACTIVATE_VERSION handler.

        Args:
            config: Target configuration.
            strategy: The resolved load method strategy.

        Returns:
            The appropriate handler, or None if not applicable.
        """
        # Check if ACTIVATE_VERSION processing is disabled
        process_activate = config.get("process_activate_version_messages", True)
        if not process_activate:
            return NullHandler()

        # Check if strategy supports ACTIVATE_VERSION
        if not strategy.supports_activate_version():
            return NullHandler()

        # Choose between hard and soft delete
        hard_delete = config.get("hard_delete", False)
        if hard_delete:
            return HardDeleteHandler()

        return SoftDeleteHandler()

    @classmethod
    def register_strategy(
        cls,
        name: str,
        strategy_class: type[LoadMethodStrategy],
    ) -> None:
        """Register a custom load method strategy.

        This allows target implementations to add custom strategies
        beyond the built-in append, upsert, and overwrite.

        Args:
            name: The strategy name (used in load_method config).
            strategy_class: The strategy class to instantiate.

        Example:
            >>> class MyCustomStrategy:
            ...     @property
            ...     def name(self) -> str:
            ...         return "custom"
            ...
            ...     # ... implement other methods
            >>> LoadMethodResolver.register_strategy("custom", MyCustomStrategy)
        """
        cls._strategy_registry[name] = strategy_class

    @classmethod
    def unregister_strategy(cls, name: str) -> None:
        """Unregister a custom load method strategy.

        This removes a previously registered strategy from the registry.
        Built-in strategies can also be removed if needed.

        Args:
            name: The strategy name to remove.

        Raises:
            KeyError: If the strategy name is not registered.
        """
        if name not in cls._strategy_registry:
            msg = f"Strategy '{name}' is not registered."
            raise KeyError(msg)
        del cls._strategy_registry[name]

    @classmethod
    def list_strategies(cls) -> list[str]:
        """List all registered strategy names.

        Returns:
            A list of registered strategy names.
        """
        return list(cls._strategy_registry.keys())

    @classmethod
    def reset_registry(cls) -> None:
        """Reset the strategy registry to defaults.

        This removes any custom strategies and restores the
        built-in append, upsert, and overwrite strategies.
        """
        cls._strategy_registry = {
            TargetLoadMethods.APPEND_ONLY.value: AppendStrategy,
            TargetLoadMethods.UPSERT.value: UpsertStrategy,
            TargetLoadMethods.OVERWRITE.value: OverwriteStrategy,
        }
