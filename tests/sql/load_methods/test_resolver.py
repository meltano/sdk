"""Tests for LoadMethodResolver."""

from __future__ import annotations

import pytest

from singer_sdk.helpers.capabilities import TargetLoadMethods
from singer_sdk.sql.load_methods.handlers import (
    HardDeleteHandler,
    NullHandler,
    SoftDeleteHandler,
)
from singer_sdk.sql.load_methods.resolver import (
    LoadMethodComposite,
    LoadMethodResolver,
)
from singer_sdk.sql.load_methods.strategies import (
    AppendStrategy,
    OverwriteStrategy,
    UpsertStrategy,
)


class TestLoadMethodResolver:
    """Tests for LoadMethodResolver."""

    def setup_method(self) -> None:
        """Reset registry before each test."""
        LoadMethodResolver.reset_registry()

    def test_resolve_append_default(self) -> None:
        """Test resolving default append-only strategy."""
        config: dict = {}
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.strategy, AppendStrategy)
        assert composite.strategy.name == "append-only"
        assert isinstance(composite.activate_version_handler, SoftDeleteHandler)

    def test_resolve_append_explicit(self) -> None:
        """Test resolving explicit append-only strategy."""
        config = {"load_method": "append-only"}
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.strategy, AppendStrategy)

    def test_resolve_append_with_enum(self) -> None:
        """Test resolving with TargetLoadMethods enum value."""
        config = {"load_method": TargetLoadMethods.APPEND_ONLY}
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.strategy, AppendStrategy)

    def test_resolve_upsert(self) -> None:
        """Test resolving upsert strategy."""
        config = {"load_method": "upsert"}
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.strategy, UpsertStrategy)
        assert composite.strategy.name == "upsert"

    def test_resolve_overwrite(self) -> None:
        """Test resolving overwrite strategy."""
        config = {"load_method": "overwrite"}
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.strategy, OverwriteStrategy)
        assert composite.strategy.name == "overwrite"
        # Overwrite doesn't support ACTIVATE_VERSION
        assert isinstance(composite.activate_version_handler, NullHandler)

    def test_resolve_with_hard_delete(self) -> None:
        """Test resolving with hard_delete enabled."""
        config = {"load_method": "append-only", "hard_delete": True}
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.activate_version_handler, HardDeleteHandler)

    def test_resolve_with_soft_delete(self) -> None:
        """Test resolving with hard_delete disabled (soft delete)."""
        config = {"load_method": "upsert", "hard_delete": False}
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.activate_version_handler, SoftDeleteHandler)

    def test_resolve_with_activate_version_disabled(self) -> None:
        """Test resolving with ACTIVATE_VERSION processing disabled."""
        config = {
            "load_method": "append-only",
            "process_activate_version_messages": False,
        }
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.activate_version_handler, NullHandler)

    def test_resolve_unknown_method_raises(self) -> None:
        """Test that unknown load_method raises ValueError."""
        config = {"load_method": "invalid-method"}

        with pytest.raises(ValueError, match="Unknown load_method"):
            LoadMethodResolver.resolve(config)

    def test_register_custom_strategy(self) -> None:
        """Test registering a custom strategy."""

        class CustomStrategy:
            @property
            def name(self) -> str:
                return "custom"

            def prepare_table(self, table, context, engine):
                pass

            def process_batch(self, table, context, records, engine, dialect):
                pass

            def handle_soft_delete_record(self, context, record):
                return record

            def supports_activate_version(self) -> bool:
                return True

        LoadMethodResolver.register_strategy("custom", CustomStrategy)

        config = {"load_method": "custom"}
        composite = LoadMethodResolver.resolve(config)

        assert isinstance(composite.strategy, CustomStrategy)

    def test_unregister_strategy(self) -> None:
        """Test unregistering a strategy."""

        class TempStrategy:
            @property
            def name(self) -> str:
                return "temp"

            def prepare_table(self, table, context, engine):
                pass

            def process_batch(self, table, context, records, engine, dialect):
                pass

            def handle_soft_delete_record(self, context, record):
                return record

            def supports_activate_version(self) -> bool:
                return True

        LoadMethodResolver.register_strategy("temp", TempStrategy)
        LoadMethodResolver.unregister_strategy("temp")

        config = {"load_method": "temp"}
        with pytest.raises(ValueError):
            LoadMethodResolver.resolve(config)

    def test_unregister_unknown_raises(self) -> None:
        """Test unregistering unknown strategy raises KeyError."""
        with pytest.raises(KeyError, match="not registered"):
            LoadMethodResolver.unregister_strategy("nonexistent")

    def test_list_strategies(self) -> None:
        """Test listing registered strategies."""
        strategies = LoadMethodResolver.list_strategies()

        assert "append-only" in strategies
        assert "upsert" in strategies
        assert "overwrite" in strategies

    def test_reset_registry(self) -> None:
        """Test resetting registry to defaults."""

        class TempStrategy:
            @property
            def name(self) -> str:
                return "temp"

            def prepare_table(self, table, context, engine):
                pass

            def process_batch(self, table, context, records, engine, dialect):
                pass

            def handle_soft_delete_record(self, context, record):
                return record

            def supports_activate_version(self) -> bool:
                return True

        LoadMethodResolver.register_strategy("temp", TempStrategy)
        assert "temp" in LoadMethodResolver.list_strategies()

        LoadMethodResolver.reset_registry()
        assert "temp" not in LoadMethodResolver.list_strategies()
        assert "append-only" in LoadMethodResolver.list_strategies()


class TestLoadMethodComposite:
    """Tests for LoadMethodComposite."""

    def test_supports_activate_version_true(self) -> None:
        """Test supports_activate_version when both conditions met."""
        composite = LoadMethodComposite(
            strategy=AppendStrategy(),
            activate_version_handler=SoftDeleteHandler(),
        )
        assert composite.supports_activate_version() is True

    def test_supports_activate_version_no_handler(self) -> None:
        """Test supports_activate_version with no handler."""
        composite = LoadMethodComposite(
            strategy=AppendStrategy(),
            activate_version_handler=None,
        )
        assert composite.supports_activate_version() is False

    def test_supports_activate_version_strategy_unsupported(self) -> None:
        """Test supports_activate_version when strategy doesn't support it."""
        composite = LoadMethodComposite(
            strategy=OverwriteStrategy(),
            activate_version_handler=SoftDeleteHandler(),
        )
        # OverwriteStrategy.supports_activate_version() returns False
        assert composite.supports_activate_version() is False
