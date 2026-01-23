"""Load method strategies for SQL targets.

This module provides a pluggable architecture for SQL target load methods,
supporting append, upsert, and overwrite strategies through a SOLID design
that separates concerns and enables easy testing.

Basic usage:

    >>> from singer_sdk.sql.load_methods import LoadMethodResolver
    >>>
    >>> # Resolve strategy from target config
    >>> config = {"load_method": "upsert", "hard_delete": False}
    >>> composite = LoadMethodResolver.resolve(config)
    >>>
    >>> # Use the strategy
    >>> result = composite.strategy.process_batch(context, records, connector)

Available strategies:

- **AppendStrategy**: Always INSERT all records (may create duplicates)
- **UpsertStrategy**: UPDATE existing records, INSERT new ones (requires key_properties)
- **OverwriteStrategy**: DROP and recreate table on first batch, then INSERT

ACTIVATE_VERSION handlers:

- **SoftDeleteHandler**: Sets `_sdc_deleted_at` timestamp on old version records
- **HardDeleteHandler**: DELETEs old version records from the database
- **NullHandler**: No-op for when ACTIVATE_VERSION processing is disabled

Custom strategies can be registered:

    >>> from singer_sdk.sql.load_methods import LoadMethodResolver
    >>>
    >>> class MyCustomStrategy:
    ...     @property
    ...     def name(self) -> str:
    ...         return "custom"
    ...
    ...     def prepare_table(self, context, connector): ...
    ...
    ...     def process_batch(self, context, records, connector): ...
    ...
    ...     def handle_soft_delete_record(self, context, record):
    ...         return record
    ...
    ...     def supports_activate_version(self) -> bool:
    ...         return True
    >>>
    >>> LoadMethodResolver.register_strategy("custom", MyCustomStrategy)
"""

from __future__ import annotations

from singer_sdk.sql.load_methods.context import (
    ActivateVersionResult,
    BatchResult,
    LoadContext,
    TablePreparationResult,
)
from singer_sdk.sql.load_methods.dialect import (
    DialectHelper,
    GenericDialectHelper,
    MySQLDialectHelper,
    PostgreSQLDialectHelper,
    SQLiteDialectHelper,
    get_dialect_helper,
)
from singer_sdk.sql.load_methods.handlers import (
    HardDeleteHandler,
    NullHandler,
    SoftDeleteHandler,
)
from singer_sdk.sql.load_methods.protocols import (
    ActivateVersionHandler,
    LoadMethodStrategy,
)
from singer_sdk.sql.load_methods.resolver import (
    LoadMethodComposite,
    LoadMethodResolver,
)
from singer_sdk.sql.load_methods.strategies import (
    AppendStrategy,
    BaseStrategy,
    OverwriteStrategy,
    UpsertStrategy,
)

__all__ = [
    "ActivateVersionHandler",
    "ActivateVersionResult",
    "AppendStrategy",
    "BaseStrategy",
    "BatchResult",
    "DialectHelper",
    "GenericDialectHelper",
    "HardDeleteHandler",
    "LoadContext",
    "LoadMethodComposite",
    "LoadMethodResolver",
    "LoadMethodStrategy",
    "MySQLDialectHelper",
    "NullHandler",
    "OverwriteStrategy",
    "PostgreSQLDialectHelper",
    "SQLiteDialectHelper",
    "SoftDeleteHandler",
    "TablePreparationResult",
    "UpsertStrategy",
    "get_dialect_helper",
]
