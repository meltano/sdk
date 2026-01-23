# SQL Target Load Methods

This document describes the pluggable load method architecture for SQL targets, which provides a strategy-based approach to data loading with support for append, upsert, and overwrite operations.

## Overview

SQL targets need to handle different data loading patterns depending on use case requirements:

- **Append-only**: Insert all records, allowing duplicates (fast, simple)
- **Upsert**: Update existing records, insert new ones (requires primary keys)
- **Overwrite**: Drop and recreate table on each sync (full refresh)

The load methods architecture separates these concerns using the Strategy pattern, enabling clean composition, easy testing, and extensibility.

## Architecture

The system is composed of several components:

```
┌─────────────────────────────────────────────────────────────────┐
│                          SQLSink                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              LoadMethodComposite                         │    │
│  │  ┌─────────────────────┐  ┌────────────────────────┐    │    │
│  │  │  LoadMethodStrategy │  │ ActivateVersionHandler │    │    │
│  │  │  (append/upsert/    │  │ (soft/hard delete)     │    │    │
│  │  │   overwrite)        │  │                        │    │    │
│  │  └─────────────────────┘  └────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    DialectHelper                         │    │
│  │         (PostgreSQL, MySQL, SQLite, Generic)             │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `LoadMethodStrategy` | `protocols.py` | Protocol defining strategy interface |
| `ActivateVersionHandler` | `protocols.py` | Protocol for ACTIVATE_VERSION handling |
| `LoadContext` | `context.py` | Immutable context with configuration and state |
| `LoadMethodResolver` | `resolver.py` | Factory that resolves config to strategies |
| `LoadMethodComposite` | `resolver.py` | Composes strategy with version handler |
| `DialectHelper` | `dialect.py` | Database-specific operations (upsert syntax, type mapping) |

### Strategies

Three built-in strategies implement `LoadMethodStrategy`:

#### AppendStrategy

Always INSERTs all records. Simple and fast but creates duplicates if the same record is sent multiple times.

```python
def process_batch(self, table, context, records, engine, dialect):
    # Bulk INSERT all records
    return BatchResult(records_inserted=len(records))
```

**Use cases**: Event logs, time-series data, high-throughput scenarios.

#### UpsertStrategy

Updates existing records and inserts new ones based on `key_properties`. Uses native upsert syntax when available (PostgreSQL `ON CONFLICT`, MySQL `ON DUPLICATE KEY UPDATE`), otherwise falls back to a staging table pattern.

```python
def process_batch(self, table, context, records, engine, dialect):
    if dialect.supports_native_upsert():
        # INSERT ... ON CONFLICT DO UPDATE
        return self._native_upsert(...)
    else:
        # Staging table: INSERT → DELETE matching → INSERT from staging
        return self._staging_table_upsert(...)
```

**Use cases**: Dimension tables, master data, any scenario requiring deduplication.

#### OverwriteStrategy

Drops and recreates the table on the first batch, then inserts all subsequent batches.

```python
def prepare_table(self, table, context, engine):
    if context.is_first_batch:
        table.drop(engine, checkfirst=True)
        table.create(engine)
```

**Use cases**: Full table replication, small lookup tables, testing scenarios.

### ACTIVATE_VERSION Handlers

Three handlers manage ACTIVATE_VERSION messages (used to mark old records after full sync):

| Handler | Behavior |
|---------|----------|
| `SoftDeleteHandler` | Sets `_sdc_deleted_at` timestamp on old version records |
| `HardDeleteHandler` | DELETEs old version records from the database |
| `NullHandler` | No-op (for overwrite mode or when disabled) |

### DialectHelper

Abstracts database-specific operations:

```python
class DialectHelper(Protocol):
    def to_sql_type(self, jsonschema_type: dict) -> sa.types.TypeEngine: ...
    def generate_upsert_statement(self, table, key_columns) -> Executable: ...
    def supports_native_upsert(self) -> bool: ...
```

Built-in implementations:

- `GenericDialectHelper` - Basic type mapping, no native upsert
- `SQLiteDialectHelper` - SQLite `ON CONFLICT` syntax
- `PostgreSQLDialectHelper` - PostgreSQL `ON CONFLICT`, JSONB for objects
- `MySQLDialectHelper` - MySQL `ON DUPLICATE KEY UPDATE`

### LoadContext

Immutable dataclass containing configuration and state:

```python
@dataclass(frozen=True)
class LoadContext:
    key_properties: tuple[str, ...] = ()
    hard_delete: bool = False
    add_record_metadata: bool = False
    soft_delete_column: str = "_sdc_deleted_at"
    version_column: str = "_sdc_table_version"
    is_first_batch: bool = True
```

The `frozen=True` design enables pure-function testing and prevents accidental state mutation.

### Result Types

Operations return structured results for metrics and observability:

```python
@dataclass(frozen=True)
class BatchResult:
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_deleted: int = 0
    duplicates_merged: int = 0

@dataclass(frozen=True)
class ActivateVersionResult:
    records_soft_deleted: int = 0
    records_hard_deleted: int = 0
    version_activated: int = 0
```

## Configuration

The resolver reads these configuration options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `load_method` | string | `"append-only"` | Strategy: `"append-only"`, `"upsert"`, `"overwrite"` |
| `hard_delete` | bool | `False` | Use DELETE vs soft delete timestamp |
| `process_activate_version_messages` | bool | `True` | Handle ACTIVATE_VERSION messages |

## Integration with SQLSink

The `SQLSink` class integrates with load methods via a feature flag:

```python
class SQLSink(BatchSink):
    #: Enable strategy-based load methods
    use_load_method_strategies: bool = False
```

When enabled, `SQLSink`:

1. Resolves the composite from configuration in `__init__`
1. Uses `strategy.prepare_table()` in `setup()`
1. Delegates to `strategy.process_batch()` in `process_batch()`
1. Uses `activate_version_handler.activate_version()` for version activation

The flag allows gradual migration while maintaining backwards compatibility with the legacy implementation.

## Extension Points

### Custom Strategies

Register custom strategies for specialized loading patterns:

```python
from singer_sdk.sql.load_methods import LoadMethodResolver

class MyCustomStrategy:
    @property
    def name(self) -> str:
        return "custom"

    def prepare_table(self, table, context, engine):
        # Custom preparation logic
        ...

    def process_batch(self, table, context, records, engine, dialect):
        # Custom batch processing
        ...

    def handle_soft_delete_record(self, context, record):
        return record

    def supports_activate_version(self) -> bool:
        return True

LoadMethodResolver.register_strategy("custom", MyCustomStrategy)
```

### Custom Dialect Helpers

Implement `DialectHelper` for databases not covered by built-in helpers:

```python
class SnowflakeDialectHelper:
    def to_sql_type(self, jsonschema_type):
        # Snowflake-specific type mapping (VARIANT for JSON, etc.)
        ...

    def generate_upsert_statement(self, table, key_columns):
        # MERGE INTO syntax
        ...

    def supports_native_upsert(self):
        return True
```

## Testing

The architecture enables unit testing without database connections:

```python
def test_append_strategy():
    strategy = AppendStrategy()
    context = LoadContext(key_properties=("id",))

    # Test soft delete handling
    record = {"id": 1, "_sdc_deleted_at": "2024-01-01"}
    result = strategy.handle_soft_delete_record(context, record)
    assert result == record  # Append returns record as-is

def test_resolver():
    config = {"load_method": "upsert", "hard_delete": True}
    composite = LoadMethodResolver.resolve(config)

    assert composite.strategy.name == "upsert"
    assert isinstance(composite.activate_version_handler, HardDeleteHandler)
```

## Current Status

This is a work-in-progress feature. The following items are planned:

### TODO

- [ ] Integration tests with real databases (SQLite, PostgreSQL)
- [ ] Performance benchmarks comparing legacy vs strategy implementations
- [ ] Documentation for target developers on migration path
- [ ] Consider making `use_load_method_strategies` the default
- [ ] Add metrics/logging for batch results
- [ ] Handle schema evolution during batch processing
- [ ] Add support for batch-level transactions

### Known Limitations

1. **Hard delete in append mode**: Cannot delete existing records since append only inserts. Records marked for deletion are skipped.

1. **Staging table upsert**: The fallback upsert uses a staging table which requires appropriate permissions and may be slower than native upsert.

1. **No transactional batches**: Currently each batch is processed independently. A failed batch does not roll back previous batches.

## Related Documentation

- [Record Metadata](./record_metadata.md) - Metadata columns like `_sdc_deleted_at`
- [State Management](./state.md) - How ACTIVATE_VERSION relates to state
