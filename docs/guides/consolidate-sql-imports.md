# Consolidating SQL imports into `singer_sdk.sql`

In v0.57 the deprecated SQL import shim modules will be removed. All SQL classes are
now consolidated in the `singer_sdk.sql` package and its `connector` submodule. This guide covers
every affected import path.

## Module-level import paths

The following modules are shims that re-export from `singer_sdk.sql`. They will be
deleted in v0.58.

| Deprecated import | Replacement |
|---|---|
| `from singer_sdk.connectors.sql import SQLConnector` | `from singer_sdk.sql import SQLConnector` |
| `from singer_sdk.connectors.sql import FullyQualifiedName` | `from singer_sdk.sql.connector import FullyQualifiedName` |
| `from singer_sdk.connectors.sql import JSONSchemaToSQL` | `from singer_sdk.sql.connector import JSONSchemaToSQL` |
| `from singer_sdk.connectors.sql import JSONtoSQLHandler` | `from singer_sdk.sql.connector import JSONtoSQLHandler` |
| `from singer_sdk.connectors.sql import SQLToJSONSchema` | `from singer_sdk.sql.connector import SQLToJSONSchema` |
| `from singer_sdk.sinks.sql import SQLSink` | `from singer_sdk.sql import SQLSink` |
| `from singer_sdk.streams.sql import SQLStream` | `from singer_sdk.sql import SQLStream` |

## Top-level `singer_sdk` imports

The top-level `singer_sdk` package no longer re-exports the SQL classes.

| Deprecated import | Replacement |
|---|---|
| `from singer_sdk import SQLConnector` | `from singer_sdk.sql import SQLConnector` |
| `from singer_sdk import SQLSink` | `from singer_sdk.sql import SQLSink` |
| `from singer_sdk import SQLStream` | `from singer_sdk.sql import SQLStream` |
| `from singer_sdk import SQLTap` | `from singer_sdk.sql import SQLTap` |
| `from singer_sdk import SQLTarget` | `from singer_sdk.sql import SQLTarget` |

## Typical tap migration

```python
# Old (deprecated)
from singer_sdk import SQLConnector, SQLStream, SQLTap


class MyConnector(SQLConnector): ...


class MyStream(SQLStream): ...


class MyTap(SQLTap): ...


# New
from singer_sdk.sql import SQLConnector, SQLStream, SQLTap


class MyConnector(SQLConnector): ...


class MyStream(SQLStream): ...


class MyTap(SQLTap): ...
```

## Typical target migration

```python
# Old (deprecated)
from singer_sdk import SQLConnector, SQLSink, SQLTarget


class MyConnector(SQLConnector): ...


class MySink(SQLSink): ...


class MyTarget(SQLTarget): ...


# New
from singer_sdk.sql import SQLConnector, SQLSink, SQLTarget


class MyConnector(SQLConnector): ...


class MySink(SQLSink): ...


class MyTarget(SQLTarget): ...
```

## Custom type converters

`SQLToJSONSchema` and `JSONSchemaToSQL` moved to `singer_sdk.sql.connector`:

```python
# Old (deprecated)
from singer_sdk.connectors.sql import SQLToJSONSchema, JSONSchemaToSQL


class MyConverter(SQLToJSONSchema): ...


# New
from singer_sdk.sql.connector import SQLToJSONSchema, JSONSchemaToSQL


class MyConverter(SQLToJSONSchema): ...
```

`FullyQualifiedName` follows the same pattern:

```python
# Old (deprecated)
from singer_sdk.connectors.sql import FullyQualifiedName

# New
from singer_sdk.sql.connector import FullyQualifiedName
```

## Type annotations in `TYPE_CHECKING` blocks

The pattern is the same — just update the import path:

```python
from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    # Old (deprecated)
    from singer_sdk.connectors.sql import FullyQualifiedName

    # New
    from singer_sdk.sql.connector import FullyQualifiedName
```
