# Exception Hierarchy

The Singer SDK defines a structured exception hierarchy rooted at `SingerSDKError`.
Every exception raised by the SDK is a subclass of this root, so you can always catch
all SDK-level errors with a single `except SingerSDKError` clause.

## SDK hierarchy (`singer_sdk.exceptions`)

```
SingerSDKError
├── ConfigurationError
│   ├── ConfigValidationError
│   └── MapperNotInitialized
├── DiscoveryError
│   ├── EmptySchemaTypeError
│   ├── InvalidReplicationKeyException
│   ├── SchemaNotFoundError
│   ├── SchemaNotValidError
│   └── UnsupportedSchemaFormatError  (alias: UnsupportedOpenAPISpec)
├── MappingError
│   ├── ConformedNameClashException
│   ├── MapExpressionError
│   └── StreamMapConfigError
├── SyncError
│   ├── FatalSyncError
│   │   ├── FatalAPIError
│   │   ├── InvalidStreamSortException
│   │   ├── MissingKeyPropertiesError
│   │   ├── RecordsWithoutSchemaException
│   │   ├── TapStreamConnectionFailure
│   │   └── TooManyRecordsException
│   ├── RetriableSyncError
│   │   └── RetriableAPIError
│   ├── SkippableSyncError
│   │   ├── SkippableAPIError
│   │   └── InvalidRecord
│   └── DataError
│       └── InvalidJSONSchema
└── SyncLifecycleSignal
    ├── RequestedAbortException
    │   └── MaxRecordsLimitException
    └── AbortedSyncExceptionBase  (abstract)
        ├── AbortedSyncFailedException
        └── AbortedSyncPausedException
```

All of these names can be imported from `singer_sdk.exceptions`.

## Singer-protocol hierarchy (`singer_sdk.singerlib.exceptions`)

A separate, lighter hierarchy covers exceptions that belong to the Singer *protocol*
layer — raised during message parsing before the SDK's sync machinery is involved.
These are **not** subclasses of `SingerSDKError`.

```
SingerError
└── SingerReadError
    └── InvalidInputLine
```

Import these from `singer_sdk.singerlib.exceptions`.

______________________________________________________________________

## Exception groups

### `ConfigurationError`

Raised during plugin startup when the provided configuration is invalid or missing
required values.

| Class | When raised |
|---|---|
| `ConfigValidationError` | The config dict fails JSON Schema validation. Carries `.errors` (list of messages) and `.schema` (the schema that was checked). |
| `MapperNotInitialized` | `setup_mapper()` was not called before the mapper was accessed. |

**Tap developers** — you do not normally raise these directly. The SDK raises
`ConfigValidationError` automatically when `Tap.config_jsonschema` validation fails.

______________________________________________________________________

### `DiscoveryError`

Raised during catalog discovery (the `--discover` run) before any records are synced.

| Class | When raised |
|---|---|
| `DiscoveryError` | Generic discovery failure; subclass for specific cases. |
| `EmptySchemaTypeError` | Type detection was attempted on an empty schema dict (likely a missing property definition). |
| `InvalidReplicationKeyException` | The stream's `replication_key` is not present in the stream's schema properties. |
| `SchemaNotFoundError` | A schema component could not be found in the schema source. |
| `SchemaNotValidError` | A fetched schema is not a valid JSON object. |
| `UnsupportedSchemaFormatError` | The schema source file format is not supported. Also available as `UnsupportedOpenAPISpec` for backward compatibility. |

______________________________________________________________________

### `MappingError`

Raised when a [stream map](../../stream_maps.md) configuration or expression is
invalid.

| Class | When raised |
|---|---|
| `MappingError` | Generic mapping failure; subclass for specific cases. |
| `MapExpressionError` | A Jinja/eval expression in a map definition could not be evaluated. |
| `StreamMapConfigError` | The stream map configuration itself is structurally invalid. |
| `ConformedNameClashException` | Two or more columns conform to the same output name after name normalization. |

______________________________________________________________________

### `SyncError` — base for runtime errors

All errors that occur *during* data extraction or loading inherit from `SyncError`.
They are subdivided by recovery strategy.

#### Fatal (`FatalSyncError`)

The SDK should **abort the sync immediately** and exit with a non-zero code.

| Class | When raised |
|---|---|
| `FatalAPIError` | An HTTP/API error that cannot be retried (e.g. 403 Forbidden, 400 Bad Request). |
| `TapStreamConnectionFailure` | The stream connection was lost and cannot be recovered. |
| `TooManyRecordsException` | The query returned more records than `max_records` allows. |
| `RecordsWithoutSchemaException` | A target received `RECORD` messages before the corresponding `SCHEMA` message. |
| `MissingKeyPropertiesError` | A received record is missing one or more declared key properties. |
| `InvalidStreamSortException` | Records arrived out of order, violating the stream's sort invariant. |

**Raising in tap code:**

```python
from singer_sdk.exceptions import FatalAPIError


def validate_response(self, response: requests.Response) -> None:
    if response.status_code == 403:
        msg = f"Access denied: {response.url}"
        raise FatalAPIError(msg)
    super().validate_response(response)
```

#### Retriable (`RetriableSyncError`)

The SDK should **retry the request with exponential backoff**. The sync aborts only if
all retry attempts are exhausted.

| Class | When raised |
|---|---|
| `RetriableAPIError` | A transient HTTP/API error that is safe to retry (e.g. 429 Too Many Requests, 503 Service Unavailable). Carries an optional `.response` attribute. |

**Raising in tap code:**

```python
from singer_sdk.exceptions import RetriableAPIError


def validate_response(self, response: requests.Response) -> None:
    if response.status_code == 429:
        msg = f"Rate limited: {response.url}"
        raise RetriableAPIError(msg, response=response)
    super().validate_response(response)
```

#### Skip (`SkippableSyncError`)

The SDK should **log a warning, skip the current record or page, and continue**.
The sync completes normally (exit code 0).

| Class | When raised |
|---|---|
| `SkippableAPIError` | An HTTP/API error for an expected non-fatal response (e.g. 404 on a per-record enrichment endpoint). No retry is attempted. |
| `InvalidRecord` | A record fails schema validation. Carries `.error_message` and `.record`. |

**Raising in tap code:**

```python
from singer_sdk.exceptions import SkippableAPIError


def validate_response(self, response: requests.Response) -> None:
    if response.status_code == 404:
        msg = f"Resource not found: {response.url}"
        raise SkippableAPIError(msg)
    super().validate_response(response)
```

#### Data quality (`DataError`)

Raised when a data quality or schema violation is detected. The SDK logs a warning
and continues; severity is configurable.

| Class | When raised |
|---|---|
| `InvalidJSONSchema` | A stream's declared JSON Schema is structurally invalid. |

______________________________________________________________________

### `SyncLifecycleSignal`

These are **control-flow signals**, not errors. They are raised to manage graceful
shutdown. The SDK catches them, emits a final `STATE` message where possible, and
exits cleanly.

| Class | When raised |
|---|---|
| `RequestedAbortException` | A graceful abort was requested (e.g. SIGTERM). |
| `MaxRecordsLimitException` | The `--max-records` limit was reached. |
| `AbortedSyncExceptionBase` | Abstract base; use one of the concrete subclasses below. |
| `AbortedSyncFailedException` | The sync stopped in a non-resumable state. |
| `AbortedSyncPausedException` | The sync stopped cleanly and emitted a resumable state artifact. |

______________________________________________________________________

## Catching SDK exceptions broadly

```python
from singer_sdk.exceptions import (
    SingerSDKError,
    FatalSyncError,
    RetriableSyncError,
    SkippableSyncError,
)

try:
    stream.sync()
except SkippableSyncError as e:
    logger.warning("Skipping: %s", e)
except RetriableSyncError as e:
    # handled automatically by the SDK's backoff decorator
    raise
except FatalSyncError:
    raise
except SingerSDKError:
    # catch-all for any other SDK error
    raise
```

______________________________________________________________________

## Choosing the right exception

| Situation | Raise |
|---|---|
| HTTP error, must abort | `FatalAPIError` |
| HTTP error, safe to retry | `RetriableAPIError` |
| HTTP error, expected / skip silently | `SkippableAPIError` |
| Config value is wrong | `ConfigValidationError` |
| Replication key not in schema | `InvalidReplicationKeyException` |
| Map expression fails at runtime | `MapExpressionError` |
| Record missing primary key | `MissingKeyPropertiesError` |
| Record fails schema validation | `InvalidRecord` |
