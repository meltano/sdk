# Exception Hierarchy Spec

This document specifies the design of a clean, hierarchical exception taxonomy for
the Meltano Singer SDK. It covers the current state, design principles, the proposed
hierarchy, recovery semantics, and a phased implementation roadmap.

______________________________________________________________________

## 1. Current State

### 1.1 Flat hierarchy

All exceptions in `singer_sdk/exceptions.py` inherit directly from the built-in
`Exception` class. There is no shared base class, no grouping by domain phase, and no
shared type that SDK users can catch to handle "any SDK-level error".

### 1.2 Naming inconsistency

The codebase mixes three different naming conventions, in violation of
[PEP 8](https://peps.python.org/pep-0008/#exception-names), which requires the `Error`
suffix:

| Convention | Examples |
|---|---|
| `…Error` suffix | `ConfigValidationError`, `FatalAPIError`, `MissingKeyPropertiesError` |
| `…Exception` suffix | `InvalidReplicationKeyException`, `InvalidStreamSortException`, `RecordsWithoutSchemaException`, `ConformedNameClashException` |
| No suffix | `InvalidJSONSchema`, `InvalidRecord`, `UnsupportedOpenAPISpec`, `MapperNotInitialized` |

### 1.3 Exceptions scattered across files

Public exceptions are defined in at least five places, making it impossible to do a
single `from singer_sdk.exceptions import …` to get all of them:

| File | Exceptions defined there |
|---|---|
| `singer_sdk/exceptions.py` | `ConfigValidationError`, `DiscoveryError`, `FatalAPIError`, `RetriableAPIError`, `InvalidRecord`, `InvalidJSONSchema`, `MissingKeyPropertiesError`, `MapExpressionError`, `StreamMapConfigError`, `ConformedNameClashException`, `RequestedAbortException`, `MaxRecordsLimitException`, `AbortedSyncFailedException`, `AbortedSyncPausedException`, `RecordsWithoutSchemaException`, `TapStreamConnectionFailure`, `TooManyRecordsException`, `InvalidReplicationKeyException`, `InvalidStreamSortException` |
| `singer_sdk/singerlib/exceptions.py` | `InvalidInputLine` |
| `singer_sdk/schema/source.py` | `SchemaNotFoundError`, `SchemaNotValidError`, `UnsupportedOpenAPISpec` |
| `singer_sdk/helpers/_typing.py` | `EmptySchemaTypeError` |
| `singer_sdk/plugin_base.py` | `MapperNotInitialized` |

### 1.4 No recovery semantics encoded in the type

Because all exceptions share the same base (`Exception`), a caller cannot distinguish
between "this request should be retried", "this record should be skipped", and "this
sync must abort" without inspecting the concrete type. Recovery logic is scattered
across ad-hoc `except` blocks.

______________________________________________________________________

## 2. Design Principles

### 2.1 Encode intent in the type

Each exception carries a recoverable *intent*: what should the SDK do when it catches
this? The hierarchy groups exceptions by recovery strategy first, domain phase second.

### 2.2 Group by domain phase *and* recovery strategy

Two orthogonal axes inform the hierarchy:

- **Domain phase**: configuration → discovery → mapping → sync
- **Recovery strategy**: fatal (abort) / retriable (backoff) / ignorable (skip)

### 2.3 Consistent `Error` suffix (PEP 8)

All new exception names end in `Error`. Existing names with the `Exception` suffix are
preserved unchanged in Phase 1 (backward compatibility — see §6); they may be given
`Error`-suffixed aliases in a later phase.

### 2.4 Single source of truth

All *public* SDK exceptions live in `singer_sdk/exceptions.py`. File-local exceptions
(e.g. `UnsupportedOpenAPISpec`) should be migrated there over time.

### 2.5 Only handle what you can act on

The SDK catches an exception only when it can take a meaningful, policy-defined action
(retry, skip, abort). Unknown exceptions are allowed to propagate.

______________________________________________________________________

## 3. Proposed Hierarchy

### 3.1 Annotated tree

```
SingerSDKError                          ← base for everything SDK-specific
├── ConfigurationError                  ← invalid/missing plugin configuration
│   └── ConfigValidationError           ← JSON Schema validation failed (existing)
├── DiscoveryError                      ← schema catalog discovery (existing)
│   ├── SchemaNotFoundError             ← component/path not found in source (existing)
│   ├── SchemaNotValidError             ← schema is not a JSON object (existing)
│   └── UnsupportedSchemaFormatError    ← file type / spec version not supported
├── MappingError                        ← stream map configuration/evaluation
│   ├── MapExpressionError              ← jinja/eval expression failed (existing)
│   ├── StreamMapConfigError            ← invalid map config (existing)
│   └── ConformedNameClashError         ← two columns conform to the same name (existing)
├── SyncError                           ← runtime errors during extraction/load
│   ├── FatalSyncError                  ← abort the entire sync, non-zero exit
│   │   ├── FatalAPIError               ← non-retriable HTTP/API error (existing)
│   │   ├── RecordsWithoutSchemaError   ← target got RECORD before SCHEMA (existing)
│   │   ├── MissingKeyPropertiesError   ← record missing primary key fields (existing)
│   │   └── InvalidStreamSortError      ← sort invariant violated (existing)
│   ├── RetriableSyncError              ← retry with exponential backoff
│   │   └── RetriableAPIError           ← retriable HTTP/API error (existing)
│   ├── IgnorableSyncError              ← log + skip current record/page, continue
│   │   ├── IgnorableAPIError           ← NEW (closes #1689)
│   │   └── InvalidRecord               ← record fails schema validation (existing)
│   └── DataError                       ← data quality / schema violations
│       └── InvalidJSONSchema           ← malformed JSON Schema (existing)
└── SyncLifecycleSignal                 ← control-flow signals (not "errors")
    ├── AbortRequested                  ← graceful shutdown requested (existing)
    │   └── MaxRecordsLimitReached      ← record cap hit (existing)
    ├── SyncAbortedFatally              ← stopped in non-resumable state (existing)
    └── SyncAbortedPaused               ← stopped in paused/resumable state (existing)
```

### 3.2 Exceptions not yet in the hierarchy (migration candidates)

The following exceptions are defined outside `singer_sdk/exceptions.py` and are not
yet placed in the hierarchy. They will be migrated in Phase 2:

| Current location | Class | Proposed placement |
|---|---|---|
| `singerlib/exceptions.py` | `InvalidInputLine` | `SyncError` → `FatalSyncError` |
| `schema/source.py` | `UnsupportedOpenAPISpec` | `DiscoveryError` → `UnsupportedSchemaFormatError` |
| `helpers/_typing.py` | `EmptySchemaTypeError` | `DiscoveryError` (schema introspection) |
| `plugin_base.py` | `MapperNotInitialized` | `ConfigurationError` |

______________________________________________________________________

## 4. Recovery Strategy Table

| Exception category | SDK action | Exit code |
|---|---|---|
| `FatalSyncError` | Log error, abort sync, exit non-zero | 1 |
| `RetriableSyncError` | Exponential backoff + retry; abort after max retries | 1 (if exhausted) |
| `IgnorableSyncError` | Log at WARNING, skip current record/page, continue sync | 0 |
| `DataError` | Log at WARNING, continue (severity configurable) | 0 |
| `SyncAbortedFatally` | Exit non-zero | 1 |
| `SyncAbortedPaused` | Emit STATE, exit zero | 0 |
| `ConfigurationError` | Print error, exit non-zero at startup | 1 |
| `DiscoveryError` | Print error, exit non-zero during catalog discovery | 1 |

______________________________________________________________________

## 5. `IgnorableAPIError` Specification (closes #1689)

### 5.1 Motivation

Tap developers frequently encounter HTTP responses that are expected to return no data
for a specific request (e.g. 404 on a per-record enrichment endpoint, 204 on an
empty page). Today they must subclass `FatalAPIError` or suppress the exception
themselves. Issue #1689 requests a first-class SDK exception for this pattern.

### 5.2 Class definition

```python
class IgnorableAPIError(IgnorableSyncError):
    """Raised when a failed API request should be silently skipped.

    Raise this in ``validate_response()`` to indicate that the current HTTP
    request produced an expected non-fatal error. The SDK will:

    1. Log the exception message at WARNING level.
    2. Skip the current request (no records emitted for it).
    3. Continue the sync with the next request.

    No retry will be attempted. Use ``RetriableAPIError`` for transient
    failures that should be retried.

    Example::

        def validate_response(self, response: requests.Response) -> None:
            if response.status_code == 404:
                msg = f"Resource not found: {response.url}"
                raise IgnorableAPIError(msg)
            super().validate_response(response)
    """
```

### 5.3 Catch site

`IgnorableAPIError` is caught in `RESTStream._request_with_backoff()` (or the
outermost request loop), at the same level as `RetriableAPIError`. The catch block:

1. Calls `self.logger.warning("Ignoring API error: %s", exc)`
1. Returns an empty iterable / `None` for the current page
1. Does **not** update state or emit records for the skipped request
1. Allows the caller to proceed to the next request/page

### 5.4 Interaction with backoff

`IgnorableAPIError` must **not** trigger the backoff decorator. It is raised *after*
backoff has already decided to give up (or from `validate_response()` before backoff
is invoked). The backoff decorator is configured to re-raise on `IgnorableSyncError`
and its subclasses, not to swallow them.

______________________________________________________________________

## 6. Migration / Backward Compatibility

**Decision: hierarchy only — no renames in Phase 1.**

All existing exception names are kept exactly as-is. The change is purely additive:
new intermediate base classes are inserted above existing leaf classes. Any code that
currently catches `FatalAPIError` will continue to work unchanged, because
`FatalAPIError` still exists and is still raised.

### 6.1 Additive insertion pattern

```python
# ── Before (current state) ────────────────────────────────────────
class FatalAPIError(Exception): ...


class RetriableAPIError(Exception): ...


class ConfigValidationError(Exception): ...


# ── After Phase 1 (hierarchy inserted, names unchanged) ───────────
class SingerSDKError(Exception): ...  # new


class SyncError(SingerSDKError): ...  # new


class FatalSyncError(SyncError): ...  # new


class RetriableSyncError(SyncError): ...  # new


class IgnorableSyncError(SyncError): ...  # new


class IgnorableAPIError(IgnorableSyncError): ...  # new (closes #1689)


class ConfigurationError(SingerSDKError): ...  # new


class FatalAPIError(FatalSyncError): ...  # was Exception → now FatalSyncError


class RetriableAPIError(
    RetriableSyncError
): ...  # was Exception → now RetriableSyncError


class ConfigValidationError(
    ConfigurationError
): ...  # was Exception → now ConfigurationError
```

### 6.2 What changes for SDK users

| Code pattern | Still works after Phase 1? |
|---|---|
| `except FatalAPIError` | Yes — same concrete type |
| `except RetriableAPIError` | Yes — same concrete type |
| `except ConfigValidationError` | Yes — same concrete type |
| `except SingerSDKError` | New — catches any SDK exception |
| `except FatalSyncError` | New — catches all fatal sync errors |
| `except IgnorableSyncError` | New — catches `IgnorableAPIError`, `InvalidRecord` |
| `raise FatalAPIError(...)` | Yes — unchanged |

Zero breaking changes are introduced by the hierarchy-only insertion.

______________________________________________________________________

## 7. Implementation Roadmap

### PR 1 — Hierarchy scaffold (no behavior change)

**Files:** `singer_sdk/exceptions.py`

- Add `SingerSDKError` as the new root
- Add intermediate classes: `ConfigurationError`, `SyncError`, `FatalSyncError`,
  `RetriableSyncError`, `IgnorableSyncError`
- Re-wire existing exceptions to new bases (additive only)
- Add `IgnorableAPIError(IgnorableSyncError)` (closes #1689)
- Update `__all__` in `exceptions.py`
- No behavior changes; all existing tests pass unchanged

### PR 2 — Consolidate scattered exceptions

**Files:** `singer_sdk/exceptions.py`, `singerlib/exceptions.py`,
`schema/source.py`, `helpers/_typing.py`, `plugin_base.py`

- Move `InvalidInputLine`, `UnsupportedOpenAPISpec`, `EmptySchemaTypeError`,
  `MapperNotInitialized` into `singer_sdk/exceptions.py`
- Keep re-exports in original files for one release cycle (with `# noqa: F401`)
- Wire migrated exceptions into the hierarchy

### PR 3 — `IgnorableAPIError` handling in REST stream

**Files:** `singer_sdk/streams/rest.py`

- Catch `IgnorableSyncError` in the request loop / `_request_with_backoff()`
- Log at WARNING, return empty page, continue
- Add unit tests for `IgnorableAPIError` in `validate_response()`

### PR 4 — Wire lifecycle signals and fatal handlers

**Files:** `singer_sdk/streams/core.py`, `singer_sdk/sinks/core.py`

- Replace bare `except Exception` or ad-hoc exception checks with typed catches on
  `FatalSyncError` / `RetriableSyncError`
- Ensure `SyncLifecycleSignal` subclasses are propagated correctly

### PR 5 — Docs, changelog, deprecations

**Files:** `docs/`, `CHANGELOG.md`, potentially `singer_sdk/exceptions.py`

- Add deprecation warnings on `Exception`-suffix names if aliases are introduced
- Update API reference
- Add changelog entry for `IgnorableAPIError`

______________________________________________________________________

## 8. Verification

After each PR:

```bash
nox -s tests          # no regressions
nox -t typing         # mypy clean
pre-commit run --all  # lint/format clean
nox -s docs           # Sphinx builds without errors
```

Confirm the annotated tree in §3.1 matches the implemented class hierarchy.
