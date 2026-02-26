# Exception Hierarchy — Design

This document specifies the design of a clean, hierarchical exception taxonomy for
the Meltano Singer SDK. It covers the pre-Phase-1 state, design principles, the
implemented hierarchy, recovery semantics, and a phased implementation roadmap.

**Implementation status:** Phase 1 (hierarchy scaffold) is complete. See §7 for the
full roadmap and the status of each phase.

______________________________________________________________________

## 1. Pre-Phase-1 State

### 1.1 Flat hierarchy

Before Phase 1, all exceptions in `singer_sdk/exceptions.py` inherited directly from
the built-in `Exception` class. There was no shared base class, no grouping by domain
phase, and no shared type that SDK users could catch to handle "any SDK-level error".

### 1.2 Naming inconsistency

The codebase mixed three different naming conventions, in violation of
[PEP 8](https://peps.python.org/pep-0008/#exception-names), which requires the `Error`
suffix:

| Convention | Examples |
|---|---|
| `…Error` suffix | `ConfigValidationError`, `FatalAPIError`, `MissingKeyPropertiesError` |
| `…Exception` suffix | `InvalidReplicationKeyException`, `InvalidStreamSortException`, `RecordsWithoutSchemaException`, `ConformedNameClashException` |
| No suffix | `InvalidJSONSchema`, `InvalidRecord`, `UnsupportedOpenAPISpec`, `MapperNotInitialized` |

### 1.3 Exceptions scattered across files

Public exceptions were defined in at least five places, making it impossible to do a
single `from singer_sdk.exceptions import …` to get all of them:

| File | Exceptions defined there |
|---|---|
| `singer_sdk/exceptions.py` | `ConfigValidationError`, `DiscoveryError`, `FatalAPIError`, `RetriableAPIError`, `InvalidRecord`, `InvalidJSONSchema`, `MissingKeyPropertiesError`, `MapExpressionError`, `StreamMapConfigError`, `ConformedNameClashException`, `RequestedAbortException`, `MaxRecordsLimitException`, `AbortedSyncFailedException`, `AbortedSyncPausedException`, `RecordsWithoutSchemaException`, `TapStreamConnectionFailure`, `TooManyRecordsException`, `InvalidReplicationKeyException`, `InvalidStreamSortException` |
| `singer_sdk/singerlib/exceptions.py` | `InvalidInputLine` |
| `singer_sdk/schema/source.py` | `SchemaNotFoundError`, `SchemaNotValidError`, `UnsupportedOpenAPISpec` |
| `singer_sdk/helpers/_typing.py` | `EmptySchemaTypeError` |
| `singer_sdk/plugin_base.py` | `MapperNotInitialized` |

### 1.4 No recovery semantics encoded in the type

Because all exceptions shared the same base (`Exception`), a caller could not
distinguish between "this request should be retried", "this record should be skipped",
and "this sync must abort" without inspecting the concrete type. Recovery logic was
scattered across ad-hoc `except` blocks.

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

## 3. Implemented Hierarchy

### 3.1 Annotated tree (Phase 1)

The tree below reflects the hierarchy as implemented. Nodes marked *(Phase 2+)* are
migration candidates that will be wired in once they are moved into
`singer_sdk/exceptions.py`. Names with the `Exception` suffix are preserved from the
original codebase; `Error`-suffixed aliases may be introduced in a later phase (§7).

```
SingerSDKError                             ← base for everything SDK-specific
├── ConfigurationError                     ← invalid/missing plugin configuration
│   └── ConfigValidationError              ← JSON Schema validation failed
├── DiscoveryError                         ← schema catalog discovery
│   ├── InvalidReplicationKeyException     ← replication key not in schema properties
│   ├── SchemaNotFoundError                ← (Phase 2+, currently in schema/source.py)
│   ├── SchemaNotValidError                ← (Phase 2+, currently in schema/source.py)
│   └── UnsupportedSchemaFormatError       ← (Phase 2+, currently in schema/source.py)
├── MappingError                           ← stream map configuration/evaluation
│   ├── ConformedNameClashException        ← two columns conform to the same name
│   ├── MapExpressionError                 ← jinja/eval expression failed
│   └── StreamMapConfigError               ← invalid map config
├── SyncError                              ← runtime errors during extraction/load
│   ├── FatalSyncError                     ← abort the entire sync, non-zero exit
│   │   ├── FatalAPIError                  ← non-retriable HTTP/API error
│   │   ├── InvalidStreamSortException     ← sort invariant violated
│   │   ├── MissingKeyPropertiesError      ← record missing primary key fields
│   │   ├── RecordsWithoutSchemaException  ← target got RECORD before SCHEMA
│   │   ├── TapStreamConnectionFailure     ← stream connection lost
│   │   └── TooManyRecordsException        ← query exceeded max_records limit
│   ├── RetriableSyncError                 ← retry with exponential backoff
│   │   └── RetriableAPIError              ← retriable HTTP/API error
│   ├── IgnorableSyncError                 ← log + skip current record/page, continue
│   │   ├── IgnorableAPIError              ← expected non-fatal API response (NEW)
│   │   └── InvalidRecord                  ← record fails schema validation
│   └── DataError                          ← data quality / schema violations
│       └── InvalidJSONSchema              ← malformed JSON Schema
└── SyncLifecycleSignal                    ← control-flow signals (not "errors")
    ├── RequestedAbortException            ← graceful shutdown requested
    │   └── MaxRecordsLimitException       ← record cap hit
    └── AbortedSyncExceptionBase  (ABC)    ← abstract base; use concrete subclasses
        ├── AbortedSyncFailedException     ← stopped in non-resumable state
        └── AbortedSyncPausedException     ← stopped with resumable state artifact
```

### 3.2 Phase 2+ migration candidates

The following exceptions are defined outside `singer_sdk/exceptions.py` and are not
yet placed in the hierarchy. `InvalidInputLine` is already re-exported from
`singer_sdk/exceptions.py` (and included in `__all__`) but has not yet been moved or
re-based. The rest will be migrated in Phase 2:

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
| `AbortedSyncFailedException` | Exit non-zero | 1 |
| `AbortedSyncPausedException` | Emit STATE, exit zero | 0 |
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

### PR 1 — Hierarchy scaffold ✅ Complete

**Files:** `singer_sdk/exceptions.py`, `tests/core/test_exceptions.py`

- Added `SingerSDKError` as the new root
- Added intermediate classes: `ConfigurationError`, `MappingError`, `DiscoveryError`,
  `SyncError`, `FatalSyncError`, `RetriableSyncError`, `IgnorableSyncError`, `DataError`,
  `SyncLifecycleSignal`
- Re-wired all existing exceptions to new bases (additive only — no renames)
- Added `IgnorableAPIError(IgnorableSyncError)` (closes #1689)
- Added `__all__` to `exceptions.py`
- Added `tests/core/test_exceptions.py` with 80 hierarchy assertions
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

### PR 4 — Wire lifecycle signals, fatal handlers, and per-stream outcomes

**Files:** `singer_sdk/streams/core.py`, `singer_sdk/sinks/core.py`

- Replace bare `except Exception` or ad-hoc exception checks with typed catches on
  `FatalSyncError` / `RetriableSyncError`
- Ensure `SyncLifecycleSignal` subclasses are propagated correctly
- Implement `SyncResult` and per-stream outcome tracking (see §9)

### PR 5 — Docs, changelog, deprecations

**Files:** `docs/`, `CHANGELOG.md`, potentially `singer_sdk/exceptions.py`

- Add deprecation warnings on `Exception`-suffix names if aliases are introduced
- Update API reference
- Add changelog entry for `IgnorableAPIError`

______________________________________________________________________

## 8. Per-Stream Sync Outcomes

Exceptions govern *control flow* (raise, catch, abort now). Per-stream outcomes govern
*reporting* — recording what happened to each stream after it finishes so the process
can emit an appropriate exit code and structured log summary.

These are complementary mechanisms: an exception is caught, a recovery action is taken,
and the result of that action is written into the stream's outcome record.

### 9.1 `SyncResult` enum

```python
import enum


class SyncResult(enum.Enum):
    """The outcome of a single stream's sync run."""

    SUCCESS = "success"  # completed with no errors
    PARTIAL = "partial"  # completed; some records skipped via IgnorableSyncError
    FAILED = "failed"  # aborted due to FatalSyncError or exhausted RetriableSyncError
    ABORTED = (
        "aborted"  # stopped by SyncLifecycleSignal (e.g. MaxRecordsLimitException)
    )
```

Severity order (lowest → highest): `SUCCESS < PARTIAL < ABORTED < FAILED`.

### 9.2 How outcomes are set

Each `Stream` instance holds a `sync_result: SyncResult` attribute, initialised to
`SUCCESS` before the sync starts. The attribute is updated at the catch sites:

| Event | Outcome set |
|---|---|
| `IgnorableSyncError` caught; record skipped | `PARTIAL` (if current < `PARTIAL`) |
| `SyncLifecycleSignal` caught; sync stopped | `ABORTED` (if current < `ABORTED`) |
| `FatalSyncError` caught at top-level | `FAILED` |
| `RetriableAPIError` retries exhausted | `FAILED` |

"If current \<" means the outcome is only *escalated*, never downgraded. A stream that
already has `FAILED` cannot be reset to `PARTIAL` by a later ignorable error.

### 9.3 Child stream outcome propagation

A parent stream's final outcome is the *maximum severity* of its own outcome and all
its child streams' outcomes:

```
parent.sync_result = max(
    parent.sync_result,
    *[child.sync_result for child in parent.child_streams],
    key=lambda r: list(SyncResult).index(r),
)
```

This ensures that a parent stream is never reported as `SUCCESS` if any of its children
failed, even if the parent's own records synced cleanly.

### 9.4 Tap-level exit code

The tap's exit code is derived from the worst outcome across all top-level streams
(parents propagate children per §9.3 before this step):

| Worst outcome across all streams | Exit code |
|---|---|
| `SUCCESS` | 0 |
| `PARTIAL` | 0 |
| `ABORTED` | 0 |
| `FAILED` | 1 |

`PARTIAL` and `ABORTED` exit 0 because the state artifact emitted by those runs is
valid and resumable. `FAILED` exits 1 because the state is either absent or
untrustworthy.

### 9.5 Log summary

After all streams finish, the tap logs a one-line summary per stream at INFO level:

```
Stream 'orders'          SUCCESS   (12 345 records)
Stream 'order_items'     PARTIAL   (8 902 records, 3 skipped)
Stream 'customers'       FAILED    (FatalAPIError: 403 Forbidden)
```

The format is intentionally machine-parseable to support downstream observability
tooling.

### 9.6 Relationship to the exception hierarchy

```
Exception raised              →  caught by            →  outcome written
─────────────────────────────────────────────────────────────────────────
IgnorableSyncError            →  request loop         →  PARTIAL
SyncLifecycleSignal           →  stream.sync()        →  ABORTED
FatalSyncError                →  tap top-level        →  FAILED
RetriableAPIError (exhausted) →  backoff decorator    →  FAILED
```

This table is the normative mapping between §3 (hierarchy) and §8 (outcomes). Any
catch site that handles a `SyncError` subclass **must** also update `sync_result`.

______________________________________________________________________

## 9. Verification

After each PR:

```bash
nox -s tests          # no regressions
nox -t typing         # mypy clean
pre-commit run --all  # lint/format clean
nox -s docs           # Sphinx builds without errors
```

Confirm the annotated tree in §3.1 matches the implemented class hierarchy.
