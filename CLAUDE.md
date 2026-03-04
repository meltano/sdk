# CLAUDE.md

This is the Meltano Singer SDK, a Python framework for building Singer taps (data extractors) and targets (data loaders).

## Development Setup

```bash
uv sync --all-groups --all-extras --all-packages  # Full development environment
```

## Common Commands

### Testing

```bash
nox -s tests              # Run core tests
nox -s test-contrib       # Run contrib (experimental) tests
nox -s test-packages      # Run package integration tests
nox -t typing             # Type checking with mypy
```

### Linting & Formatting

```bash
pre-commit run --all      # Run all pre-commit hooks
ruff check --fix          # Lint and auto-fix
ruff format               # Format code
```

### Documentation

```bash
nox -s docs               # Build Sphinx documentation
```

## Code Style

- Required: `from __future__ import annotations` at top of every file
- Use `typing as t` for type imports (abbreviated import per ruff config)
- Google-style docstrings with full parameter documentation
- Line length: 88 characters
- Ruff handles all formatting and linting

## Exceptions

All SDK exceptions live in `singer_sdk/exceptions.py` and inherit from `SingerSDKError`.
Choose the right base class by recovery strategy:

| Situation | Raise |
|---|---|
| HTTP/API error, must abort sync | `FatalAPIError` |
| HTTP/API error, safe to retry | `RetriableAPIError` |
| HTTP/API error, expected / skip silently | `IgnorableAPIError` |
| Config value is wrong | `ConfigValidationError` |
| Discovery / catalog problem | `DiscoveryError` (or a subclass) |
| Stream map config or expression fails | `MappingError` (or a subclass) |

When adding a new exception:

1. Place it in `singer_sdk/exceptions.py` — never define public exceptions in other files.
1. Inherit from the appropriate intermediate base (`FatalSyncError`, `RetriableSyncError`,
   `IgnorableSyncError`, `DataError`, `ConfigurationError`, `MappingError`, etc.) rather
   than from `Exception` or `SingerSDKError` directly.
1. Add it to `__all__` in that file.
1. Add `issubclass` assertions to `tests/core/test_exceptions.py`.

See `docs/implementation/errors/hierarchy.md` for the full hierarchy and
`docs/implementation/errors/design.md` for the design rationale.

## Deprecation Warnings

Three warning classes in `singer_sdk/helpers/_compat.py`:

| Class | Base | When to use |
|---|---|---|
| `SingerSDKDeprecationWarning` | `DeprecationWarning` | Removal version is known |
| `SingerSDKPendingDeprecationWarning` | `PendingDeprecationWarning` | No committed removal timeline; silenced by default |
| `SingerSDKPythonEOLWarning` | `FutureWarning` | Python version nearing/past EOL |

Use `singer_sdk_deprecated(msg, *, removal_version, stacklevel=1)` as a decorator factory for deprecating classes and functions. `removal_version` is **required** — it is appended to the message automatically. Never embed the version in the message string manually.

```python
@singer_sdk_deprecated(
    "Foo is deprecated. Use Bar instead.",
    removal_version="v0.58",
)
class Foo: ...
```

For `warnings.warn` call sites with no committed removal timeline, use `SingerSDKPendingDeprecationWarning` directly:

```python
warnings.warn("...", SingerSDKPendingDeprecationWarning, stacklevel=2)
```

**Deprecation policy**: at least 3 months / 3 feature releases notice, named removal version required. See `docs/release_process.md` and `docs/deprecation.md`.

## Architecture

The SDK uses abstract base classes for its plugin system:

- `Tap` / `Target` - Main plugin entry points
- `Stream` / `RESTStream` / `GraphQLStream` / `SQLStream` - Data extraction
- `Sink` / `BatchSink` / `SQLSink` - Data loading
- `SQLConnector` - Database connections

## Testing

- pytest-based with custom pytest plugin (`singer_testing`)
- Test markers: `contrib`, `external`, `packages`, `snapshot`
- Platform markers: `@pytest.mark.darwin`, `@pytest.mark.linux`, `@pytest.mark.windows`
- Standard test suites: `singer_sdk.testing.get_standard_tap_tests()` / `get_standard_target_tests()`

## Project Structure

- `singer_sdk/` - Main SDK package
- `tests/` - Test suite (core, contrib, external, packages)
- `packages/` - Reference implementations for E2E testing
- `cookiecutter/` - Templates for scaffolding new taps/targets
- `docs/` - Sphinx documentation

## Python Version

Supports Python 3.10-3.14. Primary development version is 3.14.
