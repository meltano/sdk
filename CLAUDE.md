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
