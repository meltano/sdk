# Migrating a plugin from Poetry to uv

This guide will help you migrate your Singer plugin from Poetry to uv.

## Prerequisites

- [uv](https://docs.astral.sh/uv/)

## Steps

1. Use the [migrate-to-uv](https://github.com/mkniewallner/migrate-to-uv) tool to migrate your project from Poetry to uv. Run the following command in your project directory:

```bash
uvx migrate-to-uv
```

2. Update the `build-system` section in your `pyproject.toml` file to use `hatchling`:

```toml
[build-system]
build-backend = "hatchling.build"
requires = ["hatchling>=1,<2"]
```

3. Update your CI/CD to use `uv` commands instead of `poetry` commands. For example, replace `poetry install` with `uv sync`. You may also want to use the [official `setup-uv`](https://github.com/astral-sh/setup-uv/) GitHub Action to install `uv` in your CI/CD workflow.

## Example

For an example of a migration to `uv` for a Singer tap, see [how it was done for tap-stackexchange](https://github.com/MeltanoLabs/tap-stackexchange/pull/507).
