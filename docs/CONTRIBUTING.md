# Contributing to the SDK

## Local Developer Setup

1. If you are using VS Code, make sure you have also installed the `Python` extension.
2. Ensure you have the correct test library, formatters, and linters installed:
    - `poetry install`
3. Configure Linting and Formatting Settings:
    - We use `pytest` for testing (`poetry run pytest`).
    - We use `black`, `flake8`, `mypy`, and `pydocstyle` as CI linting tests.
    - We use `coverage` for code coverage metrics.
    - The project-wide max line length is `89`.
    - In the future we will add support for linting
      [pre-commit hooks](https://gitlab.com/meltano/singer-sdk/-/issues/12) as well.
4. Set interpreter to match poetry's virtualenv:
    - In VS Code, run `Python: Select interpreter` and select the poetry interpreter.

## Testing Locally

To run tests and gather coverage metrics:

```bash
poetry run pytest
```

To run tests while gathering coverage metrics:

```bash
poetry run coverage run -m pytest
```

To view the code coverage report:

```bash
# CLI output
poetry run coverage report

# Or html output:
poetry run coverage html && open ./htmlcov/index.html
```

To run all tests:

```bash
poetry run tox
```


## Workspace Development Strategies for Singer SDK

### Universal Code Formatting

- From the [Black](https://black.readthedocs.io) website:
    > By using Black, you agree to cede control over minutiae of hand-formatting. In return, Black gives you speed, determinism, and freedom from pycodestyle nagging about formatting. You will save time and mental energy for more important matters. **Black makes code review faster by producing the smallest diffs possible.** Blackened code looks the same regardless of the project you’re reading. **Formatting becomes transparent after a while and you can focus on the content instead.**

### Pervasive Python Type Hints

Type hints allow us to spend less time reading documentation.
