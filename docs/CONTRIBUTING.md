# Contributing to the SDK

_**Note:** The SDK currently works with Python versions 3.6 through 3.8.x. Python 3.9 is not yet supported._

## Setting up Prereqs

If poetry and pipx are not already installed:

```bash
pip3 install pipx
pipx ensurepath
pipx install poetry
```

Now you can use Poetry to install package dependencies:

```bash
cd singer-sdk
```

```bash
# Install package and dependencies:
poetry install
# OR install in editable mode:
poetry install --no-root
```

## Local Developer Setup

First clone, then...

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

## Testing Updates to Docs

Documentation runs on Sphinx, a using ReadtheDocs style template, and hosting from
ReadtheDocs.org. When a push is detected by readthedocs.org, they automatically rebuild
and republish the docs. ReadtheDocs is also version aware, so it retains prior and unreleased
versions of the docs for us.

First, make sure your virtual env has all the right tools and versions:

```bash
poetry install
```

To build the docs:

```bash
cd docs
# Build docs
poetry run make html
# Open in the local browser:
open _build/html/index.html
```

To build missing stubs:

```bash
cd docs
poetry run sphinx-autogen -o classes *.rst
```

## Workspace Development Strategies for the SDK

### Universal Code Formatting

- From the [Black](https://black.readthedocs.io) website:
    > By using Black, you agree to cede control over minutiae of hand-formatting. In return, Black gives you speed, determinism, and freedom from pycodestyle nagging about formatting. You will save time and mental energy for more important matters. **Black makes code review faster by producing the smallest diffs possible.** Blackened code looks the same regardless of the project you’re reading. **Formatting becomes transparent after a while and you can focus on the content instead.**

### Pervasive Python Type Hints

Type hints allow us to spend less time reading documentation.

### What is Poetry and why do we need it?

For more info on `Poetry` and `Pipx`, please see the topic in our
[python tips](python_tips.md) guide.
