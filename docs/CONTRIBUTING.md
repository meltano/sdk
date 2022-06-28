# Contributing to the SDK

_**Note:** The SDK currently works with Python versions 3.7 through 3.10.x. Python 3.6 is no longer supported._

Let's build together! Please see our [Contributor Guide](https://docs.meltano.com/contribute/)
for more information on contributing to Meltano.

We believe that everyone can contribute and we welcome all contributions.
If you're not sure what to work on, here are some [ideas to get you started](https://github.com/meltano/sdk/labels/accepting%20pull%20requests).

Chat with us in [#contributing](https://meltano.slack.com/archives/C013Z450LCD) on [Slack](https://meltano.com/slack).

Contributors are expected to follow our [Code of Conduct](https://docs.meltano.com/contribute/#code-of-conduct).

## Setting up Prereqs

Make sure [`poetry`](https://python-poetry.org/docs/),
[`pre-commit`](https://pre-commit.com/) and [`tox`](https://tox.wiki/en/latest/)
are installed. You can use [`pipx`](https://pypa.github.io/pipx/) to install
all of them. To install `pipx`:

```bash
pip3 install pipx
pipx ensurepath
```

With `pipx` installed, you globally add the required tools:

```bash
pipx install poetry
pipx install pre-commit
pipx install tox
```

Now you can use Poetry to install package dependencies:

```bash
cd sdk
```

```bash
# Install package and dependencies:
poetry install
# OR install in editable mode:
poetry install --no-root
```

## Local Developer Setup

First clone, then...

1. Ensure you have the correct test library, formatters, and linters installed:
    - `poetry install`
1. If you are going to update documentation, install the `docs` extras:
    - `poetry install -E docs`
1. The project has `pre-commit` hooks. Install them with:
    - `pre-commit install`
1. Most development tasks you might need should be covered by `tox` environments. You can use `tox -l` to list all available tasks.
For example:

    - Run unit tests: `tox -e py`.

      We use `coverage` for code coverage metrics.

    - Run pre-commit hooks: `tox -e pre-commit`.

      We use `black`, `flake8`, `isort`, `mypy` and `pyupgrade`. The project-wide max line length is `88`.

    - Build documentation: `tox -e docs`

      We use `sphinx` to build documentation.

### If you are using VSCode

1. Make sure you have also installed the `Python` extension.
1. Set interpreter to match poetry's virtualenv: run
   `Python: Select interpreter` and select the poetry interpreter.
1. The [pre-commit extension](https://marketplace.visualstudio.com/items?itemName=MarkLarah.pre-commit-vscode)
will allow to run pre-commit hooks on the current file from the VSCode command palette.

## Testing Locally

To run tests:

```bash
# Run just the core and cookiecutter tests (no external creds required):
tox -e py

# Run all tests (external creds required):
tox -e py -- -m "external"
```

To gather and display coverage metrics:

```bash
tox -e combine_coverage
```

To view the code coverage report in HTML format:

```bash
tox -e coverage_artifact -- html && open ./htmlcov/index.html
```

## Testing Updates to Docs

Documentation runs on Sphinx, using ReadtheDocs style template, and hosting from
ReadtheDocs.org. When a push is detected by readthedocs.org, they automatically rebuild
and republish the docs. ReadtheDocs is also version aware, so it retains prior and unreleased
versions of the docs for us.

To build the docs:

```bash
# Build docs
tox -e docs

# Open in the local browser:
open build/index.html
```

Sphinx will automatically generate class stubs, so be sure to `git add` them.

## Semantic Pull Requests

This repo uses the [semantic-prs](https://github.com/Ezard/semantic-prs) GitHub app to check all PRs againts the conventional commit syntax.

Pull requests should be named according to the conventional commit syntax to streamline changelog and release notes management. We encourage (but do not require) the use of conventional commits in commit messages as well.

In general, PR titles should follow the format "<type>: <desc>", where type is any one of these:

- `ci`
- `chore`
- `build`
- `docs`
- `feat`
- `fix`
- `perf`
- `refactor`
- `revert`
- `style`
- `test`

Optionally, you may use the expanded syntax to specify a scope in the form `<type>(<scope>): <desc>`. Currently scopes are:

 scopes:
  - `taps`       # tap SDK only
  - `targets`    # target SDK only
  - `mappers`    # mappers only
  - `templates`  # cookiecutters

More advanced rules and settings can be found within the file [`.github/semantic.yml`](https://github.com/meltano/sdk/blob/main/.github/semantic.yml).

## Workspace Development Strategies for the SDK

### Universal Code Formatting

- From the [Black](https://black.readthedocs.io) website:
    > By using Black, you agree to cede control over minutiae of hand-formatting. In return, Black gives you speed, determinism, and freedom from pycodestyle nagging about formatting. You will save time and mental energy for more important matters. **Black makes code review faster by producing the smallest diffs possible.** Blackened code looks the same regardless of the project you’re reading. **Formatting becomes transparent after a while and you can focus on the content instead.**

### Pervasive Python Type Hints

Type hints allow us to spend less time reading documentation. Public modules are checked in CI for type annotations on all methods and functions.

### Docstring convention

All public modules in the SDK are checked for the presence of docstrings in classes and functions. We follow the [Google Style convention](https://www.sphinx-doc.org/en/master/usage/extensions/example_google.html) for Python docstrings so functions are required to have a description of every argument and the return value, if applicable.


### What is Poetry and why do we need it?

For more info on `Poetry` and `Pipx`, please see the topic in our
[python tips](python_tips.md) guide.
