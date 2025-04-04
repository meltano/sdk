# Contributing Guide

_**Note:** The SDK currently works with Python versions 3.8 through 3.12.x._

Let's build together! Please see our [Contributor Guide](https://docs.meltano.com/contribute/)
for more information on contributing to Meltano.

We believe that everyone can contribute and we welcome all contributions.
If you're not sure what to work on, here are some [ideas to get you started](https://github.com/meltano/sdk/labels/accepting%20pull%20requests).

Chat with us in [#contributing](https://meltano.slack.com/archives/C013Z450LCD) on [Slack](https://meltano.com/slack).

Contributors are expected to follow our [Code of Conduct](https://docs.meltano.com/contribute/#code-of-conduct).

## Setting up Prereqs

Make sure [`uv`](https://docs.astral.sh/uv/),
[`pre-commit`](https://pre-commit.com/) and [`nox`](https://nox.thea.codes/en/stable/)
are installed. Once you have installed `uv`, you can use it to install other tools:

```bash
uv tool install pre-commit
uv tool install nox
```

Now you can use `uv` to install package dependencies:

```bash
cd sdk
```

```bash
# Install package and dependencies:
uv sync --all-groups --all-extras
```

## Local Developer Setup

First clone, then...

1. Ensure you have the correct test library, formatters, and linters installed:
    - `uv sync --all-groups --all-extras`
1. The project has `pre-commit` hooks. Install them with:
    - `pre-commit install`
1. Most development tasks you might need should be covered by `nox` sessions. You can use `nox -l` to list all available tasks.
For example:

    - Run unit tests: `nox -r`.

      We use `coverage` for code coverage metrics.

    - Run pre-commit hooks: `pre-commit run --all`.

      We use [Ruff](https://github.com/charliermarsh/ruff),
      [black](https://black.readthedocs.io/en/stable/index.html),
      [flake8](https://flake8.pycqa.org/en/latest/) and
      [mypy](https://mypy.readthedocs.io/en/stable/).
      The project-wide max line length is `88`.

    - Build documentation: `nox -rs docs`

      We use `sphinx` to build documentation.

### If you are using VSCode

1. Make sure you have also installed the `Python` extension.
1. Set interpreter to match uv's managed virtualenv: run
   `Python: Select interpreter` and select the interpreter.
1. The [pre-commit extension](https://marketplace.visualstudio.com/items?itemName=MarkLarah.pre-commit-vscode)
will allow to run pre-commit hooks on the current file from the VSCode command palette.

## Testing Locally

To run tests:

```bash
# Run just the core and cookiecutter tests (no external creds required):
nox -rs tests

# Run all tests (external creds required):
nox -rs tests -- -m "external"
```

To view the code coverage report in HTML format:

```bash
nox -rs coverage -- html && open ./htmlcov/index.html
```

### Platform-specific Testing

To mark a test as platform-specific, use the `@pytest.mark.<platform>` decorator:

```python
import pytest

@pytest.mark.windows
def test_windows_only():
    pass
```

Supported platform markers are `windows`, `darwin`, and `linux`.

### Snapshot Testing

We use [pytest-snapshot](https://pypi.org/project/pytest-snapshot/) for snapshot testing.

#### Adding a new snapshot

To add a new snapshot, use the `snapshot` fixture and mark the test with the
`@pytest.mark.snapshot` decorator. The fixture will create a new snapshot file
if one does not already exist. If a snapshot file already exists, the fixture
will compare the snapshot to the actual value and fail the test if they do not
match.

The `tests/snapshots` directory is where snapshot files should be stored and
it's available as the `snapshot_dir` fixture.

```python
@pytest.mark.snapshot
def test_snapshot(snapshot, snapshot_dir):
    # Configure the snapshot directory
    snapshot.snapshot_dir = snapshot_dir.joinpath("test_snapshot_subdir")

    snapshot_name = "test_snapshot"
    expected_content = "Hello, World!"
    snapshot.assert_match(expected_content, snapshot_name)
```

#### Generating or updating snapshots

To update or generate snapshots, run the nox `update_snapshots` session

```bash
nox -rs snap
```

or use the `--snapshot-update` flag

```bash
uv run pytest --snapshot-update -m 'snapshot'
```

This will run all tests with the `snapshot` marker and update any snapshots that have changed.
Commit the updated snapshots to your branch if they are expected to change.

## Testing Updates to Docs

Documentation runs on Sphinx, using ReadtheDocs style template, and hosting from
ReadtheDocs.org. When a push is detected by readthedocs.org, they automatically rebuild
and republish the docs. ReadtheDocs is also version aware, so it retains prior and unreleased
versions of the docs for us.

To build the docs and live-reload them locally:

```bash
nox -rs docs-serve
```

Sphinx will automatically generate class stubs, so be sure to `git add` them.

## Semantic Pull Requests

This repo uses the [semantic-prs](https://github.com/Ezard/semantic-prs) GitHub app to check all PRs against the conventional commit syntax.

Pull requests should be named according to the conventional commit syntax to streamline changelog and release notes management. We encourage (but do not require) the use of conventional commits in commit messages as well.

In general, PR titles should follow the format `<type>: <desc>`, where type is any one of these:

- `ci`
- `chore`
- `build`
- `docs`
- `feat`
- `fix`
- `packaging`
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

### What is uv and why do we need it?

For more info on `uv`, please see the topic in our
[python tips](./python_tips.md) guide.
