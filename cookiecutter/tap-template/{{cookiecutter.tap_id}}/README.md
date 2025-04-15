# {{ cookiecutter.tap_id }}

`{{ cookiecutter.tap_id }}` is a Singer tap for {{ cookiecutter.source_name }}.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPI repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPI:

```bash
pipx install {{ cookiecutter.tap_id }}
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/{{ cookiecutter.tap_id }}.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
{{ cookiecutter.tap_id }} --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
tap is available by running:

```bash
{{ cookiecutter.tap_id }} --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `{{ cookiecutter.tap_id }}` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
{{ cookiecutter.tap_id }} --version
{{ cookiecutter.tap_id }} --help
{{ cookiecutter.tap_id }} --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
uv run pytest
```

You can also test the `{{cookiecutter.tap_id}}` CLI interface directly using `uv run`:

```bash
uv run {{cookiecutter.tap_id}} --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd {{ cookiecutter.tap_id }}
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke {{ cookiecutter.tap_id }} --version

# OR run a test ELT pipeline:
meltano run {{ cookiecutter.tap_id }} target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
