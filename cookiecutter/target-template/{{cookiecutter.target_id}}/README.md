# {{ cookiecutter.target_id }}

`{{ cookiecutter.target_id }}` is a Singer target for {{ cookiecutter.destination_name }}.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPI repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPI:

```bash
pipx install {{ cookiecutter.target_id }}
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/{{ cookiecutter.target_id }}.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the target.

This section can be created by copy-pasting the CLI output from:

```
{{ cookiecutter.target_id }} --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
target is available by running:

```bash
{{ cookiecutter.target_id }} --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `{{ cookiecutter.target_id }}` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
{{ cookiecutter.target_id }} --version
{{ cookiecutter.target_id }} --help
# Test using the "Smoke Test" tap:
tap-smoke-test | {{ cookiecutter.target_id }} --config /path/to/{{ cookiecutter.target_id }}-config.json
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

You can also test the `{{cookiecutter.target_id}}` CLI interface directly using `uv run`:

```bash
uv run {{cookiecutter.target_id}} --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
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
cd {{ cookiecutter.target_id }}
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke {{ cookiecutter.target_id }} --version

# OR run a test ELT pipeline with the Smoke Test sample tap:
meltano run tap-smoke-test {{ cookiecutter.target_id }}
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
