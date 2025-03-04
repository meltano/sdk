# {{ cookiecutter.mapper_id }}

`{{ cookiecutter.mapper_id }}` is a Singer mapper for {{ cookiecutter.name }}.

Built with the [Meltano Mapper SDK](https://sdk.meltano.com) for Singer Mappers.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPI repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPI:

```bash
pipx install {{ cookiecutter.mapper_id }}
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/{{ cookiecutter.mapper_id }}.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the mapper.

This section can be created by copy-pasting the CLI output from:

```
{{ cookiecutter.mapper_id }} --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
mapper is available by running:

```bash
{{ cookiecutter.mapper_id }} --about
```

### Configure using environment variables

This Singer mapper will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your mapper requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `{{ cookiecutter.mapper_id }}` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Mapper Directly

```bash
{{ cookiecutter.mapper_id }} --version
{{ cookiecutter.mapper_id }} --help
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

You can also test the `{{cookiecutter.mapper_id}}` CLI interface directly using `uv run`:

```bash
uv run {{cookiecutter.mapper_id}} --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This mapper will work in any Singer environment and does not require Meltano.
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
cd {{ cookiecutter.mapper_id }}
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Run a test `run` pipeline:
meltano run tap-smoke-test {{ cookiecutter.mapper_id }} target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps, targets, and mappers.
