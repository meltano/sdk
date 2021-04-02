# {{cookiecutter.tap_id}}

This Singer tap was created using the [Singer SDK](https://gitlab.com/meltano/singer-sdk).

## Getting Started

- [ ] As a first step, you will want to scan the entire project for the text "`TODO:`" and complete any recommended steps.
- [ ] `TODO:` Once you have a boilerplate prepped, you'll want to setup Poetry and create the virtual environment for your project:

    ```bash
    pipx install poetry
    poetry install
    ```

- [ ] `TODO:` You can test out your new CLI directly with:

    ```bash
    poetry run {{cookiecutter.tap_id}} --help
    ```


_`TODO: Remove the above section once complete.`_

## Testing Guide

Create tests within the `{{ cookiecutter.library_name }}/tests` subfolder and
  then run:

```bash
poetry run pytest
```

## Testing with [Meltano](meltano.com)

_Note: This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created.

To test using Meltano, first install (if you haven't already):

```bash
cd {{ cookiecutter.tap_id }}
# Install meltano
pipx install meltano
# Initialize meltano within this directory
meltano install
```

And now you can test and orchestrate using Meltano:

```bash
meltano invoke {{ cookiecutter.tap_id }}
```

## Singer SDK Dev Guide

See the [dev guide](../../docs/dev_guide.md) for more instructions on how to use the Singer SDK to 
develop your own taps and targets.

## Config Guide

_`TODO:` Provide instructions here for users of the tap:_

### Accepted Config Options

- [ ] `TODO:` Provide a list of config options accepted by the tap.

### Source Authentication and Authorization

- [ ] `TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.
