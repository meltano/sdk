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

## Singer SDK Dev Guide

See the [dev guide](../../docs/dev_guide.md) for more instructions on how to use the Singer SDK to 
develop your own taps and targets.

## Config Guide

_`TODO:` Provide instructions here for users of the tap:_

### Accepted Config Options

- [ ] `TODO:` Provide a list of config options accepted by the tap.

### Source Authentication and Authorization

- [ ] `TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.
