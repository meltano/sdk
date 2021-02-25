_**NOTE:** The Singer SDK framework is still in early exploration and development phases. For more
information and to be updated on this project, please feel free to subscribe to our
[original Meltano thread](https://gitlab.com/meltano/meltano/-/issues/2401) and the
[initial PR for the underlying framework](https://gitlab.com/meltano/singer-sdk/-/merge_requests/1)._

--------------------------------

# Welcome to the {{cookiecutter.tap_id}} Singer Tap!

This Singer-compliant tap was created using the [Singer SDK](https://gitlab.com/meltano/singer-sdk).

## Getting Started

- [ ] As a first step, you will want to scan the entire project for the text "`TODO:`" and complete any recommended steps.
- [ ] Once you have a boilerplate prepped, you'll want to setup Poetry and create the virtual environment for your project:

    ```bash
    pipx install poetry
    poetry install
    # Now navigate to your repo root and then run:
    poetry init
    ```

- [ ] You can test out your new CLI directly with:

    ```bash
    poetry run {{cookiecutter.tap_id}} --help
    ```

- [ ] Create some tests and then run:

    ```bash
    poetry run pytest
    ```

_`TODO: Remove the above section once complete.`_

## Singer SDK Dev Guide

See the [dev guide](../../docs/dev_guide.md) for more instructions on how to use the Singer SDK to 
develop your own taps and targets.

## Config Guide

`TODO:` Provide instructions here for users of the tap:

### Accepted Config Options

- [ ] `TODO:` Provide a list of config options accepted by the tap.

### Source Authentication and Authorization

- [ ] `TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.
