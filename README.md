# `singer-sdk` - a framework for building Singer taps

## Less is More

Because taps built from the SDK require substantially less code, developers are able to dramatically reduce the time to develop a fully mature Singer tap.

## Build Future-Proof Data Extractors

We will continue to add new features to the SDK. You can always take advantage of the latest capabilities by simply updating your SDK version and then retesting and republishing with the latest version.

## Cookie-Cutter Quick Start

The SDK provides a quickstart `cookiecutter` template for starting new taps.

* [Click here for the **Cookiecutter Tap Template**](cookiecutter/tap-template/README.md)

## Tap Dev Guide

See the [dev guide](docs/dev_guide.md) for instructions on how to get started building your own
taps.

## SDK Implementation Details

For more detailed information about the SDK implementation, please see the 
[SDK Implementation Details](./docs/implementation/README.md) section.

## Contributing back to the SDK

First clone, then...

### Install

_**Note:** The SDK currently works with Python versions 3.6 through 3.8.x. Python 3.9 is not yet supported._

Install prereqs:

```bash
pip3 install pipx
pipx ensurepath
pipx install poetry
```

Install package dependencies:

```bash
cd singer-sdk
```

```bash
# Install package and dependencies:
poetry install
# OR install in editable mode:
poetry install --no-root
```

- For more information, see our [Contributors Guide](CONTRIBUTING.md).

### Run tests

```bash
poetry run pytest
```
