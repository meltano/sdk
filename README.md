# `singer-sdk` - an open framework for building Singer-compliant taps

- _Note: This framework is still in early development and planning phases_

## Strategies for Optimized Tap Development

1. **Universal Code Formatting.**
    - _From the [Black](https://black.readthedocs.io) product description:_
      > By using Black, you agree to cede control over minutiae of hand-formatting. In return, Black gives you speed, determinism, and freedom from pycodestyle nagging about formatting. You will save time and mental energy for more important matters. **Black makes code review faster by producing the smallest diffs possible.** Blackened code looks the same regardless of the project you’re reading. **Formatting becomes transparent after a while and you can focus on the content instead.**
    - _If you use our companion cookiecutter template, your project will start out auto-formatted by Black. You can keep this default or change it - the choice is yours._
2. **Pervasive Python Type Hints.**
    - _Spend less time reading documentation thanks to pervasive type declarations in our base class._
3. **Less is More.**
    - _Because taps built from the template require less code, taking advantage of common base class capabilities, developers are able to dramatically reduce the time to develop a fully mature tap._
4. **Create Future-Proof Plugins.**
    - _Take advantage of new base class capabilities by simply updating your dependency version and retesting with the latest versions._

## Cookie-Cutter Quick Start

`Singer SDK` provides a quickstart `cookiecutter` template for starting new taps.

* [Click here for the **Cookiecutter Tap Template**](cookiecutter/tap-template/README.md)

## Tap Dev Guide

See the [dev guide](docs/dev_guide.md) for instructions on how to get started building your own
taps.

## Working within this repo

First clone, then...

### Install

`NOTE: Singer SDK currently works with Python versions 3.6 through 3.8.x. Python 3.9 is not yet supported.`

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

### Run tests

```bash
poetry run pytest
```
