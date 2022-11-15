# Meltano Singer SDK

[![Python Versions](https://img.shields.io/pypi/pyversions/singer-sdk)](https://pypi.org/project/singer-sdk)
[![Downloads](https://img.shields.io/pypi/dw/singer-sdk?color=blue)](https://pypi.org/project/singer-sdk)
[![PyPI Version](https://img.shields.io/pypi/v/singer-sdk?color=blue)](https://pypi.org/project/singer-sdk)
[![Documentation Status](https://readthedocs.org/projects/meltano-sdk/badge/?version=latest)](https://sdk.meltano.com/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/meltano/sdk/branch/main/graph/badge.svg?token=kS1zkemAgo)](https://codecov.io/gh/meltano/sdk)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/meltano/sdk/main.svg)](https://results.pre-commit.ci/latest/github/meltano/sdk/main)

The Tap and Target SDKs are the fastest way to build custom data extractors and loaders!
Taps and targets built on the SDK are automatically compliant with the
[Singer Spec](https://hub.meltano.com/singer/spec), the
de-facto open source standard for extract and load pipelines.

## Future-proof extractors and loaders, with less code

On average, developers tell us that they write about 70% less code by using the SDK, which
makes learning the SDK a great investment. Furthermore, as new features and capabilities
are added to the SDK, your taps and targets can always take advantage of the latest
capabilities and bug fixes, simply by updating your SDK dependency to the latest version.

## Meltano

*Not familiar with Meltano?*  [Meltano](https://docs.meltano.com/getting-started/meltano-at-a-glance) is your CLI for ELT+ that:

- **Starts simple**: Meltano is pip-installable and comes in a prepackaged docker container, you can have your first ELT pipeline running within minutes.
- **Has DataOps out-of-the-box**: Meltano provides tools that make DataOps best practices easy to use in every project.
- **Integrates with everything**: 300+ natively supported data sources & targets, as well as additional plugins like great expectations or dbt are natively available.
- **Is easily customizable**: Meltano isn't just extensible, it's built to be extended! The Singer SDK (for Connectors) & EDK (for Meltano Components) are easy to use. Meltano Hub helps you find all of the connectors and components created across the data community.
- **Is a mature system**: Developed since 2018, runs in production at large companies like GitLab, and currently powers over a million pipeline runs monthly.
- **Has first class ELT tooling built-in**: Extract data from any data source, load into any target, use inline maps to transform on data on the fly, and test the incoming data, all in one package.

If you want to get started with Meltano, we suggest you:
- head over to the [Installation](https://docs.meltano.com/getting-started/installation)
- or if you have it installed, go through the [Meltano Tutorial](https://docs.meltano.com/getting-started/part1).

## Documentation

- See our [online documentation](https://sdk.meltano.com) for instructions on how
to get started with the SDK.

## Contributing back to the SDK

- For more information on how to contribute, see our [Contributors Guide](https://sdk.meltano.com/en/latest/CONTRIBUTING.html).

## Making a new release of the SDK

1. Trigger a version bump [using the GitHub web UI](https://github.com/edgarrmondragon/sdk/actions/workflows/version_bump.yml) or the cli:

   ```console
   $ gh workflow run
   ```

   The `increment: auto` option will figure out the most appropriate bump based on commit history.

1. Follow the checklist in the PR description.

1. Publish a new release [using the GitHub web UI](https://github.com/meltano/sdk/releases/new).
