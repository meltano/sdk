# Meltano SDK for Taps and Targets

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
