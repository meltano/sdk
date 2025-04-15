<h1 align="center">Meltano Singer SDK</h1>

<h3 align="center">
The Tap and Target SDKs are the fastest way to build custom data extractors and loaders!
Taps and targets built on the SDK are automatically compliant with the
<a href="https://hub.meltano.com/singer/spec">Singer Spec</a>, the
de-facto open source standard for extract and load pipelines.
</h3>

---

</br>

<div align="center">
  <img alt="Meltano Singer SDK Logo" src="https://user-images.githubusercontent.com/11428666/231584532-ffa694e6-60f9-4fd6-b2ee-5ff3e39d3ad6.svg" width="600"/>
</div>

</br>

<div align="center">
  <a href="https://meltano.com/slack">
    <img alt="Meltano Community Slack" src="https://img.shields.io/badge/Slack-Join%20the%20Community-blue?logo=slack"/>
  </a>
  <a href="https://pypi.org/project/singer-sdk">
   <img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/singer-sdk"/>
  </a>
  <a href="https://pypi.org/project/singer-sdk">
   <img alt="Downloads" src="https://img.shields.io/pypi/dw/singer-sdk?color=blue"/>
  </a>
  </br>
  <a href="https://pypi.org/project/singer-sdk">
   <img alt="PyPI Version" src="https://img.shields.io/pypi/v/singer-sdk?color=blue"/>
  </a>
  <a href="https://sdk.meltano.com/en/latest/?badge=latest">
   <img alt="Documentation Status" src="https://readthedocs.org/projects/meltano-sdk/badge/?version=latest"/>
  </a>
  <a href="https://codecov.io/gh/meltano/sdk">
   <img alt="codecov" src="https://codecov.io/gh/meltano/sdk/branch/main/graph/badge.svg?token=kS1zkemAgo"/>
  </a>
  <a href="https://results.pre-commit.ci/latest/github/meltano/sdk/main">
   <img alt="pre-commit.ci status" src="https://results.pre-commit.ci/badge/github/meltano/sdk/main.svg"/>
  </a>
  <a href="https://github.com/astral-sh/uv">
   <img alt="uv" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json"/>
  </a>
</div>

---

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
