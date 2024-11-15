---
myst:
  heading_anchors: 4
---

# Release Process

The Singer SDK currently follows roughly a [ZeroVer versioning scheme](https://0ver.org/). Starting with the Singer SDK 1.0, version numbers will use [semantic versioning](https://semver.org/).

## PyPI releases

### Stable releases

The following steps are required to create a stable release:

1. Trigger the `.github/workflows/version_bump.yml` workflow from the GitHub Actions tab, or from the CLI with `gh workflow run version_bump.yml`.
2. Wait for the workflow to complete. It will create a new PR with the version bump, and also a draft release.
3. Review the PR for any errors in the changelog and version bumps, then merge it.
4. Publish the draft release from the GitHub Releases tab.

#### Feature releases

Feature releases are the primary way that new features are added to the Singer SDK. They are released on a roughly monthly cadence.

#### Patch releases

Patch releases are released as needed to fix bugs or security issues. They are released on an as-needed basis.

#### Release Highlights

Once the release is published, we manually add a section to the changelog with the release highlights. This section should include a brief description of the most important changes in the release. For example:

```markdown
## v0.41.0 (2024-10-02)

### Highlights

- It's easier now for SQL tap developers to customize the mapping from SQL column types to JSON schema. See [the guide](https://sdk.meltano.com/en/v0.41.0/guides/sql-tap.html#custom-type-mapping) for details.
```

### Pre-releases

```bash
git tag v0.42.0a3
git push origin v0.42.0a3
```

Pre-releases are normal tags with pre-release identifiers. They are used to test new features before a stable release. Pre-releases are triggered by pushing a tag with a pre-release identifier, e.g. `v0.42.0a3`.

We don't generate release notes for pre-releases, nor do we update the changelog so creating a pre-release is as simple as pushing a tag:

## Release cadence

The Singer SDK follows a roughly monthly release cadence. [Milestones](https://github.com/meltano/sdk/milestones) are used to track the progress of each release. The milestones are named after the release version, e.g. `v0.42.0`.

## Deprecation policy

A [feature release](#feature-releases) may deprecate a feature, but it will not remove it until the next major release. A deprecation will be clearly documented in the changelog and in the code.

All deprecated features will emit a `SingerSDKDeprecationWarning` when used, so users can raise them as exceptions when running their tests to ensure that they are not using any deprecated features:

```console
$ pytest -W error::singer_sdk.helpers._compat.SingerSDKDeprecationWarning
```
