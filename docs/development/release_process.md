# Release Process

## PyPI releases

Releases are published to PyPI by a GitHub Actions workflow, triggered when a GitHub [Release](https://github.com/meltano/sdk/releases) is published.

### Feature releases

Feature releases are the primary way that new features are added to the Singer SDK. They are released on a roughly monthly cadence.

### Patch releases

Patch releases are released as needed to fix bugs or security issues. They are released on an as-needed basis.

## Release cadence

Starting with the Singer SDK 1.0, version numbers will use a loose form of [semantic versioning](https://semver.org/).

SemVer makes it easier to see at a glance how compatible releases are with each other. It also helps to anticipate when compatibility shims will be removed.

## Deprecation policy

A [feature release](#feature-releases) may deprecate a feature, but it will not remove it until the next major release. A deprecation will be clearly documented in the changelog and in the code.

All deprecated features will emit a `SingerDeprecationWarning` when used, so users can raise them as exceptions when running their tests to ensure that they are not using any deprecated features:

```console
$ pytest -W error::singer_sdk.utils.deprecation.SingerSDKDeprecationWarning
```
