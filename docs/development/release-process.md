# Release Process

## PyPI releases

TODO: This section needs to be updated to reflect the release process.

### Feature releases

Feature releases are the primary way that new features are added to the Singer SDK. They are released on a roughly monthly cadence.

### Patch releases

Patch releases are released as needed to fix bugs or security issues. They are released on an as-needed basis.

## Release cadence

Starting with the Singer SDK 1.0, version numbers will use a loose form of [semantic versioning](https://semver.org/).

SemVer makes it easier to see at a glance how compatible releases are with each other. It also helps to anticipate when compatibility shims will be removed.

## Deprecation policy

A [feature release](#feature-releases) may deprecate a feature, but it will not remove it until the next major release. A deprecation will be clearly documented in the changelog and in the code.
