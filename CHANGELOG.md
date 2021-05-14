# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!---
DO NOT DELETE
The next few lines form the template for unreleased changes.
## [Unreleased]
### Added
### Changed
### Deprecated
### Removed
### Fixed
-->

## v0.2.0 [Unreleased]

### Added

- Added support for parent-child hierarchical streams
- Added support for "Target SDK", including target and sink classes

## v0.1.6

Stability and bugfix release. No breaking changes.

### Fixed

- Resolved excessive logging during selection filtering. (#125, !83)
- Resolved issue where deselected sub-fields were incorrectly included in stream records. (#126, !85) -- _Thanks, **[Alex Levene](https://gitlab.com/alex.levene)**!_

### Improved

- Added improved type hints for developers, mypy code compliance for improved stability. (#125, !83)

## v0.1.5

Bugfix release. No breaking changes.

### Fixed

- Resolved tap failure when a sorted stream has non-unique replication keys. (#120, !82)

## v0.1.4

Significant release with newly added features. No breaking changes.

### Added

- Added support for GraphQL query variables (#115, !78)
- Added selection rules support for record and schema messages (#7, !26)

### Changed

- Improved cookiecutter template coverage, resolved readability issues. (#116, #119, !75)

### Fixed

- Resolved tap failure when a stream is missing from the input catalog. (#105, !80)
- Resolved bug where unsorted streams did not properly advance state bookmarks for incremental streams. (#118, !74)

## v0.1.3

Significant release with newly added features. No breaking changes.

### Added

- Added `is_sorted` stream property, which enables long-running incremental streams to be
  resumed if interrupted. (!61)
- Added signpost feature to prevent bookmarks from advancing beyond the point where all
  records have been streamed. (!61)
- Added `get_replication_key_signpost()` stream method which defaults to the current time 
  for timestamp-based replication keys. (!61)

### Added

- Added ability for developer to override `Stream.get_max_replication_key_bookmark()`. When
  set, the SDK will prevent bookmarks from advancing beyond the specified value.

### Changed

- For timestamp-type replication keys, bookmarks are disallowed to advance ahead of
  `utcnow` at the time the stream sync begins. This prevents potentially missing records in
  subsequent executions. (!61)
- Unsorted streams have special handling which prevents incremental bookmark keys from
  being incorrectly applied in case of failure. (!61)
- Unsorted streams will now raise an error during incremental replication. Developers
  must explicitly declare `Stream.sort_keys = None` to enable incremental sync. (!61)

## v0.1.3 [Unreleased]

- Fixed a scenario where _unsorted_ incremental streams would generate incorrect STATE bookmarks. (!61) -- _Thanks, **[Egi Gjevori](https://gitlab.com/egi-gjevori)**!_
- Fixed a problem where CI pipelines would fail when run from a fork. (!71) -- _Thanks, **[Derek Visch](https://gitlab.com/vischous)**!_
- Fixed fatal error when running from the cookiecutter shell script (#102, !64)

## v0.1.2

Fixes bug in state handling, adds improvements to documentation.

### Documentation

- Streamlined Dev Guide (!56)
- Added Code Samples page, including dynamic schema discovery examples (#33, !56)
- Added links to external sdk-based taps (#32, !56)
- Added static/dynamic property documentation (#86, !56)
- Added "implementation" docs for debugging and troubleshooting (#71, !41)

### Fixed

- Fixes bug in `Stream.get_starting_timestamp()` using incorrect state key (#94, !58)

## v0.1.1

Documentation and cookiecutter template improvements.

## Added

- Added 'admin_name' field in cookiecutter, streamline poetry setup (!25)
- Added meltano integration and testing options (#47, !52)
- Added new cookiecutter `.sh` script to ease testing during development (!52)

### Changes

- Improved cookiecutter readme template with examples (#76, !53)

## v0.1.0

First official SDK release. Numerous changes and improvements implemented, with the goal of stabilizing the SDK
and making it broadly available to the community.

### Added

- Added this CHANGELOG.md file (#68, !43)
- Added standardized tap tests (!36, #78, !46)
- Added SDK testing matrix for python versions 3.6, 3.7, 3.8 (#61, !33)
- Added support for multiple `--config=` inputs, combining one or more config.json files (#53, !27)
- Added new CLI `--test` option to perform connection test on all defined streams (#14, !28)
- Added default value support for plugin configs (!12) -- _Contributed by: **[Ken Payne](https://gitlab.com/kgpayne)**_

### Changed

- Promote `singer_sdk.helpers.typing` to `singer_sdk.typing` (#84)
- Modified environment variable parsing logic for arrays (#82)
- Renamed `http_headers` in `Authenticator` class to `auth_headers` (#75, !47)
- Expect environment variables in all caps (`<PLUGIN>_<SETTING>`) (#59, !34)
- Parse environment variables only if `--config=ENV` is passed (#53, !27)

### Fixed

- OAuth no longer applies `client_email` automatically if `client_id` is missing (#83)
- Resolved issue on Python 3.6: `cannot import 'metadata' from 'importlib'` (#58)
- Fixed issue reading from JSON file (!11) -- _Contributed by: **[Edgar R. Mondragón](https://gitlab.com/edgarrmondragon)**_
- Look only for valid plugin settings in environment variables (!21) -- _Contributed by: **[Edgar R. Mondragón](https://gitlab.com/edgarrmondragon)**_
- Fixed bug in `STATE` handling (!13) -- _Contributed by: **[Ken Payne](https://gitlab.com/kgpayne)**_

### Removed

- Remove parquet sample (#81,!48)

## v0.0.1-devx

Initial prerelease version for review and prototyping.
