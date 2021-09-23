# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).



## Unreleased
---

### New

### Changes

### Fixes

- Tap SDK: Fixes issue where stream map schema generation fails when overriding the value of an existing property. ([#196](https://gitlab.com/meltano/sdk/-/issues/196), [!165](https://gitlab.com/meltano/sdk/-/merge_requests/165))

### Breaks


## 0.3.9 - (2021-09-23)
---

### Changes
- Tap SDK: Set key_properties = [] vs null per the Singer spec ([#160](https://gitlab.com/meltano/sdk/-/merge_requests/160)


## 0.3.8 - (2021-09-16)
---

### Fixes

- Tap and Target SDK: Resolves `2to3` compatibility issues when installed with `setuptools>=58.0`.
- Resolve issue preventing repo from being cloned on Windows.


## 0.3.7 - (2021-09-09)
---

### New

- Tap and Target SDK: Added compatibility with [PEP 561](https://www.python.org/dev/peps/pep-0561/) and `mypy` type checking ([#212](https://gitlab.com/meltano/sdk/-/issues/212), [!150](https://gitlab.com/meltano/sdk/-/merge_requests/150)) -- _Thanks, **[Laurent Savaëte](https://gitlab.com/LaurentS)**!_

### Changes

- Tap SDK: Improved record parsing and validation performance, especially with large record objects ([#161](https://gitlab.com/meltano/sdk/-/issues/161), [!146](https://gitlab.com/meltano/sdk/-/merge_requests/146))
- Tap SDK: Changed the signature of `Stream.apply_catalog` to reflect new catalog parsing flow ([#161](https://gitlab.com/meltano/sdk/-/issues/161), [!146](https://gitlab.com/meltano/sdk/-/merge_requests/146))


## 0.3.6 - (2021-08-26)
---

### New

- Tap and Target SDK: Adds support for Python 3.9 (#66, !38)
- Tap SDK: Added support for new authenticator classes: `BasicAuthenticator`, `BearerTokenAuthenticator`, and `APIKeyAuthenticator` (#185, !128) -- _Thanks, **[Stephen Bailey](https://gitlab.com/stkbailey)**!_

### Changes

- Tap and Target SDK: Bumps `click` library version to 8.0 (#178, !140).
- Target SDK: Improves `BatchSink` performance by reducing the frequency by which batches are processed. (#172, !137)

### Fixes

- Tap SDK: Improves CLI `--help` output (#177, !140).
- Tap SDK: Fixes a bug in state tracking where timezone-aware timestamps are appended again with `+0:00` (#176, !142) -- _Thanks, **[Joshua Adeyemi](https://gitlab.com/joshua.a.adeyemi)**!_
- Tap SDK: Improve performance by reusing a single authenticator instance (#168, #173, !136)


## 0.3.5 - (2021-08-17)
---

### Fixes

- Tap SDK: Fixed a bug where not using a catalog file resulted in all streams being selected but all properties being removed from the schema and records (#190, !132)


## v0.3.4
---

### New

- Tap SDK: Added full support for selection metadata as specified by the [Singer Spec](https://hub.meltano.com/singer/spec#metadata), including metadata for `selected`, `selected-by-default`, and `inclusion` (!121)

### Changes

- Target SDK: Improved performance for Batch Sinks by skipping extra drain operations when newly recieved STATE messages are unchanged from the prior received STATE (#172, !125) -- _Thanks, **[Pat Nadolny](https://gitlab.com/pnadolny13)**!_

### Fixes

- Target SDK: Fixed a bug where would emit an invalid STATE payload (#188, !130) -- _Thanks, **[Pat Nadolny](https://gitlab.com/pnadolny13)**!_
- Tap SDK: Fixed a bug where replication key signposts were not correctly applied for streams which defined them (#180, !129)

## v0.3.3
---

### New

- Added JSONPath for taps to handle record extraction from API responses (!77)

### Fixes

- Resolve batch `context` not being reset between batches (#164, !117)

### Removes

- Removed unused `DEFAULT_BATCH_SIZE_ROWS` in favor of `max_size` for `BatchSink` implementations (#163, !118)

## v0.3.2

### Fixes

- Resolve stream map duplicates not aliased correctly (#154, !114)

## v0.3.1

### New

- Added target support for `add_record_metadata` config (#157, !111)

### Fixes

- Resolve target failures when dates are parsed prior to JSON Schema validation (#156, !110)
- Resolve target failures when `default_sink_class` is not used (#153, !109)
- Improved tap log messages when child property's selection metadata defaults to the parent's (#131, !91)

## v0.3.0

### New

- Added Stream Map feature for inline transformation and filtering capabilities (#63, !4, !92, !103)
- Added Target SDK, components and templates for building Singer targets (#96, !4)

### Breaks

- Removed methods deprecated in v0.2.0: `get_partition_state()` and `get_stream_or_partition_state()`. Affected developers should replace these with references to `get_context_state()`. (#152, !107)

## v0.2.0

### New

- Added support for parent-child streams (#97, !79)
- Added support for configurable metrics logging (#91, !79)
- Added ability to use fewer state bookmarks by setting `Stream.state_partitioning_keys` to a
  subset of available context keys (!79)

### Changes

- Renamed the optional `partition` dictionary arg in method signatures to the more generic `context` (!79)

### Deprecates

- The methods `Stream.get_partition_state()` and `Stream.get_stream_or_partition_state()`
  have been deprecated in favor of the new and simpler `get_context_state()` (!79)

### Improves

- Code coverage is now tracked and available as a tool for SDK contributors to further
  improve overall stability and help prioritize unit test development. (#39, !89)

## v0.1.6

Stability and bugfix release. No breaking changes.

### Fixes

- Resolved excessive logging during selection filtering. (#125, !83)
- Resolved issue where deselected sub-fields were incorrectly included in stream records. (#126, !85) -- _Thanks, **[Alex Levene](https://gitlab.com/alex.levene)**!_

### Improves

- Added improved type hints for developers, including mypy code compliance for improved stability. (#127, !86)

## v0.1.5

Bugfix release. No breaking changes.

### Fixes

- Resolved tap failure when a sorted stream has non-unique replication keys. (#120, !82)

## v0.1.4

Significant release with newly added features. No breaking changes.

### New

- Added support for GraphQL query variables (#115, !78)
- Added selection rules support for record and schema messages (#7, !26)

### Changes

- Improved cookiecutter template coverage, resolved readability issues. (#116, #119, !75)

### Fixes

- Resolved tap failure when a stream is missing from the input catalog. (#105, !80)
- Resolved bug where unsorted streams did not properly advance state bookmarks for incremental streams. (#118, !74)

## v0.1.3

Significant release with newly added features. No breaking changes.

### New

- Added `is_sorted` stream property, which enables long-running incremental streams to be
  resumed if interrupted. (!61)
- Added signpost feature to prevent bookmarks from advancing beyond the point where all
  records have been streamed. (!61)
- Added `get_replication_key_signpost()` stream method which defaults to the current time
  for timestamp-based replication keys. (!61)

### Fixes

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

### Fixes

- Fixes bug in `Stream.get_starting_timestamp()` using incorrect state key (#94, !58)

## v0.1.1

Documentation and cookiecutter template improvements.

## New

- Added 'admin_name' field in cookiecutter, streamline poetry setup (!25)
- Added meltano integration and testing options (#47, !52)
- Added new cookiecutter `.sh` script to ease testing during development (!52)

### Changes

- Improved cookiecutter readme template with examples (#76, !53)

## v0.1.0

First official SDK release. Numerous changes and improvements implemented, with the goal of stabilizing the SDK
and making it broadly available to the community.

### New

- Added this CHANGELOG.md file (#68, !43)
- Added standardized tap tests (!36, #78, !46)
- Added SDK testing matrix for python versions 3.6, 3.7, 3.8 (#61, !33)
- Added support for multiple `--config=` inputs, combining one or more config.json files (#53, !27)
- Added new CLI `--test` option to perform connection test on all defined streams (#14, !28)
- Added default value support for plugin configs (!12) -- _Contributed by: **[Ken Payne](https://gitlab.com/kgpayne)**_

### Changes

- Promote `singer_sdk.helpers.typing` to `singer_sdk.typing` (#84)
- Modified environment variable parsing logic for arrays (#82)
- Renamed `http_headers` in `Authenticator` class to `auth_headers` (#75, !47)
- Expect environment variables in all caps (`<PLUGIN>_<SETTING>`) (#59, !34)
- Parse environment variables only if `--config=ENV` is passed (#53, !27)

### Fixes

- OAuth no longer applies `client_email` automatically if `client_id` is missing (#83)
- Resolved issue on Python 3.6: `cannot import 'metadata' from 'importlib'` (#58)
- Fixed issue reading from JSON file (!11) -- _Contributed by: **[Edgar R. Mondragón](https://gitlab.com/edgarrmondragon)**_
- Look only for valid plugin settings in environment variables (!21) -- _Contributed by: **[Edgar R. Mondragón](https://gitlab.com/edgarrmondragon)**_
- Fixed bug in `STATE` handling (!13) -- _Contributed by: **[Ken Payne](https://gitlab.com/kgpayne)**_

### Removes

- Remove parquet sample (#81,!48)

## v0.0.1-devx

Initial prerelease version for review and prototyping.
