# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.35.0 (2024-02-02)

### ✨ New

- [#2208](https://github.com/meltano/sdk/issues/2208) Allow users to disable schema validation in targets
- [#2170](https://github.com/meltano/sdk/issues/2170) Generate fake data with stream maps -- _**Thanks @ReubenFrankel!**_
- [#937](https://github.com/meltano/sdk/issues/937) Support validating configuration for any tap with a dynamic catalog
- [#2144](https://github.com/meltano/sdk/issues/2144) Support fanning out parent record into multiple child contexts/syncs
- [#1918](https://github.com/meltano/sdk/issues/1918) End RESTStream pagination if an empty page is received

### 🐛 Fixes

- [#2203](https://github.com/meltano/sdk/issues/2203) Fix serialization of arbitrary objects (e.g. `ObjectId` from mongoDB) during flattening -- _**Thanks @dgawlowsky!**_
- [#2200](https://github.com/meltano/sdk/issues/2200) Quote column names in INSERT statement
- [#2195](https://github.com/meltano/sdk/issues/2195) Include empty `schemas` directory in REST tap cookiecutter
- [#2187](https://github.com/meltano/sdk/issues/2187) Replace use of deprecated `jsonschema._RefResolver` with recommended `referencing` library
- [#2184](https://github.com/meltano/sdk/issues/2184) Reduce amount of unnecessary whitespace in Singer output
- [#2183](https://github.com/meltano/sdk/issues/2183) Ensure `.vscode` directory is included when requested in cookiecutters and avoid failing if it does not exist
- [#2180](https://github.com/meltano/sdk/issues/2180) Limit supported Python versions in `--about` output to existing ones
- [#2108](https://github.com/meltano/sdk/issues/2108) Log sink name when an unhandled error occurs during setup
- [#2158](https://github.com/meltano/sdk/issues/2158) Fix pytest plugin declaration so it can be used without requiring defining `pytest_plugins` in `conftest.py`
- [#2105](https://github.com/meltano/sdk/issues/2105) Default handling of `ACTIVATE_VERSION` messages to soft deletes and add new `SQLConnector.delete_old_versions` method

### ⚙️ Under the Hood

- [#2189](https://github.com/meltano/sdk/issues/2189) Use `functools.lru_cache` instead of the stale `memoization` library (#1981)
- [#2188](https://github.com/meltano/sdk/issues/2188) Remove unused `logger` parameter from private catalog helper functions
- [#2143](https://github.com/meltano/sdk/issues/2143) Drop support for Python 3.7
- [#2157](https://github.com/meltano/sdk/issues/2157) Remove `pytz` dependency and use `datetime.timezone.utc` instead of `pytz.UTC` where possible
- [#2136](https://github.com/meltano/sdk/issues/2136) Create interface for schema validation in sinks, and implement it for `python-jsonschema` -- _**Thanks @BuzzCutNorman!**_
- [#2130](https://github.com/meltano/sdk/issues/2130) Allow loading stream schemas from `importlib.resources.abc.Traversable` types

### 📚 Documentation Improvements

- [#2204](https://github.com/meltano/sdk/issues/2204) Document supported package extras
- [#2186](https://github.com/meltano/sdk/issues/2186) Call out minimum recommended `cookiecutter` version
- [#2168](https://github.com/meltano/sdk/issues/2168) Explain `Progress is not resumable if interrupted` in docs FAQ
- [#2140](https://github.com/meltano/sdk/issues/2140) Update auth caching example to use `functools.cached_property`

## v0.34.1 (2023-12-19)

### 🐛 Fixes

- [#2118](https://github.com/meltano/sdk/issues/2118) Output JSONPath expression with match count message -- _**Thanks @mjsqu!**_
- [#2107](https://github.com/meltano/sdk/issues/2107) Respect forced replication method when retrieving state
- [#2094](https://github.com/meltano/sdk/issues/2094) Use `nulls_first` when available to order `NULL` results in incremental SQL streams

### ⚙️ Under the Hood

- [#1733](https://github.com/meltano/sdk/issues/1733) Test with Python 3.12 🐍
- [#2095](https://github.com/meltano/sdk/issues/2095) Use `CursorResult.mappings()` in SQL streams
- [#2092](https://github.com/meltano/sdk/issues/2092) Use `datetime.fromisoformat` in other places
- [#2090](https://github.com/meltano/sdk/issues/2090) Explicitly use `T` iso date separator

### 📚 Documentation Improvements

- [#2111](https://github.com/meltano/sdk/issues/2111) Fix broken requests documentation links -- _**Thanks @mjsqu!**_

## v0.34.0 (2023-12-05)

## v0.34.0rc1 (2023-12-05)

### 🐛 Fixes

- [#2076](https://github.com/meltano/sdk/issues/2076) Make a explicit dependency on `python-dateutil`

## v0.34.0b1 (2023-11-28)

### ✨ New

- [#2044](https://github.com/meltano/sdk/issues/2044) Add Parquet as a batch encoding option -- _**Thanks @jamielxcarter!**_
- [#768](https://github.com/meltano/sdk/issues/768) Better error messages when config validation fails
- [#1854](https://github.com/meltano/sdk/issues/1854) Make stream logger a child of the tap logger

### ⚙️ Under the Hood

- [#2070](https://github.com/meltano/sdk/issues/2070) Parse dates with `datetime.fromisoformat`/`backports.datetime_fromisoformat` -- _**Thanks @BuzzCutNorman!**_
- [#2056](https://github.com/meltano/sdk/issues/2056) Break up `TapTestClassFactory._annotate_test_class` into simpler methods
- [#2058](https://github.com/meltano/sdk/issues/2058) Implement a `SingerWriter` class in `singer_sdk.io_base` and use it to emit Singer messages -- _**Thanks @BuzzCutNorman!**_
- [#2061](https://github.com/meltano/sdk/issues/2061) Simplify target template file names with `post_gen_project.py` hook -- _**Thanks @vicmattos!**_
- [#2060](https://github.com/meltano/sdk/issues/2060) Simplify tap template file names with `post_gen_project.py` hook -- _**Thanks @vicmattos!**_

### 📚 Documentation Improvements

- [#2039](https://github.com/meltano/sdk/issues/2039) Add 404 page with `sphinx-notfound-page`
- [#2037](https://github.com/meltano/sdk/issues/2037) Add flattening configuration examples

## v0.33.1 (2023-11-08)

### 🐛 Fixes

- [#2035](https://github.com/meltano/sdk/issues/2035) Retry all 5xx status codes -- _**Thanks @asamasoma!**_

## v0.33.0 (2023-10-12)

### ✨ New

- [#1999](https://github.com/meltano/sdk/issues/1999) Log JSONPath match count at the INFO level
- [#1779](https://github.com/meltano/sdk/issues/1779) Cache SQL columns and schemas
- [#2003](https://github.com/meltano/sdk/issues/2003) Add ability to do list comprehensions in stream map expressions -- _**Thanks @haleemur!**_
- [#2018](https://github.com/meltano/sdk/issues/2018) Drop Python 3.7 support in cookiecutter templates -- _**Thanks @visch!**_

### 🐛 Fixes

- [#2006](https://github.com/meltano/sdk/issues/2006) Parse record `time_extracted` into `datetime.datetime` instance
- [#1996](https://github.com/meltano/sdk/issues/1996) Respect nullability of leaf properties when flattening schema
- [#1844](https://github.com/meltano/sdk/issues/1844) Safely skip parsing record field as date-time if it is missing in schema
- [#1885](https://github.com/meltano/sdk/issues/1885) Map `record` field to a JSON `object` type
- [#2015](https://github.com/meltano/sdk/issues/2015) Ensure `default` property is passed to SCHEMA messages -- _**Thanks @prakharcode!**_

### 📚 Documentation Improvements

- [#2017](https://github.com/meltano/sdk/issues/2017) Document support for comprehensions in stream maps

## v0.32.0 (2023-09-22)

### ✨ New

- [#1893](https://github.com/meltano/sdk/issues/1893) Standard configurable load methods
- [#1861](https://github.com/meltano/sdk/issues/1861) SQLTap connector instance shared with streams -- _**Thanks @BuzzCutNorman!**_

### 🐛 Fixes

- [#1954](https://github.com/meltano/sdk/issues/1954) Missing begin()s related to SQLAlchemy 2.0 -- _**Thanks @andyoneal!**_
- [#1951](https://github.com/meltano/sdk/issues/1951) Ensure SQL streams are sorted when a replication key is set
- [#1949](https://github.com/meltano/sdk/issues/1949) Retry SQLAlchemy engine creation for adapters without JSON SerDe support
- [#1939](https://github.com/meltano/sdk/issues/1939) Handle `decimal.Decimal` instances in flattening
- [#1927](https://github.com/meltano/sdk/issues/1927) Handle replication key not found in stream schema -- _**Thanks @mjsqu!**_
- [#1977](https://github.com/meltano/sdk/issues/1977) Fix hanging downstream tests in tap-postgres
- [#1970](https://github.com/meltano/sdk/issues/1970) Warn instead of crashing when schema helpers cannot append `null` to types

### ⚡ Performance Improvements

- [#1925](https://github.com/meltano/sdk/issues/1925) Add viztracer command for testing targets -- _**Thanks @mjsqu!**_

- [#1962](https://github.com/meltano/sdk/issues/1962) Ensure `raw_schema` in stream mapper is immutable

## v0.31.1 (2023-08-17)

### ✨ New

- [#1905](https://github.com/meltano/sdk/issues/1905) Add email field and use human-readable questions in templates

### 🐛 Fixes

- [#1913](https://github.com/meltano/sdk/issues/1913) Fix tap tests for multiple test classes with different input catalogs

## v0.31.0 (2023-08-07)

### ✨ New

- [#1892](https://github.com/meltano/sdk/issues/1892) Add a mapper cookiecutter template
- [#1864](https://github.com/meltano/sdk/issues/1864) SQLTarget connector instance shared with sinks -- _**Thanks @BuzzCutNorman!**_
- [#1878](https://github.com/meltano/sdk/issues/1878) Add `_sdc_sync_started_at` metadata column to indicate the start of the target process
- [#1484](https://github.com/meltano/sdk/issues/1484) Bump latest supported sqlalchemy from `1.*` to `2.*`

### 🐛 Fixes

- [#1898](https://github.com/meltano/sdk/issues/1898) Correctly serialize `decimal.Decimal` in JSON fields of SQL targets
- [#1881](https://github.com/meltano/sdk/issues/1881) Expose `add_record_metadata` as a builtin target setting
- [#1880](https://github.com/meltano/sdk/issues/1880) Append batch config if target supports the batch capability
- [#1865](https://github.com/meltano/sdk/issues/1865) Handle missing record properties in SQL sinks
- [#1838](https://github.com/meltano/sdk/issues/1838) Add deprecation warning when importing legacy testing helpers
- [#1842](https://github.com/meltano/sdk/issues/1842) Ensure all expected tap parameters are passed to `SQLTap` initializer
- [#1853](https://github.com/meltano/sdk/issues/1853) Check against the unconformed key properties when validating record keys
- [#1843](https://github.com/meltano/sdk/issues/1843) Target template should not reference `tap_id`
- [#1708](https://github.com/meltano/sdk/issues/1708) Finalize and write last state message with dedupe
- [#1835](https://github.com/meltano/sdk/issues/1835) Avoid setting up mapper in discovery mode

### ⚙️ Under the Hood

- [#1877](https://github.com/meltano/sdk/issues/1877) Use `importlib.resources` instead of `__file__` to retrieve sample Singer output files

### 📚 Documentation Improvements

- [#1852](https://github.com/meltano/sdk/issues/1852) Fix stale `pip_url` example that uses shell script workaround for editable installation

## v0.30.0 (2023-07-10)

### ✨ New

- [#1815](https://github.com/meltano/sdk/issues/1815) Support optional headers for OAuth request -- _**Thanks @s7clarke10!**_
- [#1800](https://github.com/meltano/sdk/issues/1800) Publish supported python versions in `--about`

### 🐛 Fixes

- [#1829](https://github.com/meltano/sdk/issues/1829) Update cookiecutter copyright assignment to cookiecutter user -- _**Thanks @riordan!**_
- [#1826](https://github.com/meltano/sdk/issues/1826) Serialization of `decimal.Decimal`
- [#1827](https://github.com/meltano/sdk/issues/1827) Add explicit dependency on `packaging` library
- [#1820](https://github.com/meltano/sdk/issues/1820) Include SCHEMA message count in target logs

### 📚 Documentation Improvements

- [#1824](https://github.com/meltano/sdk/issues/1824) Document `RESTStream.rest_method`
- [#1818](https://github.com/meltano/sdk/issues/1818) Update testing.md

## v0.29.0 (2023-07-06)

### ✨ New

- [#1769](https://github.com/meltano/sdk/issues/1769) Validate parsed/transformed record against schema message
- [#1525](https://github.com/meltano/sdk/issues/1525) Support union schemas

### 🐛 Fixes

- [#1809](https://github.com/meltano/sdk/issues/1809) Deserialize floats as `decimal.Decimal`
- [#1770](https://github.com/meltano/sdk/issues/1770) Check schema has arrived before record
- [#1796](https://github.com/meltano/sdk/issues/1796) Create batch directory if missing
- [#1688](https://github.com/meltano/sdk/issues/1688) Incremental where clause generation from triggering TypeError -- _**Thanks @BuzzCutNorman!**_
- [#1778](https://github.com/meltano/sdk/issues/1778) Sink schema comparison before adding metadata columns
- [#1698](https://github.com/meltano/sdk/issues/1698) Force stream selection in tests
- [#1775](https://github.com/meltano/sdk/issues/1775) Add tests for SQL type conversion from JSON schemas
- [#1771](https://github.com/meltano/sdk/issues/1771) Add descriptions for `batch_config` properties
- [#1752](https://github.com/meltano/sdk/issues/1752) Change runner scope to function for target tests
- [#1753](https://github.com/meltano/sdk/issues/1753) Always emit a STATE message at the start of the sync process

### ⚙️ Under the Hood

- [#1745](https://github.com/meltano/sdk/issues/1745) Change `SQLStream.schema` into a cached property -- _**Thanks @mjsqu!**_

### 📚 Documentation Improvements

- [#1756](https://github.com/meltano/sdk/issues/1756) Fix invalid JSON in Stream Maps page and add `meltano.yml` tabs -- _**Thanks @mjsqu!**_
- [#1763](https://github.com/meltano/sdk/issues/1763) Add Cloud banner

## v0.28.0 (2023-06-05)

### ✨ New

- [#1728](https://github.com/meltano/sdk/issues/1728) Add an optional Dependabot file to projects generated from templates
- [#1572](https://github.com/meltano/sdk/issues/1572) Add `batch_config` handling in `append_builtin_config()` -- _**Thanks @aaronsteers!**_
- [#1686](https://github.com/meltano/sdk/issues/1686) Log stream errors
- [#1711](https://github.com/meltano/sdk/issues/1711) Validate records against stream schema in standard tap tests
- [#1709](https://github.com/meltano/sdk/issues/1709) Add a default Apache 2.0 license to tap and target templates

### 🐛 Fixes

- [#1742](https://github.com/meltano/sdk/issues/1742) Recommend `meltano run` in target cookiecutter README

### ⚙️ Under the Hood

- [#936](https://github.com/meltano/sdk/issues/936) Use inheritance to construct plugin CLI

### 📚 Documentation Improvements

- [#1721](https://github.com/meltano/sdk/issues/1721) Remove unsupported `previous_token` from HATEOAS example
- [#1703](https://github.com/meltano/sdk/issues/1703) Fix broken docs link for `record_metadata` page -- _**Thanks @menzenski!**_

## v0.27.0 (2023-05-11)

### ✨ New

- [#1681](https://github.com/meltano/sdk/issues/1681) Allow SQL tap developers to leverage `post_process` -- _**Thanks @BuzzCutNorman!**_
- [#1672](https://github.com/meltano/sdk/issues/1672) Support deselecting streams by default
- [#1648](https://github.com/meltano/sdk/issues/1648) Use Ruff to lint projects generated with Cookiecutter templates

### 🐛 Fixes

- [#1680](https://github.com/meltano/sdk/issues/1680) Pin `urllib3` to `<2` to avoid incompatibility issues with botocore
- [#1646](https://github.com/meltano/sdk/issues/1646) Use `get_new_paginator` in REST tap cookiecutter template

### ⚙️ Under the Hood

- [#1668](https://github.com/meltano/sdk/issues/1668) Break out default batch file writer into a separate class

### 📚 Documentation Improvements

- [#1685](https://github.com/meltano/sdk/issues/1685) Add PyCharm debugging tips to docs
- [#1673](https://github.com/meltano/sdk/issues/1673) Fix docs build by specifying OS in RTD config file

## v0.26.0 (2023-05-02)

### ✨ New

- [#1623](https://github.com/meltano/sdk/issues/1623) Explicitly support URL params in string form

## v0.25.0 (2023-04-25)

### ✨ New

- [#1603](https://github.com/meltano/sdk/issues/1603) Allow `allowed_values` and `examples` in any JSON schema type constructor

### ⚙️ Under the Hood

- [#1610](https://github.com/meltano/sdk/issues/1610) Consolidate config parsing for all plugin base classes

## v0.24.0 (2023-04-12)

### ✨ New

- [#1601](https://github.com/meltano/sdk/issues/1601) Allow skipping child streams by returning an empty child context from parent stream
- [#1581](https://github.com/meltano/sdk/issues/1581) Add `pattern`, `contentMediaType`, and `contentEncoding` to Schema data class -- _**Thanks @BuzzCutNorman!**_

### 🐛 Fixes

- [#1587](https://github.com/meltano/sdk/issues/1587) Update cookiecutter tests path

### ⚙️ Under the Hood

- [#1570](https://github.com/meltano/sdk/issues/1570) Move "about" formatting logic into dedicated classes

## v0.23.0 (2023-04-04)

### ✨ New

- [#1563](https://github.com/meltano/sdk/issues/1563) Migrate shell scripts for cookiecutter e2e tests to Nox -- _**Thanks @mkranna!**_

### 🐛 Fixes

- [#1574](https://github.com/meltano/sdk/issues/1574) Conform metric field `type` to Singer spec
- [#1436](https://github.com/meltano/sdk/issues/1436) Handle sync abort, reduce duplicate `STATE` messages, rename `_MAX_RECORD_LIMIT` as `ABORT_AT_RECORD_COUNT`

## v0.22.1 (2023-03-28)

### 🐛 Fixes

- [#1172](https://github.com/meltano/sdk/issues/1172) Handle merging of SQL types when character column lengths are less than the max -- _**Thanks @BuzzCutNorman!**_
- [#1524](https://github.com/meltano/sdk/issues/1524) Preserve `__alias__` when mapping streams with repeated schema messages -- _**Thanks @DanilJr!**_
- [#1526](https://github.com/meltano/sdk/issues/1526) Handle missing `type` value when checking JSON schema types

### 📚 Documentation Improvements

- [#1553](https://github.com/meltano/sdk/issues/1553) Change link color from pink to blue
- [#1544](https://github.com/meltano/sdk/issues/1544) Update branding colors in docs site
- [#1518](https://github.com/meltano/sdk/issues/1518) Fix HATEOAS pagination example

## v0.22.0 (2023-03-14)

### ✨ New

- [#1478](https://github.com/meltano/sdk/issues/1478) Retry some streaming and decoding request errors -- _**Thanks @visch!**_
- [#1480](https://github.com/meltano/sdk/issues/1480) Added `RESTStream.backoff_jitter` to support custom backoff jitter generators -- _**Thanks @visch!**_
- [#1438](https://github.com/meltano/sdk/issues/1438) Cookiecutter target tox ini -- _**Thanks @mkranna!**_

### 🐛 Fixes

- [#1467](https://github.com/meltano/sdk/issues/1467) Move `pyarrow` and `viztracer` extras to main dependencies
- [#1487](https://github.com/meltano/sdk/issues/1487) Address SQLAlchemy 2.0 deprecation warnings
- [#1482](https://github.com/meltano/sdk/issues/1482) Use pipx to run tox in CI template
- [#1454](https://github.com/meltano/sdk/issues/1454) Cookiecutter bearer auth config -- _**Thanks @radbrt!**_
- [#1434](https://github.com/meltano/sdk/issues/1434) Tap template: fix style and docstrings, and add test cases for SQL and "Other" sources -- _**Thanks @flexponsive!**_

### 📚 Documentation Improvements

- [#1492](https://github.com/meltano/sdk/issues/1492) Fix imports in pagination guide
- [#1446](https://github.com/meltano/sdk/issues/1446) Property conformance doc typo fix -- _**Thanks @radbrt!**_

## v0.21.0 (2023-02-21)

### 🐛 Fixes

- [#1410](https://github.com/meltano/sdk/issues/1410) Tap template: fix style, types and imports; and also catch more errors by building from replay files in CI -- _**Thanks @flexponsive!**_
- [#1428](https://github.com/meltano/sdk/issues/1428) Tap template: cover all REST authentication cases, and one GraphQL case -- _**Thanks @flexponsive!**_

## v0.20.0 (2023-02-13)

### ✨ New

- [#1365](https://github.com/meltano/sdk/issues/1365) Add `strptime_to_utc` and `strftime` functions to `_singerlib.utils` -- _**Thanks @menzenski!**_
- [#1394](https://github.com/meltano/sdk/issues/1394) Refactor SQLConnector connection handling -- _**Thanks @qbatten!**_
- [#1241](https://github.com/meltano/sdk/issues/1241) Support declaring variant for use in package name
- [#1109](https://github.com/meltano/sdk/issues/1109) Support `requests.auth` authenticators

### 🐛 Fixes

- [#1380](https://github.com/meltano/sdk/issues/1380) Move tests in cookiecutters to project root to support `pytest_plugins`
- [#1406](https://github.com/meltano/sdk/issues/1406) Use a version of `isort` compatible with Python 3.8
- [#1385](https://github.com/meltano/sdk/issues/1385) SQL Targets ignore collation when evaluating column data types -- _**Thanks @BuzzCutNorman!**_
- [#1342](https://github.com/meltano/sdk/issues/1342) Remove SQLSink snakecase conform in favor of simpler transformations
- [#1364](https://github.com/meltano/sdk/issues/1364) TapDiscoveryTest remove catalog if one is passed

### 📚 Documentation Improvements

- [#1390](https://github.com/meltano/sdk/issues/1390) Add incremental replication example -- _**Thanks @flexponsive!**_

## v0.19.0 (2023-01-30)

### ✨ New

- [#1171](https://github.com/meltano/sdk/issues/1171) Improve included tap and target tests in `singer_sdk.testing`

### 🐛 Fixes

- [#1345](https://github.com/meltano/sdk/issues/1345) Remove tox dependency from tap/target template

### 📚 Documentation Improvements

- [#1358](https://github.com/meltano/sdk/issues/1358) Fix typo in `if __name__ == ` example

## v0.18.0 (2023-01-23)

### ✨ New

- [#1283](https://github.com/meltano/sdk/issues/1283) Automatic catalog selection of replication keys

### 📚 Documentation Improvements

- [#1335](https://github.com/meltano/sdk/issues/1335) Stream maps example for adding property with hardcoded string value

## v0.17.0 (2023-01-06)

### 🐛 Fixes

- [#1308](https://github.com/meltano/sdk/issues/1308) Replace hyphens with underscores when generating expected env var name `<PLUGIN_NAME>_LOGLEVEL` -- _**Thanks @adherr!**_
- [#887](https://github.com/meltano/sdk/issues/887) Make `conform_record_data_types` work on nested objects and arrays -- _**Thanks @Jack-Burnett!**_
- [#1287](https://github.com/meltano/sdk/issues/1287) Targets to fail gracefully when schema message is missing the `properties` key -- _**Thanks @visch!**_

### 📚 Documentation Improvements

- [#1293](https://github.com/meltano/sdk/issues/1293) Add link to the [EDK](https://edk.meltano.com)

## v0.16.0 (2022-12-19)

### ✨ New

- [#1262](https://github.com/meltano/sdk/issues/1262) Support string `"__NULL__"` whereever null values are allowed in stream maps configuration

### 🐛 Fixes

- [#1281](https://github.com/meltano/sdk/issues/1281) Apply Version bump commit file perms with sudo
- [#1280](https://github.com/meltano/sdk/issues/1280) Set repo file perms after checkout in Version bump workflow
- [#1214](https://github.com/meltano/sdk/issues/1214) Avoid duplicate entries in `required` array of JSON schema helpers

## v0.15.0 (2022-12-08)

### ✨ New

- [#1188](https://github.com/meltano/sdk/issues/1188) Support boolean `additional_properties` in JSON schema helper objects
- [#1237](https://github.com/meltano/sdk/issues/1237) Catch and retry `ConnectionResetError` exceptions in HTTP taps
- [#1087](https://github.com/meltano/sdk/issues/1087) S3 batch storage -- _**Thanks @jamielxcarter!**_
- [#1197](https://github.com/meltano/sdk/issues/1197) Support `patternProperties` in JSON schema helpers
- [#1157](https://github.com/meltano/sdk/issues/1157) Built-in handling of `default-target-schema` for SQL Targets -- _**Thanks @BuzzCutNorman!**_

### 🐛 Fixes

- [#1238](https://github.com/meltano/sdk/issues/1238) Ensure metric tags coming from stream context can be JSON-serialized
- [#1233](https://github.com/meltano/sdk/issues/1233) Add level and logger name to default log format
- [#1219](https://github.com/meltano/sdk/issues/1219) Schema passthrough for whitelisted fields
- [#1174](https://github.com/meltano/sdk/issues/1174) Do not emit log message if no record properties were ignored
- [#1192](https://github.com/meltano/sdk/issues/1192) Change max record age for emitting state messages to 5 instead of 30 mins -- _**Thanks @spacecowboy!**_

### ⚡ Performance Improvements

- [#1196](https://github.com/meltano/sdk/issues/1196) Improve performance of record message serialization -- _**Thanks @Jack-Burnett!**_

### 📚 Documentation Improvements

- [#1243](https://github.com/meltano/sdk/issues/1243) Document inherited `PluginBase` attributes and methods
- [#1209](https://github.com/meltano/sdk/issues/1209) Fix argument descriptions for `OAuthAuthenticator`

## v0.14.0 (2022-11-16)

### ✨ New

- [#1175](https://github.com/meltano/sdk/issues/1175) Add `datetime` functions to simpleeval env in stream maps -- _**Thanks @qbatten!**_

### 🐛 Fixes

- [#1182](https://github.com/meltano/sdk/issues/1182) Update `SQLConnector` import for SQL target cookiecutter -- _**Thanks @radbrt!**_
- [#1168](https://github.com/meltano/sdk/issues/1168) `SQLConnector.table_exists()` to use separate `table_name` and `schema_name` instead of fully qualified name -- _**Thanks @BuzzCutNorman!**_
- [#1164](https://github.com/meltano/sdk/issues/1164) Write a valid final state message at the end of each stream sync -- _**Thanks @laurentS!**_

### ⚙️ Under the Hood

- [#1114](https://github.com/meltano/sdk/issues/1114) Make DDL overridable for column `ADD`, `ALTER`, and `RENAME` operations

## v0.13.1 (2022-11-08)

### 🐛 Fixes

- [#1126](https://github.com/meltano/sdk/issues/1126) Resolve failure in `_increment_stream_state()` for cases when `replication_method` is `LOG_BASED`
- [#1111](https://github.com/meltano/sdk/issues/1111) Push `_MAX_RECORDS_LIMIT` down into SQL

### ⚙️ Under the Hood

- [#1091](https://github.com/meltano/sdk/issues/1091) Move SQLConnector into a separate module, for use by both SQLStream and SQLSink

### 📚 Documentation Improvements

- [#1133](https://github.com/meltano/sdk/issues/1133) Fix duplicate `Known Limitations` header
- [#1118](https://github.com/meltano/sdk/issues/1118) Document `BATCH` limitations

## v0.13.0 (2022-10-24)

### ✨ New

- [#1098](https://github.com/meltano/sdk/issues/1098) Add JSON Schema `Property` helpers for `allowed_values` (`enum`) and `examples`
- [#1096](https://github.com/meltano/sdk/issues/1096) Add secrets support for tap and target config, via `Property(..., secret=True)`
- [#1039](https://github.com/meltano/sdk/issues/1039) Support conforming singer property names to target identifier constraints in SQL sinks

### 🐛 Fixes

- [#1093](https://github.com/meltano/sdk/issues/1093) Add environment support to the cookie cutter for `meltano.yml`
- [#1036](https://github.com/meltano/sdk/issues/1036) Create schema and table on `add_sink`

## v0.12.0 (2022-10-17)

### ✨ New

- [#1032](https://github.com/meltano/sdk/issues/1032) Support stream property selection push-down in SQL streams
- [#978](https://github.com/meltano/sdk/issues/978) Allow configuring a dedicated metrics logger

### 🐛 Fixes

- [#1043](https://github.com/meltano/sdk/issues/1043) Batch storage `split_url` to work with Windows paths -- _**Thanks @BuzzCutNorman!**_
- [#826](https://github.com/meltano/sdk/issues/826) Remove Poetry version pin for GitHub Actions -- _**Thanks @visch!**_
- [#1001](https://github.com/meltano/sdk/issues/1001) Use column name in `allow_column_alter` error message

### 📚 Documentation Improvements

- [#1060](https://github.com/meltano/sdk/issues/1060) Add explanation and recommendations for context usage
- [#1074](https://github.com/meltano/sdk/issues/1074) Document an example implementation and usage of `BaseHATEOASPaginator`
- [#1020](https://github.com/meltano/sdk/issues/1020) Fixed typo in `docs/stream_maps.md` -- _**Thanks @spacecowboy!**_
- [#1006](https://github.com/meltano/sdk/issues/1006) Add links to Meltano install/tut

## v0.11.1 (2022-09-27)

### 🐛 Fixes

- [#999](https://github.com/meltano/sdk/issues/999) Absolute file paths created by taps running in BATCH mode can't be processed by the Sink

### 📚 Documentation Improvements

- Change `targetUrl` of semantic PR title check to point to SDK docs

## v0.11.0 (2022-09-23)

### ✨ New

- [#968](https://github.com/meltano/sdk/issues/968) Added cookiecutter support and docs for VSCode debugging
- [#904](https://github.com/meltano/sdk/issues/904) Add support for new `BATCH` message type in taps and targets

### 🐛 Fixes

- [#972](https://github.com/meltano/sdk/issues/972) Resolve issue where TypeError is thrown by SQLConnector cookiecutter implementation due to super() references

### 📚 Documentation Improvements

- [#988](https://github.com/meltano/sdk/issues/988) Add pipe before SDK logo in header
- [#970](https://github.com/meltano/sdk/issues/970) Move cookiecutter TODOs into markdown comments

## v0.10.0 (2022-09-12)

### ✨ New

- [#829](https://github.com/meltano/sdk/issues/829) Add checks for primary keys, replication keys and state partitioning keys to standard tap tests -- _**Thanks @laurentS!**_
- [#732](https://github.com/meltano/sdk/issues/732) Implement reference paginators.

### 🐛 Fixes

- [#898](https://github.com/meltano/sdk/issues/898) Fix SQL type merging for pre-existing target tables -- _**Thanks @BuzzCutNorman!**_
- [#856](https://github.com/meltano/sdk/issues/856) Fix typo RecordsWitoutSchemaException -> RecordsWithoutSchemaException.

### ⚙️ Under the Hood

- Use `__future__.annotations` on `singer_sdk.helpers._singer`

### 📚 Documentation Improvements

- [#950](https://github.com/meltano/sdk/issues/950) Document missing initializers for authentication and pagination helpers.
- [#947](https://github.com/meltano/sdk/issues/947) Remove stale autodoc page for RecordsWitoutSchemaException.
- [#942](https://github.com/meltano/sdk/issues/942) Add docs preview links to PR description.

## v0.9.0 (2022-08-24)

### ✨ New

- [#842](https://github.com/meltano/sdk/issues/842) Allow authenticating more generic requests
- [#919](https://github.com/meltano/sdk/issues/919) add `ConnectionError` to list of backoff exceptions for auto-retry

### 🐛 Fixes

- [#917](https://github.com/meltano/sdk/issues/917) Allow Singer schemas to include the `required` and `enum` fields
- [#759](https://github.com/meltano/sdk/issues/759) Use recent start_date as starting_replication_value

### ⚙️ Under the Hood

- [#908](https://github.com/meltano/sdk/issues/908) Allow overriding the bulk insert statement in `SQLSink`

### 📚 Documentation Improvements

- [#914](https://github.com/meltano/sdk/issues/914) Bump Pygments and update dbt example
- [#900](https://github.com/meltano/sdk/issues/900) Generate documentation for constructor parameters

## v0.8.0 (2022-08-05)

### 🐛 Fixes

- [#784](https://github.com/meltano/sdk/issues/784) Update return type for `backoff_max_tries` to reflect it accepts a callable that returns an integer
- [#874](https://github.com/meltano/sdk/issues/874) Singer metrics are now properly emitted in JSON format --_Thanks, **@Jack-Burnett!**_

### 📚 Documentation Improvements

- [#869](https://github.com/meltano/sdk/issues/869) Cleanup whitespace in backoff code samples

## v0.7.0 (2022-07-21)

### ✨ New

- [#785](https://github.com/meltano/sdk/issues/785) Output full URL path in error messages

### 🐛 Fixes

- [#815](https://github.com/meltano/sdk/issues/815) Generate correct SQL target project from cookiecutter
- [#782](https://github.com/meltano/sdk/issues/782) Allow lists and dictionaries as types for default JSON values

### 📚 Documentation Improvements

- [#823](https://github.com/meltano/sdk/issues/823) Add link to the sdk for README generation regarding Stream Maps
- [#813](https://github.com/meltano/sdk/issues/813) Fix PyPI trove classifiers
- [#783](https://github.com/meltano/sdk/issues/783) Document using pipx inject for nox-poetry

## v0.6.1 (2022-07-01)

### Fix

- Fix missing typing-extensions for Python<3.10 (#776)

## 0.6.0 - (2022-06-30)

---

### New

- Tap and Target SDK: Adds the ability to override the [logging level](https://sdk.meltano.com/en/latest/implementation/logging.html) via `LOGLEVEL` environment variables. ([!300](https://gitlab.com/meltano/sdk/-/merge_requests/300)) - Thanks, _**[Eric Boucher](https://gitlab.com/ericboucher)**_!
- Tap SDK: Allow sort checking to be disabled for taps with un-sortable replication keys. ([#729](https://github.com/meltano/sdk/issues/729), [#730](https://github.com/meltano/sdk/pull/730)) - Thanks, _**[Ilkka](https://github.com/ilkkapeltola)**_!
- Tap SDK: Add `Stream.calculate_sync_cost` hook to gather "cost" (in terms of number of requests, etc.) for each stream. ([#348](https://github.com/meltano/sdk/issues/348), [#704](https://github.com/meltano/sdk/pull/704)) - Thanks, _**[Laurent Savaëte](https://github.com/laurentS)**_!
- Target SDK: Add end-of-pipe clean up hooks for `Sink` implementations ([#750](https://github.com/meltano/sdk/pull/750)) - Thanks, _**[Alexander Butler](https://github.com/z3z1ma)**_!

### Changes

- Tap SDK: Bump minimum version of `PyJWT` to `2.4` ([#396](https://gitlab.com/meltano/sdk/-/issues/396), [!302](https://gitlab.com/meltano/sdk/-/merge_requests/302)).
- Tap SDK: Compare replication key values when incrementing state, only if the `check_sorted` flag is set to True ([#736](https://github.com/meltano/sdk/issues/736), [#737](https://github.com/meltano/sdk/pull/737)).

### Fixes

- Tap and Target SDK: Resolves an issue where SQLAlchemy logs would be printed to `STDOUT`. ([!303](https://gitlab.com/meltano/sdk/-/merge_requests/303)) - Thanks, _**[Andrey Tatarinov](https://gitlab.com/elephantum)**_!
- Tap SDK: Reduces number of log lines printed when unmapped properties are ignored from the source API ([!300](https://gitlab.com/meltano/sdk/-/merge_requests/300)) - Thanks, _**[Eric Boucher](https://gitlab.com/ericboucher)**_!
- Tap and Target SDK: Remove trailing parenthesis from logged version ([#766](https://github.com/meltano/sdk/issues/766), [#767](https://github.com/meltano/sdk/pull/767)).

## 0.5.0 - (2022-05-19)

---

### New

- Tap and Target SDK: The `--config=ENV` option now also considers environment variables from a
  dotenv (`.env`) file if present ([#384](https://gitlab.com/meltano/sdk/-/issues/384), [!293](https://gitlab.com/meltano/sdk/-/merge_requests/293)).

### Changes

- Target SDK: Use generic SQLALchemy markers for parameterized queries in SQL targets ([#376](https://gitlab.com/meltano/sdk/-/issues/376), [!287](https://gitlab.com/meltano/sdk/-/merge_requests/287)) - _Thanks, **[Thomas Briggs](https://gitlab.com/tbriggs2)**_!

- Target SDK: Explicitly specify column names when inserting rows in SQL targets ([#385](https://gitlab.com/meltano/sdk/-/issues/385), [!294](https://gitlab.com/meltano/sdk/-/merge_requests/294)) - _Thanks, **[Thomas Briggs](https://gitlab.com/tbriggs2)**_!

### Fixes

- Target SDK: Fixes bug where an `activate_version` message caused an error when destination table does not exist ([#372](https://gitlab.com/meltano/sdk/-/issues/372), [!285](https://gitlab.com/meltano/sdk/-/merge_requests/285)) - _Thanks, **[Thomas Briggs](https://gitlab.com/tbriggs2)**_!

- Tap and Target SDK: Do not fail `--about` option if tap or target is not configured ([#379](https://gitlab.com/meltano/sdk/-/issues/379), [!291](https://gitlab.com/meltano/sdk/-/merge_requests/291)).

## 0.4.9 - (2022-05-12)

---

### New

- Tap SDK: Improved helpers for handling rate limits, backoff and retries ([#137](https://gitlab.com/meltano/sdk/-/issues/137), [#140](https://gitlab.com/meltano/sdk/-/issues/140), [!277](https://gitlab.com/meltano/sdk/-/merge_requests/277)) - _Thanks, **[Fred O'Loughlin](https://gitlab.com/fred-oloughlin-fl)**_!

### Changes

- Remove "errors" from non-error log line to improve log searchability ([!279](https://gitlab.com/meltano/sdk/-/merge_requests/279/diffs)) - _Thanks, **[Laurent Savaëte](https://gitlab.com/LaurentS)**_!

### Fixes

- Tap and Target SDK: Fixed a bug where setting stream map property to `null` did not remove it from SCHEMA message ([#370](https://gitlab.com/meltano/sdk/-/issues/370), [!286](https://gitlab.com/meltano/sdk/-/merge_requests/286)) - _Thanks, **[Ryan Whitten](https://gitlab.com/rwhitten577)**_!
- Tap and Target SDK: Fixed a bug where flattening resulted in an invalid SCHEMA message ([!286](https://gitlab.com/meltano/sdk/-/merge_requests/286)) - _Thanks, **[Ryan Whitten](https://gitlab.com/rwhitten577)**_!

## 0.4.8 - (2022-05-05)

---

### Fixes

- Target SDK: Use `maxLength` in SQL targets for string fields if the schema provides it ([#371](https://gitlab.com/meltano/sdk/-/issues/371), [!284](https://gitlab.com/meltano/sdk/-/merge_requests/284)) - _Thanks, **[Thomas Briggs](https://gitlab.com/tbriggs2)**_!

## 0.4.7 - (2022-04-28)

---

### Fixes

- Tap Cookiecutter: Fixed a bug where the wrong key was used to select the appropriate `Tap` class for SQL taps ([#353](https://gitlab.com/meltano/sdk/-/issues/353), [!275](https://gitlab.com/meltano/sdk/-/merge_requests/275)) - _Thanks, **[Dan Norman](https://gitlab.com/BuzzCutNorman)**_!
- Tap Cookiecutter: Fixed a bug where `client.py` for SQL taps was overwritten during initialization ([#354](https://gitlab.com/meltano/sdk/-/issues/354), [!276](https://gitlab.com/meltano/sdk/-/merge_requests/276)) - _Thanks, **[Dan Norman](https://gitlab.com/BuzzCutNorman)**_!
- Tap SDK: Fixed a bug where a parent stream emitted schema messages when it's not selected, but at least one of its child streams is ([#366](https://gitlab.com/meltano/sdk/-/issues/366), [!280](https://gitlab.com/meltano/sdk/-/merge_requests/280))
- Tap SDK: Bump `pyjwt` dependency to `~=2.3` ([!281](https://gitlab.com/meltano/sdk/-/merge_requests/281)) - _Thanks, **[Eric Boucher](https://gitlab.com/ericboucher)**_!

## 0.4.6 - (2022-04-21)

---

### Fixes

- Raise more descriptive exceptions when wrapped JSON typing classes needs to be instantiated ([#55](https://gitlab.com/meltano/sdk/-/issues/55), [#360](https://gitlab.com/meltano/sdk/-/issues/360), [!270](https://gitlab.com/meltano/sdk/-/merge_requests/270)).
- Support JSONPath extensions in `records_jsonpath` and `next_page_token_jsonpath` ([#361](https://gitlab.com/meltano/sdk/-/issues/361), [!271](https://gitlab.com/meltano/sdk/-/merge_requests/271)).

## 0.4.5 - (2022-04-08)

---

### Fixes

- Fixed a bug where setting a stream map property to null did not remove the property ([#352](https://gitlab.com/meltano/sdk/-/issues/352), [!263](https://gitlab.com/meltano/sdk/-/merge_requests/263)).
- Avoid SQLAlchemy deprecation warning caused by using `Engine.has_table()` ([#341](https://gitlab.com/meltano/sdk/-/issues/341), [!264](https://gitlab.com/meltano/sdk/-/merge_requests/264))
- Resolve issue where "falsey" defaults like '0', '', and 'False' would not be properly applied to tap settings config. ([#357](https://gitlab.com/meltano/sdk/-/issues/357), [!265](https://gitlab.com/meltano/sdk/-/merge_requests/265))
- Return to stable `poetry-core` version in cookiecutter templates ([#338](https://gitlab.com/meltano/sdk/-/issues/338), [!260](https://gitlab.com/meltano/sdk/-/merge_requests/260))

## 0.4.4 - (2022-03-03)

---

### New

- Define all built-in JSON Schema string formats as separate types ([#336](https://gitlab.com/meltano/sdk/-/issues/336), [!250](https://gitlab.com/meltano/sdk/-/merge_requests/250)) - _Thanks, **[Reuben Frankel](https://gitlab.com/ReubenFrankel)**_!

## 0.4.3 - (2022-02-18)

---

### New

- Enable JSONPath for GraphQL record extraction ([#327](https://gitlab.com/meltano/sdk/-/issues/327), [!247](https://gitlab.com/meltano/sdk/-/merge_requests/247)) - _Thanks, **[Fred O'Loughlin](https://gitlab.com/fred-oloughlin-fl)**_!

### Changes

- Deprecate Python 3.6 ([#316](https://gitlab.com/meltano/sdk/-/issues/316), [!246](https://gitlab.com/meltano/sdk/-/merge_requests/246))

## 0.4.2 - (2022-02-04)

---

### New

- Add record and schema flattening in Stream Maps ([!236](https://gitlab.com/meltano/sdk/-/merge_requests/236)),

### Fixes

- Resolve issues when aliasing stream maps using the keywords `__alias__`, `__source__`, or `__else__` ([#301](https://gitlab.com/meltano/sdk/-/issues/301), [#302](https://gitlab.com/meltano/sdk/-/issues/302), [!243](https://gitlab.com/meltano/sdk/-/merge_requests/243))

## 0.4.1 - (2022-01-27)

---

### Changes

- Always sync one record per stream when invoking with `--test` or `--test=all` ([#311](https://gitlab.com/meltano/sdk/-/issues/311), [!241](https://gitlab.com/meltano/sdk/-/merge_requests/241))
- Add `--test=schema` option to emit tap SCHEMA messages only ([!218](https://gitlab.com/meltano/sdk/-/merge_requests/218)) - _Thanks, **[Laurent Savaëte](https://gitlab.com/LaurentS)**_!

## 0.4.0 - (2022-01-21)

---

### New

- Add support for SQL Taps ([#74](https://gitlab.com/meltano/sdk/-/issues/74), [!44](https://gitlab.com/meltano/sdk/-/merge_requests/44))
- Add support for SQL Targets ([#263](https://gitlab.com/meltano/sdk/-/issues/263), [!44](https://gitlab.com/meltano/sdk/-/merge_requests/44), [!200](https://gitlab.com/meltano/sdk/-/merge_requests/200), [!239](https://gitlab.com/meltano/sdk/-/merge_requests/239))
- Added Licence tracking to SDK GitLab Project ([#166](https://gitlab.com/meltano/sdk/-/issues/166), [!237](https://gitlab.com/meltano/sdk/-/merge_requests/237))

## 0.3.18 - (2022-01-13)

---

### New

- Inline Mapper SDK: Support for creation of inline mapper plugins ([#257](https://gitlab.com/meltano/sdk/-/issues/257), [!234](https://gitlab.com/meltano/sdk/-/merge_requests/234))
- Tap and Target Cookiecutter: Support editable pip installs (`pip install -e .`) with [PEP 660](https://www.python.org/dev/peps/pep-0660/) ([#238](https://gitlab.com/meltano/sdk/-/issues/238), [!231](https://gitlab.com/meltano/sdk/-/merge_requests/231))

### Fixes

- Tap Cookiecutter: Add output directory for `target-jsonl` ([!228](https://gitlab.com/meltano/sdk/-/merge_requests/228)) -- _Thanks, **[Niall Woodward](https://gitlab.com/NiallRees)**!_
- Tap SDK: Make the `expires_in` property optional in OAuth response ([#297](https://gitlab.com/meltano/sdk/-/issues/297), [!232](https://gitlab.com/meltano/sdk/-/merge_requests/232)) -- _Thanks, **[Daniel Ferguson](https://gitlab.com/daniel-ferguson)**!_

## 0.3.17 - (2021-12-16)

---

### New

- Tap SDK: Add configurable timeout for HTTP requests ([#287](https://gitlab.com/meltano/sdk/-/issues/287), [!217](https://gitlab.com/meltano/sdk/-/merge_requests/217), [!225](https://gitlab.com/meltano/sdk/-/merge_requests/225)) -- _Thanks, **[Josh Lloyd](https://gitlab.com/jlloyd3)**!_
- Tap and Target SDK: Adds support for Python 3.10 ([#293](https://gitlab.com/meltano/sdk/-/issues/293), [!224](https://gitlab.com/meltano/sdk/-/merge_requests/224))

### Fixes

- Resolve lint errors when ArrayType is used to wrap other types ([!223](https://gitlab.com/meltano/sdk/-/merge_requests/223)) -- _Thanks, **[David Wallace](https://gitlab.com/dwallace0723)**!_

## 0.3.16 - (2021-12-09)

---

### Fixes

- Tap SDK: Fix datelike type parsing bug with nested schemas ([#283](https://gitlab.com/meltano/sdk/-/issues/283), [!219](https://gitlab.com/meltano/sdk/-/merge_requests/219))
- Tap SDK: Resolved bug in `--test` which caused child streams to not use record limiting ([#268](https://gitlab.com/meltano/sdk/-/issues/268), [!204](https://gitlab.com/meltano/sdk/-/merge_requests/204), [!220](https://gitlab.com/meltano/sdk/-/merge_requests/220)) -- _Thanks, **[Derek Visch](https://gitlab.com/vischous)**!_

## 0.3.15 - (2021-12-03)

---

### Fixes

- Tap SDK: Fixed mapped `__key_properties__` not being passed to the emitted schema message ([#281](https://gitlab.com/meltano/sdk/-/issues/281), [!209](https://gitlab.com/meltano/sdk/-/merge_requests/209))
- Tap SDK: Fixed missing schema during development causing sync to fail [#284](https://gitlab.com/meltano/sdk/-/issues/284), [!212](https://gitlab.com/meltano/sdk/-/merge_requests/212) -- _Thanks, **[Fred Reimer](https://gitlab.com/freimer)**!_

## 0.3.14 - (2021-11-18)

---

### New

- Tap SDK: New method `RESTStream.validate_response` for custom validation of HTTP responses ([#207](https://gitlab.com/meltano/sdk/-/issues/207), [!195](https://gitlab.com/meltano/sdk/-/merge_requests/195))
- Tap SDK: New method `RESTStream.request_decorator` for custom back-off and retry parameters ([#137](https://gitlab.com/meltano/sdk/-/issues/137), [!195](https://gitlab.com/meltano/sdk/-/merge_requests/195))

### Changes

- Target SDK: Split sink classes into separate modules ([#264](https://gitlab.com/meltano/sdk/-/issues/264), [!201](https://gitlab.com/meltano/sdk/-/merge_requests/201))

### Fixes

- Target SDK: Document options for the target CLI and accept multiple config files as input ([!183](https://gitlab.com/meltano/sdk/-/merge_requests/183))

## 0.3.13 - (2021-10-28)

---

### New

- Target SDK: CLI flag for targets to read messages from a file instead of stdin ([#249](https://gitlab.com/meltano/sdk/-/issues/249), [!190](https://gitlab.com/meltano/sdk/-/merge_requests/190)) -- _Thanks, **[Charles Julian Knight](https://gitlab.com/rabidaudio)**!_
- Target SDK: Add target mock classes and tap-to-target scenario tests ([#198](https://gitlab.com/meltano/sdk/-/issues/198), [!138](https://gitlab.com/meltano/sdk/-/merge_requests/138))
- Tap and Target SDK: Create expanded list of capabilities ([#186](https://gitlab.com/meltano/sdk/-/issues/186), [!141](https://gitlab.com/meltano/sdk/-/merge_requests/141))

## 0.3.12 - (2021-10-21)

---

### Fixes

- Tap and Target SDK: Fix markdown table formatting in `--about` for multi-line settings descriptions ([#240](https://gitlab.com/meltano/sdk/-/issues/240), [!185](https://gitlab.com/meltano/sdk/-/merge_requests/185))
- Tap SDK: Clarify undocumented feature of filtering `None` from `post_process()` ([#233](https://gitlab.com/meltano/sdk/-/issues/233), [!187](https://gitlab.com/meltano/sdk/-/merge_requests/187))
- Tap and Target SDK: Add `dataclasses` as an explicit third-party dependency for Python 3.6 ([#245](https://gitlab.com/meltano/sdk/-/issues/245), [!189](https://gitlab.com/meltano/sdk/-/merge_requests/189))
- Tap and Target SDK: Allows `--discover` and `--about` execution without requiring settings validation ([#235](https://gitlab.com/meltano/sdk/-/issues/235), [!188](https://gitlab.com/meltano/sdk/-/merge_requests/188))

## 0.3.11 - (2021-10-07)

---

### New

- Tap and Target SDK: Adds capability to print markdown docs with `--about --format=markdown` ([!172](https://gitlab.com/meltano/sdk/-/merge_requests/172), [!180](https://gitlab.com/meltano/sdk/-/merge_requests/180)) -- _Thanks, **[Nick Müller](https://gitlab.com/muellernick1994)**!_

### Changes

- Tap and Target SDK: Autogenerated docstrings for arguments, return types, and exceptions raised ([!166](https://gitlab.com/meltano/sdk/-/merge_requests/166)).
- Tap and Target SDK: Support Black by default by bumping min Python version to 3.6.2. (#224, !169)

### Fixes

- Fixes a bug where tox invocations after initial setup failed ([!179](https://gitlab.com/meltano/sdk/-/merge_requests/179)) -- _Thanks, **[Jon Watson](https://gitlab.com/jawats)**!_.
- Tap SDK: Fixes a bug in `Stream.get_starting_timestamp()` and `Stream.get_starting_replication_key_value()` calls where results where not cached breaking stream sorting ([!157](https://gitlab.com/meltano/sdk/-/merge_requests/157))

## 0.3.10 - (2021-09-30)

---

### Changes

- Tap and Target SDK: Prevents the leaking of sensitive configuration values when JSON schema validation fails ([!173](https://gitlab.com/meltano/sdk/-/merge_requests/173)) -- _Thanks, **[Kevin Mullins](https://gitlab.com/zyzil)**!_.

## 0.3.9 - (2021-09-23)

---

### New

- Add description attribute to `Property` class for JSON schemas ([#159](https://gitlab.com/meltano/sdk/-/issues/159), [!164](https://gitlab.com/meltano/sdk/-/merge_requests/164)) -- _Thanks, **[Stephen Bailey](https://gitlab.com/stkbailey)**!_

### Changes

- Tap SDK: Set `key_properties = []` instead of `null` per the Singer spec ([!160](https://gitlab.com/meltano/sdk/-/merge_requests/160)) -- _Thanks, **[Niall Woodward](https://gitlab.com/NiallRees)**!_

### Fixes

- Tap SDK: Fixes issue where stream map schema generation fails when overriding the value of an existing property. ([#196](https://gitlab.com/meltano/sdk/-/issues/196), [!165](https://gitlab.com/meltano/sdk/-/merge_requests/165))

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
