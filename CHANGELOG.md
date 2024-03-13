## v0.36.1 (2024-03-13)

### 🐛 Fixes

- [#2310](https://github.com/meltano/sdk/issues/2310) Limited `importlib-resources` to < 6.2 due to breaking changes
- [#2288](https://github.com/meltano/sdk/issues/2288) Bumped min joblib dependency to `>=1.3.0` and replaced deprecated `parallel_backend` with `parallel_config` -- _**Thanks @BuzzCutNorman!**_
- [#2281](https://github.com/meltano/sdk/issues/2281) The `state_partition_context` dictionary is now correctly interpolated in the error message when duplicate partitions/contexts are detected in the input state
- [#2274](https://github.com/meltano/sdk/issues/2274) Test workflow now fails for any Python matrix job failure in cookiecutter template -- _**Thanks @ReubenFrankel!**_

## v0.36.0 (2024-02-26)

### ✨ New

- [#2241](https://github.com/meltano/sdk/issues/2241) JSON schema keywords such as `maxLength` are now supported in `StringType`, `IntegerType` and `NumberType` JSON schema helpers
- [#2263](https://github.com/meltano/sdk/issues/2263) Nested settings are now documented in the output of `--about --format=markdown`
- [#2248](https://github.com/meltano/sdk/issues/2248) Targets now accept a `batch_size_rows` setting to configure how many rows are loaded in each record batch -- _**Thanks @BuzzCutNorman!**_

### 🐛 Fixes

- [#2258](https://github.com/meltano/sdk/issues/2258) Database disconnects are now handled via SQLAlchemy `pool_pre_ping` parameter

### ⚙️ Under the Hood

- [#2220](https://github.com/meltano/sdk/issues/2220) Deprecated `singer_sdk.authenticators.BasicAuthenticator` in favor of `requests.auth.HTTPBasicAuth`

## v0.35.2 (2024-02-19)

### 🐛 Fixes

- [#2252](https://github.com/meltano/sdk/issues/2252) Null type is now correctly appended as `{"type": "null"}` to `oneOf` types

## v0.35.1 (2024-02-15)

### 🐛 Fixes

- [#2243](https://github.com/meltano/sdk/issues/2243) Force flattened record according to provided flattened schema -- _**Thanks @joaopamaral!**_
- [#2245](https://github.com/meltano/sdk/issues/2245) Instances of `oneOf` are now handled by null-appending logic
- [#2242](https://github.com/meltano/sdk/issues/2242) Hard and soft deletes for handling `ACTIVATE_VERSION` messages now use the same `WHERE` clause -- _**Thanks @vitoravancini!**_
- [#2232](https://github.com/meltano/sdk/issues/2232) Test workflow job now fails for unsupported Python versions in cookiecutter templates -- _**Thanks @ReubenFrankel!**_
- [#2225](https://github.com/meltano/sdk/issues/2225) Support non-nullable but not required SQL columns

### 📚 Documentation Improvements

- [#2239](https://github.com/meltano/sdk/issues/2239) Linked reference docs to source code
- [#2231](https://github.com/meltano/sdk/issues/2231) Added an example implemetation of JSON schema validation that uses `fastjsonschema`
- [#2219](https://github.com/meltano/sdk/issues/2219) Add reference docs for tap & target testing helpers

## v0.35.0 (2024-02-05)

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
- [#2158](https://github.com/meltano/sdk/issues/2158) Fix pytest plugin declaration so it can be used without requiring declaration of `pytest_plugins`
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

### ✨ New

- [#1733](https://github.com/meltano/sdk/issues/1733) Test with Python 3.12 🐍

### 🐛 Fixes

- [#2118](https://github.com/meltano/sdk/issues/2118) Output JSONPath expression with match count message -- _**Thanks @mjsqu!**_
- [#2107](https://github.com/meltano/sdk/issues/2107) Respect forced replication method when retrieving state
- [#2094](https://github.com/meltano/sdk/issues/2094) Use `nulls_first` when available to order `NULL` results in incremental SQL streams

### ⚙️ Under the Hood

- [#2095](https://github.com/meltano/sdk/issues/2095) Use `CursorResult.mappings()` in SQL streams
- [#2092](https://github.com/meltano/sdk/issues/2092) Use `datetime.fromisoformat` in other places
- [#2090](https://github.com/meltano/sdk/issues/2090) Explicitly use `T` iso date separator

### 📚 Documentation Improvements

- [#2111](https://github.com/meltano/sdk/issues/2111) Fix broken requests documentation links -- _**Thanks @mjsqu!**_

## v0.34.0 (2023-12-05)

### ✨ New

- [#2044](https://github.com/meltano/sdk/issues/2044) Add Parquet as a batch encoding option -- _**Thanks @jamielxcarter!**_
- [#768](https://github.com/meltano/sdk/issues/768) Better error messages when config validation fails
- [#1854](https://github.com/meltano/sdk/issues/1854) Make stream logger a child of the tap logger

### 🐛 Fixes

- [#2076](https://github.com/meltano/sdk/issues/2076) Make a explicit dependency on `python-dateutil`

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

## v0.33.0 (2023-10-16)

### ✨ New

- [#2018](https://github.com/meltano/sdk/issues/2018) Drop Python 3.7 support in cookiecutter templates -- _**Thanks @visch!**_
- [#2003](https://github.com/meltano/sdk/issues/2003) Add ability to do list comprehensions in stream map expressions -- _**Thanks @haleemur!**_
- [#1999](https://github.com/meltano/sdk/issues/1999) Log JSONPatch match count at the INFO level
- [#1779](https://github.com/meltano/sdk/issues/1779) Cache SQL columns and schemas

### 🐛 Fixes

- [#2015](https://github.com/meltano/sdk/issues/2015) Ensure `default` property is passed to SCHEMA messages -- _**Thanks @prakharcode!**_
- [#2006](https://github.com/meltano/sdk/issues/2006) Parse record `time_extracted` into `datetime.datetime` instance
- [#1996](https://github.com/meltano/sdk/issues/1996) Respect nullability of leaf properties when flattening schema
- [#1844](https://github.com/meltano/sdk/issues/1844) Safely skip parsing record field as date-time if it is missing in schema
- [#1885](https://github.com/meltano/sdk/issues/1885) Map `record` field to a JSON `object` type

### 📚 Documentation Improvements

- [#2017](https://github.com/meltano/sdk/issues/2017) Document support for comprehenions in stream maps

## v0.32.0 (2023-09-22)

### ✨ New

- [#1893](https://github.com/meltano/sdk/issues/1893) Standard configurable load methods
- [#1861](https://github.com/meltano/sdk/issues/1861) SQLTap connector instance shared with streams -- _**Thanks @BuzzCutNorman!**_

### 🐛 Fixes

- [#1977](https://github.com/meltano/sdk/issues/1977) Fix hanging downstream tests in tap-postgres
- [#1970](https://github.com/meltano/sdk/issues/1970) Warn instead of crashing when schema helpers cannot append `null` to types
- [#1954](https://github.com/meltano/sdk/issues/1954) Missing begin()s related to SQLAlchemy 2.0 -- _**Thanks @andyoneal!**_
- [#1951](https://github.com/meltano/sdk/issues/1951) Ensure SQL streams are sorted when a replication key is set
- [#1949](https://github.com/meltano/sdk/issues/1949) Retry SQLAlchemy engine creation for adapters without JSON SerDe support
- [#1939](https://github.com/meltano/sdk/issues/1939) Handle `decimal.Decimal` instances in flattening
- [#1927](https://github.com/meltano/sdk/issues/1927) Handle replication key not found in stream schema -- _**Thanks @mjsqu!**_

### ⚡ Performance Improvements

- [#1962](https://github.com/meltano/sdk/issues/1962) Ensure `raw_schema` in stream mapper is immutable

### 📚 Documentation Improvements

- [#1925](https://github.com/meltano/sdk/issues/1925) Add viztracer command for testing targets -- _**Thanks @mjsqu!**_

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
- [#1484](https://github.com/meltano/sdk/issues/1484) Bump latest supported sqlalchemy from `1.*` to `2.*` -- _**Thanks @dependabot[bot]!**_

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

- [#1668](https://github.com/meltano/sdk/issues/1668) Break out default batch file writer into separate class

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
- [#1436](https://github.com/meltano/sdk/issues/1436) Handle sync abort, reduce duplicate `STATE` messages, rename `_MAX_RECORD_LIMIT` as `ABORT_AT_RECORD_COUNT` -- _**Thanks @aaronsteers!**_

## v0.22.1 (2023-03-29)

### 🐛 Fixes

- [#1172](https://github.com/meltano/sdk/issues/1172) Handle merging of SQL types when character column lengths are less than the max -- _**Thanks @BuzzCutNorman!**_
- [#1524](https://github.com/meltano/sdk/issues/1524) Preserve `__alias__` when mapping streams with repeated schema messages -- _**Thanks @DanilJr!**_
- [#1526](https://github.com/meltano/sdk/issues/1526) Handle missing `type` value when checking JSON schema types

### 📚 Documentation Improvements

- [#1553](https://github.com/meltano/sdk/issues/1553) Change link color from pink to blue
- Add sidebar hover styling
- [#1544](https://github.com/meltano/sdk/issues/1544) Update branding colors in docs site
- Add logo to readme
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

### ✨ New

- [#1410](https://github.com/meltano/sdk/issues/1410) Build empty cookiecutters and run lint task during CI -- _**Thanks @flexponsive!**_

### 🐛 Fixes

- [#1428](https://github.com/meltano/sdk/issues/1428) E2E Cookiecutter - Cover all REST authentication cases + one GraphQL case -- _**Thanks @flexponsive!**_

## v0.20.0 (2023-02-13)

### ✨ New

- [#1394](https://github.com/meltano/sdk/issues/1394) Refactor `SQLConnector` connection handling -- _**Thanks @qbatten!**_
- [#1241](https://github.com/meltano/sdk/issues/1241) Support declaring variant for use in package name
- [#1109](https://github.com/meltano/sdk/issues/1109) Support `requests.auth` authenticators
- [#1365](https://github.com/meltano/sdk/issues/1365) Add `strptime_to_utc` and `strftime` functions to `_singerlib.utils` -- _**Thanks @menzenski!**_

### 🐛 Fixes

- [#1380](https://github.com/meltano/sdk/issues/1380) Move tests in cookiecutters to project root to support `pytest_plugins`
- [#1406](https://github.com/meltano/sdk/issues/1406) Use a version of `isort` compatible with Python 3.8
- [#1385](https://github.com/meltano/sdk/issues/1385) SQL Targets ignore collation when evaluating column data types -- _**Thanks @BuzzCutNorman!**_
- [#1342](https://github.com/meltano/sdk/issues/1342) Remove SQLSink snakecase conform in favor of simpler transformations
- [#1364](https://github.com/meltano/sdk/issues/1364) TapDiscoveryTest remove catalog if one is passed

### 📚 Documentation Improvements

- [#1407](https://github.com/meltano/sdk/issues/1407) Docs nav structure reorg follow-up -- _**Thanks @aaronsteers!**_
- [#1390](https://github.com/meltano/sdk/issues/1390) Add incremental replication example -- _**Thanks @flexponsive!**_
- [#1353](https://github.com/meltano/sdk/issues/1353) Reorganize docs and add deprecation timeline

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

- [#1305](https://github.com/meltano/sdk/issues/1305) Install dependencies in CI and bump dev dependencies
- [#1308](https://github.com/meltano/sdk/issues/1308) Replace hyphens with underscores when generating expected env var name `<PLUGIN_NAME>_LOGLEVEL` -- _**Thanks @adherr!**_
- [#887](https://github.com/meltano/sdk/issues/887) Make `conform_record_data_types` work on nested objects and arrays -- _**Thanks @Jack-Burnett!**_
- [#1287](https://github.com/meltano/sdk/issues/1287) Targets to fail gracefully when schema message is missing the `properties` key -- _**Thanks @visch!**_

### 📚 Documentation Improvements

- [#1306](https://github.com/meltano/sdk/issues/1306) Update code samples docs
- [#1293](https://github.com/meltano/sdk/issues/1293) Add link to EDK

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

## v0.14.0 (2022-11-15)

### ✨ New

- [#1175](https://github.com/meltano/sdk/issues/1175) Add `datetime` functions to simpleeval env in stream maps -- _**Thanks @qbatten!**_

### 🐛 Fixes

- [#1182](https://github.com/meltano/sdk/issues/1182) Update `SQLConnector` import for SQL target cookiecutter -- _**Thanks @radbrt!**_
- [#1168](https://github.com/meltano/sdk/issues/1168) `SQLConnector.table_exists()` to use separate `table_name` and `schema_name` instead of fully qualified name -- _**Thanks @BuzzCutNorman!**_
- [#1164](https://github.com/meltano/sdk/issues/1164) Write a valid final state message at the end of each stream sync -- _**Thanks @laurentS!**_

### ⚙️ Under the Hood

- [#1114](https://github.com/meltano/sdk/issues/1114) Make DDL overridable for column `ADD`, `ALTER`, and `RENAME` operations

## v0.13.1 (2022-11-07)

### 🐛 Fixes

- [#1126](https://github.com/meltano/sdk/issues/1126) Resolve failure in `_increment_stream_state()` for cases when `replication_method` is `LOG_BASED` -- _**Thanks @aaronsteers!**_
- [#1111](https://github.com/meltano/sdk/issues/1111) Push _MAX_RECORDS_LIMIT down into SQL

### ⚙️ Under the Hood

- [#1091](https://github.com/meltano/sdk/issues/1091) Move SQLConnector into a separate module, for use by both SQLStream and SQLSink

### 📚 Documentation Improvements

- [#1133](https://github.com/meltano/sdk/issues/1133) Fix duplicate `Known Limitations` header
- [#1118](https://github.com/meltano/sdk/issues/1118) Document `BATCH` limitations -- _**Thanks @aaronsteers!**_

## v0.13.0 (2022-10-24)

### ✨ New

- [#1098](https://github.com/meltano/sdk/issues/1098) Add JSON Schema `Property` helpers for `allowed_values` (`enum`) and `examples` -- _**Thanks @aaronsteers!**_
- [#1096](https://github.com/meltano/sdk/issues/1096) Add secrets support for tap and target config, via `Property(..., secret=True)` -- _**Thanks @aaronsteers!**_
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

### ⚙️ Under the Hood

- [#979](https://github.com/meltano/sdk/issues/979) Remove dependency on `pipelinewise-singer-python` and move singer library code into `singer_sdk._singerlib`

### 📚 Documentation Improvements

- [#988](https://github.com/meltano/sdk/issues/988) Add pipe before SDK logo in header
- [#970](https://github.com/meltano/sdk/issues/970) Move cookiecutter TODOs into markdown comments -- _**Thanks @aaronsteers!**_

## v0.10.0 (2022-09-12)

### ✨ New

- [#829](https://github.com/meltano/sdk/issues/829) Add checks for primary keys, replication keys and state partitioning keys to standard tap tests -- _**Thanks @laurentS!**_
- [#732](https://github.com/meltano/sdk/issues/732) Implement reference paginators

### 🐛 Fixes

- [#898](https://github.com/meltano/sdk/issues/898) Fix SQL type merging for pre-existing target tables -- _**Thanks @BuzzCutNorman!**_
- [#856](https://github.com/meltano/sdk/issues/856) Fix typo RecordsWitoutSchemaException -> RecordsWithoutSchemaException

### ⚙️ Under the Hood

- Use __future__.annotations on singer_sdk.helpers._singer

### 📚 Documentation Improvements

- [#950](https://github.com/meltano/sdk/issues/950) Document missing initializers for authentication and pagination helpers
- [#947](https://github.com/meltano/sdk/issues/947) Remove stale autodoc page for RecordsWitoutSchemaException
- [#942](https://github.com/meltano/sdk/issues/942) Add docs preview links to PR description

## v0.9.0 (2022-08-24)

### ✨ New

- [#842](https://github.com/meltano/sdk/issues/842) Allow authenticating more generic requests
- [#919](https://github.com/meltano/sdk/issues/919) Add `ConnectionError` to list of backoff exceptions for auto-retry -- _**Thanks @jlloyd-widen!**_

### 🐛 Fixes

- [#917](https://github.com/meltano/sdk/issues/917) Allow Singer schemas to include the `required` and `enum` fields -- _**Thanks @Jack-Burnett!**_
- [#759](https://github.com/meltano/sdk/issues/759) Use recent start_date as starting_replication_value -- _**Thanks @ericboucher!**_

### ⚙️ Under the Hood

- [#908](https://github.com/meltano/sdk/issues/908) Allow overriding the bulk insert statement in `SQLSink`

### 📚 Documentation Improvements

- [#914](https://github.com/meltano/sdk/issues/914) Bump Pygments and update dbt example
- [#900](https://github.com/meltano/sdk/issues/900) Generate documentation for constructor parameters

## v0.8.0 (2022-08-05)

### 🐛 Fixes

- [#868](https://github.com/meltano/sdk/issues/868) Update return type for `backoff_max_tries` to reflect it accepts a callable that returns an integer -- _**Thanks @rawwar!**_
- [#878](https://github.com/meltano/sdk/issues/878) Change to use json dumps for outputting metrics -- _**Thanks @Jack-Burnett!**_

### 📚 Documentation Improvements

- [#869](https://github.com/meltano/sdk/issues/869) Cleanup whitespace in backoff code samples

## v0.7.0 (2022-07-21)

### ✨ New

- [#785](https://github.com/meltano/sdk/issues/785) Output full URL path in error messages -- _**Thanks @ericboucher!**_

### 🐛 Fixes

- [#816](https://github.com/meltano/sdk/issues/816) Generate correct SQL target project from cookiecutter
- [#781](https://github.com/meltano/sdk/issues/781) Allow lists and dictionaries as types for default JSON values

### 📚 Documentation Improvements

- [#823](https://github.com/meltano/sdk/issues/823) Add link to the sdk for README generation regarding Stream Maps -- _**Thanks @visch!**_
- [#814](https://github.com/meltano/sdk/issues/814) Fix PyPI trove classifiers
- [#783](https://github.com/meltano/sdk/issues/783) Document using pipx inject for nox-poetry

## v0.6.1 (2022-06-30)

### 🐛 Fixes

- [#776](https://github.com/meltano/sdk/issues/776) Fix missing typing-extensions for Python<3.10

## v0.6.0 (2022-06-30)

### ✨ New

- [#750](https://github.com/meltano/sdk/issues/750) Add end of pipe clean up hook to Sinks -- _**Thanks @z3z1ma!**_
- [#704](https://github.com/meltano/sdk/issues/704) Add api costs hook -- _**Thanks @laurentS!**_
- [#730](https://github.com/meltano/sdk/issues/730) Allow sort checking to be disabled -- _**Thanks @ilkkapeltola!**_

### 🐛 Fixes

- [#767](https://github.com/meltano/sdk/issues/767) Remove trailing parenthesis from logged version
- [#752](https://github.com/meltano/sdk/issues/752) Ensure all streams log sync costs -- _**Thanks @laurentS!**_
- [#737](https://github.com/meltano/sdk/issues/737) Check if RK is sortable before comparing
- Use str in Generic counter type
- Do not write SQLAlchemy logs to stdout
- Do not write SQLAlchemy logs to stdout -- _**Thanks @elephantum!**_

### 📚 Documentation Improvements

- [#745](https://github.com/meltano/sdk/issues/745) Update contributing docs
- [#689](https://github.com/meltano/sdk/issues/689) Update gitlab refs to github ones -- _**Thanks @aaronsteers!**_

## v0.4.3 (2022-02-18)

## v0.3.16 (2021-12-09)

## v0.3.10 (2021-09-30)

## v0.3.8 (2021-09-16)

## v0.3.6 (2021-08-26)

## v0.3.5 (2021-08-17)

## v0.3.4 (2021-08-13)

## v0.3.3 (2021-07-09)

## v0.3.2 (2021-07-02)

## v0.3.1 (2021-06-28)

## v0.3.0 (2021-06-24)

## v0.2.0 (2021-05-20)

## v0.1.6 (2021-05-14)

## v0.1.5 (2021-05-04)

## v0.1.4 (2021-05-03)

## v0.1.3 (2021-04-23)

## v0.1.2 (2021-04-12)

## v0.1.1 (2021-04-07)
