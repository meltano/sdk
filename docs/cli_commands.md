# Command Line Samples

## Enabling CLI Execution

Poetry allows you to test command line invocation direction in the virtualenv using the
prefix `poetry run`.

- Note: CLI mapping is performed in `pyproject.toml` and shims are recreated during `poetry install`:

    ````{tab} Poetry
    ```toml
    ...
    [tool.poetry.scripts]
    tap-mysource = 'singer_sdk.tests.sample_tap_parquet.parquet_tap:cli'
    ```
    ````

    ````{tab} uv
    ```toml
    ...
    [project.scripts]
    tap-mysource = 'singer_sdk.tests.sample_tap_parquet.parquet_tap:cli'
    ```
    ````

The CLI commands defined here will be configured automatically when the python library is installed by a user.

## For example, to run `--help`

````{tab} Poetry
```bash
poetry install && \
poetry run tap-mysource --help
```
````

````{tab} uv
```bash
uv sync && \
uv run tap-mysource --help
```
````

## Run in sync mode with auto-discovery

````{tab} Poetry
```bash
poetry install && \
poetry run tap-mysource \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
```
````

````{tab} uv
```bash
uv sync && \
uv run tap-mysource \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
````

## Run in sync mode with a catalog file input

````{tab} Poetry
```bash
poetry install && \
poetry run tap-mysource \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
   --catalog singer_sdk/samples/sample_tap_parquet/parquet-catalog.sample.json
```
````

````{tab} uv
```bash
uv sync && \
uv run tap-mysource \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
   --catalog singer_sdk/samples/sample_tap_parquet/parquet-catalog.sample.json
````

## Run in discovery mode

````{tab} Poetry
```bash
poetry install && \
poetry run tap-mysource --discover \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
```
````

````{tab} uv
```bash
uv sync && \
uv run tap-mysource --discover \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
````

## Run in discovery mode with a passed catalog file

````{tab} Poetry
```bash
poetry install && \
poetry run tap-mysource --discover \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json \
   --catalog singer_sdk/samples/sample_tap_parquet/parquet-catalog.sample.json
```
````

````{tab} uv
```bash
uv sync && \
uv run tap-mysource --discover \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json \
   --catalog singer_sdk/samples/sample_tap_parquet/parquet-catalog.sample.json
````

## Test connectivity

The `--test` option allows the user to validate configuration and assess connectivity.

````{tab} Poetry
```bash
poetry install && \
poetry run tap-mysource --test \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
```
````

````{tab} uv
```bash
uv sync && \
uv run tap-mysource --test \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
````

## Package Information

The `--about` option displays metadata about the package.

````{tab} Poetry
```console
$ poetry run sdk-tap-countries-sample --about
Name: sample-tap-countries
Description: Sample tap for Countries GraphQL API.
Version: [could not be detected]
SDK Version: 0.44.2
Capabilities:
  - catalog
  - state
  - discover
  - about
  - stream-maps
  - schema-flattening
  - batch
Settings:
  - Name: stream_maps
    Type: ['object', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_STREAM_MAPS
  - Name: stream_map_config
    Type: ['object', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_STREAM_MAP_CONFIG
  - Name: faker_config
    Type: ['object', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_FAKER_CONFIG
  - Name: flattening_enabled
    Type: ['boolean', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_FLATTENING_ENABLED
  - Name: flattening_max_depth
    Type: ['integer', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_FLATTENING_MAX_DEPTH
  - Name: batch_config
    Type: ['object', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_BATCH_CONFIG
```
````

````{tab} uv
```console
$ uv run sdk-tap-countries-sample --about
Name: sample-tap-countries
Description: Sample tap for Countries GraphQL API.
Version: [could not be detected]
SDK Version: 0.44.2
Capabilities:
  - catalog
  - state
  - discover
  - about
  - stream-maps
  - schema-flattening
  - batch
Settings:
  - Name: stream_maps
    Type: ['object', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_STREAM_MAPS
  - Name: stream_map_config
    Type: ['object', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_STREAM_MAP_CONFIG
  - Name: faker_config
    Type: ['object', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_FAKER_CONFIG
  - Name: flattening_enabled
    Type: ['boolean', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_FLATTENING_ENABLED
  - Name: flattening_max_depth
    Type: ['integer', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_FLATTENING_MAX_DEPTH
  - Name: batch_config
    Type: ['object', 'null']
    Environment Variable: SAMPLE_TAP_COUNTRIES_BATCH_CONFIG
```
````

This information can also be printed in JSON format for consumption by other applications

````{tab} Poetry
```console
$ poetry run sdk-tap-countries-sample --about --format json
{
  "name": "sample-tap-countries",
  "description": "Sample tap for Countries GraphQL API.",
  "version": "[could not be detected]",
  "sdk_version": "0.44.2",
  "capabilities": [
    "sync",
    "catalog",
    "state",
    "discover"
  ],
  "settings": {
    "type": "object",
    "properties": {}
  }
}
```
````

````{tab} uv
```console
$ uv run sdk-tap-countries-sample --about --format json
{
  "name": "sample-tap-countries",
  "description": "Sample tap for Countries GraphQL API.",
  "version": "[could not be detected]",
  "sdk_version": "0.44.2",
  "capabilities": [
    "sync",
    "catalog",
    "state",
    "discover"
  ],
  "settings": {
    "type": "object",
    "properties": {}
  }
}
```
````

## Invocation options

There a are few options available to invoke your connector.

### Poetry

Activate the Poetry environment with `poetry shell` or prefix all your commands with `poetry run`. The commands you then run are the same ones you'd use if you installed your package with `pip`.

### uv

Prefix all your commands with `uv run`. The commands you then run are the same ones you'd use if you installed your package with `pip` or `uv pip`.

### Meltano

The cookiecutter templates also come with a `meltano.yml` for you to try your package with [Meltano]. To use it, you'll have to [declare settings] and their types.

For example:

```yaml
plugins:
  extractors:
  - name: my-tap
    namespace: my_tap
    executable: -e .
    capabilities:
    - state
    - catalog
    - discover
    settings:
    - name: base_url
      kind: string
    - name: api_key
      kind: password
```

### Comparison

|                     | native/shell/`poetry`/`uv`                                                                  |                          `meltano`                                   |
| ------------------- | :-----------------------------------------------------------------------------------------: | :------------------------------------------------------------------: |
| Configuration store | Config JSON file (`--config=path/to/config.json`) or environment variables (`--config=ENV`) | `meltano.yml`, `.env`, environment variables, or Meltano's system db |
| Simple invocation   | `my-tap --config=...`                                                                       | `meltano invoke my-tap`                                              |
| Other CLI options   | `my-tap --about --format=json`                                                              | `meltano invoke my-tap --about --format=json`                        |
| ELT                 | `my-tap --config=... \| path/to/target-jsonl --config=...`                                  | `meltano run my-tap target-jsonl`                                    |

[Meltano]: https://www.meltano.com
[declare settings]: https://docs.meltano.com/reference/command-line-interface#how-to-use-2
