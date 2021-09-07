# Command Line Samples

## Enabling CLI Execution

Poetry allows you to test command line invocation direction in the virtualenv using the
prefix `poetry run`.

- Note: CLI mapping is performed in `pyproject.toml` and shims are recreated during `poetry install`:

    ```toml
    ...
    [tool.poetry.scripts]
    tap-mysource = 'singer_sdk.tests.sample_tap_parquet.parquet_tap:cli'
    ```

The CLI commands defined here will be configured automatically when the python library is installed by a user.

## For example, to run `--help`

```bash
poetry install && \
poetry run tap-mysource --help
```

## Run in sync mode with auto-discovery

```bash
poetry install && \
poetry run tap-mysource \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
```

## Run in sync mode with a catalog file input

```bash
poetry install && \
poetry run tap-mysource \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
   --catalog singer_sdk/samples/sample_tap_parquet/parquet-catalog.sample.json
```

## Run in discovery mode

```bash
poetry install && \
poetry run tap-mysource --discover \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
```

## Run in discovery mode with a passed catalog file

```bash
poetry install && \
poetry run tap-mysource --discover \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json \
   --catalog singer_sdk/samples/sample_tap_parquet/parquet-catalog.sample.json
```

## Test connectivity

The `--test` option allows the user to validate configuration and assess connectivity.

```bash
poetry install && \
poetry run tap-mysource --test \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
```

## Package Information

The `--about` option displays metadata about the package.

```console
$ poetry run sdk-tap-countries-sample --about
Name: sample-tap-countries
Version: [could not be detected]
Sdk_Version: 0.3.5
Capabilities: ['sync', 'catalog', 'state', 'discover']
Settings: {'type': 'object', 'properties': {}}
```

This information can also be printed in JSON format for consumption by other applications

```console
$ poetry run sdk-tap-countries-sample --about --format json
{
  "name": "sample-tap-countries",
  "version": "[could not be detected]",
  "sdk_version": "0.3.5",
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
