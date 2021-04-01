# Command Line Samples

## Enabling CLI Execution

Poetry allows you to test command line invokation direction in the virtualenv using the
prefix `poetry run`.

- Note: CLI mapping is performed in `pyproject.toml` and shims are recreated during `poetry install`:

    ```toml
    ...
    [tool.poetry.scripts]
    tap-mysource = 'singer_sdk.tests.sample_tap_parquet.parquet_tap:cli'
    ```

The CLI commands defined here will be configured automatically when th python library is installed by a user.

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
