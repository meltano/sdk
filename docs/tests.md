# Testing

## Testing the CLI

Poetry allows us to test command line in the virtualenv using the prefix `poetry run`. Alternatively, you could fully install the project and then execute the command(s) directly.

**Run '--help'...**

```bash
poetry install && \
poetry run tap-parquet --help
```

### Run in sync sync mode with auto-discovery

```bash
poetry install && \
poetry run tap-parquet \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
```

**Run 'sync' with a catalog file input...**

```bash
poetry install && \
poetry run sample-tap-parquet \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
   --catalog singer_sdk/samples/sample_tap_parquet/parquet-catalog.sample.json
```

**Run 'discovery'...**

```bash
poetry install && \
poetry run sample-tap-parquet --discover \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json
```

**Run 'discovery' with a passed catalog file (kind of cheating, but may be valid for some cases)...**

```bash
poetry install && \
poetry run sample-tap-parquet --discover \
   --config singer_sdk/samples/sample_tap_parquet/parquet-config.sample.json \
   --catalog singer_sdk/samples/sample_tap_parquet/parquet-catalog.sample.json
```

**Note:**

- CLI mapping is performed in `pyproject.toml` and shims are recreated during `poetry install`:

    ```toml
    ...
    [tool.poetry.scripts]
    plugin-base = 'singer_sdk.plugin_base:PluginBase.cli'
    sample-tap-parquet = 'singer_sdk.tests.sample_tap_parquet.parquet_tap:cli'
    ```
