# Testing

## Testing the CLI

Poetry allows us to test command line in the virtualenv using the prefix `poetry run`. Alternatively, you could fully install the project and then execute the command(s) directly.

**Run '--help'...**

```bash
poetry install && \
poetry run sample-tap-parquet --help
```

**Run 'sync' with auto-discovery...**

```bash
poetry install && \
poetry run sample-tap-parquet \
   --config tap_base/tests/sample_tap_parquet/tests/config.sample.json
```

**Run 'sync' with a catalog file input...**

```bash
poetry install && \
poetry run sample-tap-parquet \
   --config tap_base/tests/sample_tap_parquet/tests/config.sample.json
   --catalog tap_base/tests/sample_tap_parquet/tests/catalog.sample.json
```

**Run 'discovery'...**

```bash
poetry install && \
poetry run sample-tap-parquet --discover \
   --config tap_base/tests/sample_tap_parquet/tests/config.sample.json
```

**Run 'discovery' with a passed catalog file (kind of cheating, but may be valid for some cases)...**

```bash
poetry install && \
poetry run sample-tap-parquet --discover \
   --config tap_base/tests/sample_tap_parquet/tests/config.sample.json \
   --catalog tap_base/tests/sample_tap_parquet/tests/catalog.sample.json
```

**Note:**

- CLI mapping is performed in `pyproject.toml` and shims are recreated during `poetry install`:

    ```toml
    ...
    [tool.poetry.scripts]
    plugin-base = 'tap_base.plugin_base:PluginBase.cli'
    tap-base = 'tap_base.tap_base:cli'
    sample-tap-parquet = 'tap_base.tests.sample_tap_parquet.parquet_tap:cli'
    ```
