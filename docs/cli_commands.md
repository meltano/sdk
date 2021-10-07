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

## Invocation options

There a are few options available to invoke your connector.

### Poetry

Activate the Poetry environment with `poetry shell` or prefix all your commands with `poetry run`. The commands you then run are the same ones you'd use if you installed your package with `pip`.

### Shell Script

The cookiecutter comes with a shell script (e.g. `./my-tap.sh`) to save from typing `poetry install` and `poetry run` too many times. You can use it to replace normal calls to `poetry run my-tap ...` with `./my-tap.sh ...`.

### Meltano

The cookiecutter templates also come with a `meltano.yml` for you to try your package with [Meltano]. To use it, you'll have to [declare settings] and their types.

For example:

```yaml
plugins:
  extractors:
  - name: my-tap
    namespace: my_tap
    executable: ./my-tap.sh
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

|                     | native/shell/`poetry`                                                                       |                          `meltano`                                   |
| ------------------- | :-----------------------------------------------------------------------------------------: | :------------------------------------------------------------------: |
| Configuration store | Config JSON file (`--config=path/to/config.json`) or environment variables (`--config=ENV`) | `meltano.yml`, `.env`, environment variables, or Meltano's system db |
| Simple invocation   | `my-tap --config=...`                                                                       | `meltano invoke my-tap`                                              |
| Other CLI options   | `my-tap --about --format=json`                                                              | `meltano invoke my-tap --about --format=json`                        |
| ELT                 | `my-tap --config=... \| path/to/target-jsonl --config=...`                                  | `meltano elt my-tap target-jsonl`                                    |

[Meltano]: https://www.meltano.com
[declare settings]: https://meltano.com/docs/command-line-interface.html#how-to-use-2
