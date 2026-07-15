# meltano-singer-python

A lightweight library for building [Singer](https://hub.meltano.com/singer/spec) taps
and targets, and a (mostly) drop-in replacement for
[`singer-python`](https://github.com/singer-io/singer-python) and
[`pipelinewise-singer-python`](https://github.com/transferwise/pipelinewise-singer-python).

This package ships the top-level `singer` import package. It is maintained as part of
the [Meltano Singer SDK](https://github.com/meltano/sdk) and released in lockstep with
`singer-sdk`, which depends on it.

## Installation

```bash
pip install meltano-singer-python
```

> [!WARNING]
> This package provides the top-level `singer` package and therefore **cannot be
> installed alongside** `singer-python` or `pipelinewise-singer-python`. Replace those
> dependencies with `meltano-singer-python` — do not add it next to them.

## Drop-in compatibility

Supported legacy API surface:

- Message classes with `asdict()`: `RecordMessage`, `SchemaMessage`, `StateMessage`,
  `ActivateVersionMessage`, plus `write_message()` / `format_message()`
- `singer.bookmarks` (also re-exported at the top level): `get_bookmark`,
  `write_bookmark`, `clear_bookmark`, `set_currently_syncing`, `get_currently_syncing`,
  offset helpers
- `singer.metrics`: `record_counter`, `http_request_timer`, `job_timer`, `Counter`,
  `Timer` (legacy `METRIC: {...}` log lines)
- `singer.utils`: `now`, `strftime`, `strptime_to_utc`, `load_json`, `check_config`,
  `parse_args`
- `singer.get_logger`, `singer.should_sync_field`
- `Catalog`, `CatalogEntry`, `Schema`
- `BATCH` messages: `BatchMessage` and `BaseBatchFileEncoding` (`jsonl`, `parquet`,
  `arrow`, ...)

Notable differences from the legacy libraries:

- `singer.transform` / `Transformer` and `parse_message` are **not** provided.
- The functional `singer.metadata` module (`new`, `to_map`, `to_list`, `write`, `get`,
  `delete`, `get_standard_metadata`) is **not** provided. `CatalogEntry.metadata` is
  always a `MetadataMapping`, so read and write metadata through it directly, e.g.
  `catalog_entry.metadata.root.replication_method` instead of
  `metadata.to_map(catalog_entry.metadata).get((), {}).get('replication-method')`, and
  `catalog_entry.metadata['properties', name].inclusion` instead of
  `metadata.get(md_map, ('properties', name), 'inclusion')`.
- `utils.parse_args` does not support the deprecated `--properties` flag; use
  `--catalog`.
- Metric log lines include an additional `pid` tag, and the metrics logger is named
  `singer_sdk.metrics`.
- Message serialization is Decimal-safe by default (`simplejson`), preserving values
  like `1e-38` exactly.

## Structured logging

Logging can be configured through environment variables, in priority order:

1. `SINGER_SDK_LOG_CONFIG` — path to a JSON (or YAML, if PyYAML is installed)
   [dictConfig](https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig)
   file. This is the file [Meltano](https://meltano.com) generates for Singer plugins,
   enabling structured JSON logs via `singer.logging.StructuredFormatter`.
1. `LOGGING_CONF_FILE` — path to an INI
   [fileConfig](https://docs.python.org/3/library/logging.config.html#logging.config.fileConfig)
   file (pipelinewise-singer-python compatible).

## Performance

For high-throughput taps, subclass `RecordMessage` to pre-format `time_extracted` once
per sync instead of once per record (the `FastRecordMessage` pattern), and consider
`singer-sdk[msgspec]` if you are building on the full SDK.
