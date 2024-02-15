# Batch Messages

```{warning}
The `BATCH` message functionality is currently in preview and is subject to change.
You can [open an issue](https://github.com/meltano/sdk/issues) or [join the discussion](https://github.com/meltano/sdk/discussions/963) on GitHub to provide feedback during the preview period.
```

[The Singer message specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#output) defines the three basic types of messages: `RECORD`, `STATE`, and `SCHEMA`. The `RECORD` message is used to send data from the tap to the target. The `STATE` message is used to send state data from the tap to the target. The `SCHEMA` message is used to send schema data from the tap to the target, and for example, create tables with the correct column types.

However, the Singer specification can be extended to support additional types of messages. For example, the [`ACTIVATE_VERSION`](https://sdk.meltano.com/en/latest/capabilities.html#singer_sdk.helpers.capabilities.PluginCapabilities.ACTIVATE_VERSION) message is used to manage hard deletes in the target.

This library's implementation of the `BATCH` message is used to send records in bulk from the tap to the target, using an intermediate filesystem to store _batch_ files. This is useful, for example

- when the tap outputs records at a much higher rate than the target can consume them, creating backpressure
- when the source system can directly export data in bulk (e.g. a database dump)

Currently only a local filesystem or AWS S3 are supported, but other filesystems like FTP, etc. could be supported in the future.

## The `BATCH` Message

Local
```json
{
  "type": "BATCH",
  "stream": "users",
  "encoding": {
    "format": "jsonl",
    "compression": "gzip"
  },
  "manifest": [
    "file://path/to/batch/file/1",
    "file://path/to/batch/file/2"
  ]
}
```

AWS S3
```json
{
  "type": "BATCH",
  "stream": "users",
  "encoding": {
    "format": "jsonl",
    "compression": "gzip"
  },
  "manifest": [
    "s3://path/to/batch/file/1",
    "s3://path/to/batch/file/2"
  ]
}
```

### `encoding`

The `encoding` field is used to specify the format and compression of the batch files. Currently `jsonl`, `gzip` and `parquet` are supported.

### `manifest`

The `manifest` field is used to specify the paths to the batch files. The paths are relative to the `root` directory specified in the [`batch_config`](#batch-configuration) storage configuration.

## Batch configuration

When local storage is used, targets do no require special configuration to process `BATCH` messages. Use of AWS S3 assumes S3/AWS credentials are already discoverable via the underlying S3 libraries (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION`)

Taps may be configured to specify a root storage `root` directory, file path `prefix`, and `encoding` for batch files using a configuration like the below:


In `config.json`:

```js
{
  // ...
  "batch_config": {
    "encoding": {
      "format": "jsonl",
      "compression": "gzip",
    },
    "storage": {
      "root": "file://tests/core/resources",
      "prefix": "test-batch-",
    }
  }
}
```

## Custom batch file creation and processing

### Tap side

Taps can optionally customize the batch file creation by implementing the [`get_batches`](singer_sdk.Stream.get_batches). This method should return a _tuple_ of an encoding and a list of batch files:

```python
class MyStream(Stream):
    def get_batches(self, records):
        return (
            ParquetEncoding(compression="snappy"),
            [
                "s3://my-bucket/my-batch-file-1.parquet",
                "s3://my-bucket/my-batch-file-2.parquet",
            ]
        )
```

### Target side

Targets can optionally customize the batch file processing by implementing the [`process_batch_files`](singer_sdk.Sink.process_batch_files).

```python
class MySink(Sink):
    def process_batch_files(self, encoding, storage, files):
        # process the batch files
```

## Known Limitations of `BATCH`

1. Currently the built-in `BATCH` implementation does not support incremental bookmarks or `STATE` tracking. This work is tracked in [Issue #976](https://github.com/meltano/sdk/issues/976).
2. The `BATCH` implementation is not currently compatible with [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). This is certainly possible to implement in theory, but it would also introduce some performance penalties. This limitation is tracked in [Issue 1117#](https://github.com/meltano/sdk/issues/1117).

If you are interested in contributing to one or both of these features, please add a comment in the respective issue.
