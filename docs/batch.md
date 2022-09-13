# PREVIEW - Batch Messages (A.K.A. Fast Sync)

```{warning}
The `BATCH` message functionality is currently in preview and is subject to change.
```

[The Singer message specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#output) defines the three basic types of messages: `RECORD`, `STATE`, and `SCHEMA`. The `RECORD` message is used to send data from the tap to the target. The `STATE` message is used to send state data from the tap to the target. The `SCHEMA` message is used to send schema data from the tap to the target, and for example, create tables with the correct column types.

However, the Singer specification can be extended to support additional types of messages. For example, the [`ACTIVATE_VERSION`](https://sdk.meltano.com/en/latest/capabilities.html#singer_sdk.helpers.capabilities.PluginCapabilities.ACTIVATE_VERSION) message is used to manage hard deletes in the target.

This library's implementation of the `BATCH` message is used to send records in bulk from the tap to the target, using an intermediate filesystem to store _batch_ files. This is useful, for example

- when the tap outputs records at a much higher rate than the target can consume them, creating backpressure
- when the source system can directly export data in bulk (e.g. a database dump)

Currently only a local filesystem is supported, but other filesystems like AWS S3, FTP, etc. could be supported in the future.

## The `BATCH` Message

```json
{
  "type": "BATCH",
  "stream": "users",
  "encoding": {
    "format": "jsonl",
    "compression": "gzip"
  },
  "manifest": [
    "path/to/batch/file/1",
    "path/to/batch/file/2"
  ]
}
```

### `encoding`

The `encoding` field is used to specify the format and compression of the batch files. Currently only `jsonl` and `gzip` are supported, respectively.

### `manifest`

The `manifest` field is used to specify the paths to the batch files. The paths are relative to the `root` directory specified in the [`batch_config`](#batch-configuration) storage configuration.

## Batch configuration

The batch configuration is used to specify the root directory for the batch files, and the maximum number of records per batch file.

```json
{
  "encoding": {
    "format": "jsonl",
    "compression": "gzip",
  },
  "storage": {
    "root": "file://tests/core/resources",
    "prefix": "test-batch-",
  }
}
```

## Custom batch file creation and processing

### Tap side

The tap can customize the batch file creation by implementing the [`get_batches`](singer_sdk.Stream.get_batches). This method should return a _tuple_ of an encoding and a list of batch files:

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

The target can customize the batch file processing by implementing the [`process_batch_files`](singer_sdk.Sink.process_batch_files).

```python
class MySink(Sink):
    def process_batch_files(self, encoding, storage, files):
        # process the batch files
```
