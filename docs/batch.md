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

The `encoding` field is used to specify the format and compression of the batch files. Currently the supported formats are: `jsonl`, `parquet` and `arrow`. GZip (`gzip`) compression is supported for JSONL and Parquet files.

#### JSONL Batch File Format

When using the `jsonl` encoding format, batch files should contain **raw JSON records only**, with one record per line. They should **NOT** contain Singer protocol `RECORD` messages.

**Correct format** (raw JSON records):

```json
{"id": 1, "name": "Alice", "email": "alice@example.com"}
{"id": 2, "name": "Bob", "email": "bob@example.com"}
{"id": 3, "name": "Charlie", "email": "charlie@example.com"}
```

**Incorrect format** (Singer RECORD messages):

```json
{"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "Alice", "email": "alice@example.com"}}
{"type": "RECORD", "stream": "users", "record": {"id": 2, "name": "Bob", "email": "bob@example.com"}}
{"type": "RECORD", "stream": "users", "record": {"id": 3, "name": "Charlie", "email": "charlie@example.com"}}
```

The Singer protocol messages (like `RECORD`, `SCHEMA`, `STATE`, and `BATCH`) are used for communication between taps and targets, but the batch files themselves contain only the record data.

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

`prefix` is prepended directly to each generated batch filename, e.g. `prefix: "test-batch-"` produces filenames like `test-batch-<stream>-<uuid>-1.jsonl.gz`. It is not a subdirectory, and no path separator is inserted between `prefix` and the filename.

### Default storage location

If `storage.root` is left unset (or explicitly set to `"file://"`, its schema default), batch files are written to a `singer-sdk` subdirectory of the OS temporary directory (as returned by Python's `tempfile.gettempdir()`), e.g. `/tmp/singer-sdk` on Linux/macOS. This directory is created on demand and is **not** managed or cleaned up by the SDK.

This default assumes the common case: a target reads and processes batch files shortly after the tap writes them, within the same sync run. It is not meant as durable or long-term storage — OS temp-cleanup policies (`systemd-tmpfiles`, Windows Disk Cleanup, ephemeral container filesystems that reset `/tmp` between runs) may remove files under this path independently of the sync's lifecycle. If your tap/target pairing has a significant delay between writing and reading batch files, or runs in an environment that aggressively purges temp storage, configure an explicit `storage.root` pointing at durable storage instead.

## Custom batch file creation and processing

### Tap side

Taps can optionally customize the batch file creation by implementing the [`get_batches`](singer_sdk.Stream.get_batches). This method should return a _tuple_ of an encoding and a list of batch files:

```python
class MyStream(Stream):
    def get_batches(self, records):
        return (
            BaseBatchFileEncoding(format="parquet", compression="snappy"),
            [
                "s3://my-bucket/my-batch-file-1.parquet",
                "s3://my-bucket/my-batch-file-2.parquet",
            ],
        )
```

#### Native Arrow data

If the source can produce [Apache Arrow](https://arrow.apache.org/) data directly (for example through an [ADBC](https://arrow.apache.org/adbc/) driver, a database client's `fetch_arrow_batches()`, or [PyMongoArrow](https://www.mongodb.com/docs/languages/python/pymongo-arrow-driver/current/)), implement [`get_record_batches`](singer_sdk.Stream.get_record_batches) instead of overriding `get_batches`. When `batch_config.encoding.format` is `arrow`, the SDK writes those tables straight to Arrow IPC batch files, without converting each row to a Python dictionary:

```python
class MyStream(Stream):
    replication_key = "id"
    is_sorted = True

    def get_record_batches(self, context):
        start = self.get_starting_replication_key_value(context)
        if start is None:  # First sync: no bookmark yet
            reader = my_driver.query_arrow("SELECT * FROM t ORDER BY id")
        else:
            # `>=`: rows sharing the bookmark's value may span two files
            reader = my_driver.query_arrow(
                "SELECT * FROM t WHERE id >= ? ORDER BY id",
                start,
            )
        yield from reader  # pyarrow.RecordBatch or pyarrow.Table objects
```

The SDK re-slices the tables into files of `batch_size` rows, drops deselected top-level columns, handles columns missing from the stream schema the same way RECORD mode does, and emits a `STATE` message after each file is written. For a partitioned stream, `get_record_batches` is called once per partition.

If the tables include the replication key column, state advances to each file's maximum value. As in RECORD mode:

- When [`is_sorted`](singer_sdk.Stream.is_sorted) is `True`, the bookmark itself advances, so an interrupted sync resumes from the last written file. Filter with `>=` the bookmark, as in the example above, so that rows sharing its value aren't skipped when they span two files. The rows at the bookmark's value are read again on resume.
- Otherwise, progress is kept in progress markers and the bookmark only advances once the sync completes, so an interrupted sync starts over from the previous bookmark.

The tables must already match the stream schema. In particular, cast types that JSON Schema can't express the same way as Arrow (e.g. integer-encoded booleans or `date` columns declared as `date-time`) before yielding them. Stream maps and [`post_process`](singer_sdk.Stream.post_process) aren't applied. Other encoding formats, RECORD mode, and streams with child streams still use [`get_records`](singer_sdk.Stream.get_records).

### Target side

Targets can optionally customize the batch file processing by implementing the [`process_batch_files`](singer_sdk.Sink.process_batch_files).

```python
class MySink(Sink):
    def process_batch_files(self, encoding, storage, files):
        # process the batch files
```

## Known Limitations of `BATCH`

1. Apart from [native Arrow data](#native-arrow-data), the built-in `BATCH` implementation does not support incremental bookmarks or `STATE` tracking. This work is tracked in [Issue #976](https://github.com/meltano/sdk/issues/976).
1. The `BATCH` implementation is not currently compatible with [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). This is certainly possible to implement in theory, but it would also introduce some performance penalties. This limitation is tracked in [Issue 1117#](https://github.com/meltano/sdk/issues/1117).

If you are interested in contributing to one or both of these features, please add a comment in the respective issue.
