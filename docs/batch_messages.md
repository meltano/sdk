# BATCH Messages

Batch messaging allows records (the payload usually under RECORD message 'record' key) to be piped to a file on disk, rather
than over STDOUT. This has throughput benefits for downstream targets that
support handling files (e.g. data warehouses, blob storage etc.). For targets
that do not support handling files, batch messaging can still be used to reduce memory
requirements (as batches can be read and processed line-by-line) at the expense of
overall throughput (the round-trip to disk is slower than STDOUT and in-memory
batching in most cases).

## Configuring Batch Messaging

Batch messaging can be enabled by passing the `--batch` flag when invoking
your Tap. Batching is enabled on _all_ streams.

## FAQ

**Where are messages stored?**

Batch files are created in directory with the following pattern:

`~/.singer-sdk/<tap-name>/<stream-name>/<timestamp: "%Y-%m-%d-%H%M%S">/<batch-file-name>`

_It is the responsibility of the Target to remove batch files once processed_.

**What file format is used?**

Batch writes messages in the JSON-lines (`.jsonl`) format by default. In future
other file formats such as CSV and Parquet will be supported.

## See Also

- [Add built-in support for new FAST_SYNC spec (aka BATCH message type)](https://gitlab.com/meltano/singer-sdk/-/issues/9)
- [Singer extension: `BATCH` messages to enable faster throughput](https://gitlab.com/meltano/meltano/-/issues/2364)
