# Batch Messages

Batch messaging allows RECORD messages to be piped to a file on disk, rather
than over STDOUT. This has performance benefits for downstream targets that
support handling files (e.g. data warehouses, blob storage etc.).

## Configuring Batch Messaging

Currently batch messaging can be enabled on an entire Tap only. An example
config is below:

```json
{
    "batch_enabled": true
}
```

## FAQ

**Where are messages stored?**

Batch messaging currently utilises the `tempfile` module to create a host-OS appropriate
temporary directory. In future this will be configurable.

**What file format is used?**

Batch writes messages in the JSON-lines (`.jsonl`) format by default. In future
other file formats such as CSV and Parquet will be supported.

## See Also

[Add built-in support for new FAST_SYNC spec (aka BATCH message type)](https://gitlab.com/meltano/singer-sdk/-/issues/9)
[Singer extension: `BATCH` messages to enable faster throughput](https://gitlab.com/meltano/meltano/-/issues/2364)