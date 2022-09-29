# [SDK Implementation Details](./index.md) - Tap Metrics

Metrics logging is specified in the
[Singer Spec](https://hub.meltano.com/singer/spec#metrics). The SDK will automatically
emit two types of metrics `record_count`, `http_request_duration` and `sync_duration`.

## Customization options

### `metrics_log_level`

Metrics are logged at the `INFO` level. Developers may optionally add a
`metrics_log_level` config option to their taps, `WARNING` or `ERROR` to disable
metrics logging.

### `SINGER_SDK_LOG_CONFIG`

Users of a tap can configure the SDK logging by setting the `SINGER_SDK_LOG_CONFIG`
environment variable. The value of this variable should be a path to a YAML file in the
[Python logging dict format](https://docs.python.org/3/library/logging.config.html#dictionary-schema-details).

For example, to direct metrics records to a file, you could use the following config:

```yaml
version: 1
disable_existing_loggers: false
formatters:
  metrics:
    format: "{asctime} {message}"
    style: "{"
handlers:
  metrics:
    class: logging.FileHandler
    formatter: metrics
    filename: metrics.log
loggers:
  singer_sdk.metrics:
    level: INFO
    handlers: [ metrics ]
    propagate: yes
```

## Additional Singer Metrics References

- [Singer Spec: Metrics](https://hub.meltano.com/singer/spec#metrics)
