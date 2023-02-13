# Tap Metrics

Metrics logging is specified in the
[Singer Spec](https://hub.meltano.com/singer/spec#metrics). The SDK will automatically
emit metrics for `record_count`, `http_request_duration` and `sync_duration`.

## Customization options

### `metrics_log_level`

Metrics are logged at the `INFO` level. Developers may optionally add a
`metrics_log_level` config option to their taps, `WARNING` or `ERROR` to disable
metrics logging.

### `SINGER_SDK_LOG_CONFIG`

Metrics are written by the `singer_sdk.metrics` logger, so the end user can set
`SINGER_SDK_LOG_CONFIG` to a logging config file that defines the format and output
for metrics. See the [logging docs](./logging.md) for an example file.

## Additional Singer Metrics References

- [Singer Spec: Metrics](https://hub.meltano.com/singer/spec#metrics)
