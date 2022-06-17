# [SDK Implementation Details](./index.md) - Tap Metrics

Metrics logging is specified in the
[Singer Spec](https://hub.meltano.com/singer/spec#metrics). The SDK will automatically
emit two types of metrics `record_count` and `http_request_duration`.

Customization options:

Developers may optionally add a `metrics_log_level` config option to their taps,
which will automatically allow this metrics logging to be customized at runtime.

When `metrics_log_level` is supported, users can then
set one of these values (case insensitive), `INFO`, `DEBUG`, `NONE`, to override the
default logging level for metrics. This can be helpful for REST-type sources which use
make a large number of REST calls can therefor have very noisy metrics.

## Additional Singer Metrics References

- [Singer Spec: Metrics](https://hub.meltano.com/singer/spec#metrics)
