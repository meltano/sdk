# Logging

Logs are configurable by the environment variables `<PLUGIN_NAME>_LOGLEVEL` (preferred)
or `LOGLEVEL`. Use `LOGLEVEL` when you intend to control the log output for all taps
and targets running within the environment. In contrast, we recommend setting
`<PLUGIN_NAME>_LOGLEVEL` for more granual control of each tap or target individually.

From most verbose to least verbose, the accepted values for logging level are `debug`,
`info`, `warning`, and `error`. Logging level inputs are case-insensitive.

## Custom logging configuration

Users of a tap can configure the SDK logging by setting the `SINGER_SDK_LOG_CONFIG`
environment variable. The value of this variable should be a path to a YAML file in the
[Python logging dict format](https://docs.python.org/3/library/logging.config.html#dictionary-schema-details).

For example, to send [metrics](./metrics.md) (with logger name `singer_sdk.metrics`) to a file, you could use the following config:

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

This will send metrics to a `metrics.log`:

```
2022-09-29 00:48:52,746 INFO METRIC: {"metric_type": "timer", "metric": "http_request_duration", "value": 0.501743, "tags": {"stream": "continents", "endpoint": "", "http_status_code": 200, "status": "succeeded"}}
2022-09-29 00:48:52,775 INFO METRIC: {"metric_type": "counter", "metric": "http_request_count", "value": 1, "tags": {"stream": "continents", "endpoint": ""}}
2022-09-29 00:48:52,776 INFO METRIC: {"metric_type": "timer", "metric": "sync_duration", "value": 0.7397160530090332, "tags": {"stream": "continents", "context": {}, "status": "succeeded"}}
2022-09-29 00:48:52,776 INFO METRIC: {"metric_type": "counter", "metric": "record_count", "value": 7, "tags": {"stream": "continents", "context": {}}}
2022-09-29 00:48:53,225 INFO METRIC: {"metric_type": "timer", "metric": "http_request_duration", "value": 0.392148, "tags": {"stream": "countries", "endpoint": "", "http_status_code": 200, "status": "succeeded"}}
2022-09-29 00:48:53,302 INFO METRIC: {"metric_type": "counter", "metric": "http_request_count", "value": 1, "tags": {"stream": "countries", "endpoint": ""}}
2022-09-29 00:48:53,302 INFO METRIC: {"metric_type": "timer", "metric": "sync_duration", "value": 0.5258760452270508, "tags": {"stream": "countries", "context": {}, "status": "succeeded"}}
2022-09-29 00:48:53,303 INFO METRIC: {"metric_type": "counter", "metric": "record_count", "value": 250, "tags": {"stream": "countries", "context": {}}}
```
