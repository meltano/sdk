# Logging

## Logging levels

Logging levels are configurable by the environment variables `<PLUGIN_NAME>_LOGLEVEL` (preferred)
or `LOGLEVEL`. Use `LOGLEVEL` when you intend to control the log output for all taps
and targets running within the environment. In contrast, we recommend setting
`<PLUGIN_NAME>_LOGLEVEL` for more granual control of each tap or target individually.

From most verbose to least verbose, the accepted values for logging level are `debug`,
`info`, `warning`, and `error`. Logging level inputs are case-insensitive.

To use different logging levels for different loggers, see the [custom logging configuration](#custom-logging-configuration) section below.

## Default log format

The default log format is `"{asctime:23s} | {levelname:8s} | {name:20s} | {message}"`.

This produces logs that look like this:

```
2022-12-05 19:46:46,744 | INFO     | my_tap               | Added 'child' as child stream to 'my_stream'
2022-12-05 19:46:46,744 | INFO     | my_tap               | Beginning incremental sync of 'my_stream'...
2022-12-05 19:46:46,744 | INFO     | my_tap               | Tap has custom mapper. Using 1 provided map(s).
2022-12-05 19:46:46,745 | INFO     | my_tap               | Beginning full_table sync of 'child' with context: {'parent_id': 1}...
2022-12-05 19:46:46,745 | INFO     | my_tap               | Tap has custom mapper. Using 1 provided map(s).
2022-12-05 19:46:46,746 | INFO     | singer_sdk.metrics   | INFO METRIC: {"metric_type": "timer", "metric": "sync_duration", "value": 0.0005319118499755859, "tags": {"stream": "child", "context": {"parent_id": 1}, "status": "succeeded"}}
2022-12-05 19:46:46,747 | INFO     | singer_sdk.metrics   | INFO METRIC: {"metric_type": "counter", "metric": "record_count", "value": 3, "tags": {"stream": "child", "context": {"parent_id": 1}}}
2022-12-05 19:46:46,747 | INFO     | my_tap               | Beginning full_table sync of 'child' with context: {'parent_id': 2}...
2022-12-05 19:46:46,748 | INFO     | singer_sdk.metrics   | INFO METRIC: {"metric_type": "timer", "metric": "sync_duration", "value": 0.0004410743713378906, "tags": {"stream": "child", "context": {"parent_id": 2}, "status": "succeeded"}}
2022-12-05 19:46:46,748 | INFO     | singer_sdk.metrics   | INFO METRIC: {"metric_type": "counter", "metric": "record_count", "value": 3, "tags": {"stream": "child", "context": {"parent_id": 2}}}
2022-12-05 19:46:46,749 | INFO     | my_tap               | Beginning full_table sync of 'child' with context: {'parent_id': 3}...
2022-12-05 19:46:46,749 | INFO     | singer_sdk.metrics   | INFO METRIC: {"metric_type": "timer", "metric": "sync_duration", "value": 0.0004508495330810547, "tags": {"stream": "child", "context": {"parent_id": 3}, "status": "succeeded"}}
2022-12-05 19:46:46,750 | INFO     | singer_sdk.metrics   | INFO METRIC: {"metric_type": "counter", "metric": "record_count", "value": 3, "tags": {"stream": "child", "context": {"parent_id": 3}}}
2022-12-05 19:46:46,750 | INFO     | singer_sdk.metrics   | INFO METRIC: {"metric_type": "timer", "metric": "sync_duration", "value": 0.0052759647369384766, "tags": {"stream": "my_stream", "context": {}, "status": "succeeded"}}
2022-12-05 19:46:46,750 | INFO     | singer_sdk.metrics   | INFO METRIC: {"metric_type": "counter", "metric": "record_count", "value": 3, "tags": {"stream": "my_stream", "context": {}}}
```

To use a different log format, see the [custom logging configuration](#custom-logging-configuration) section below.

## Custom logging configuration

Users of a tap can configure the SDK logging by setting the `SINGER_SDK_LOG_CONFIG`
environment variable. The value of this variable should be a path to a YAML file in the
[Python logging dict format](https://docs.python.org/3/library/logging.config.html#dictionary-schema-details).

### Metrics logging

The Singer SDK provides a logger named `singer_sdk.metrics` for logging [Singer metrics](./metrics.md). Metric log records contain an extra field `point` which is a dictionary containing the metric data. The `point` field is formatted as JSON by default.

To send metrics to a file in JSON format, you could use the following config:

```yaml
version: 1
disable_existing_loggers: false
formatters:
  metrics:
    (): pythonjsonlogger.jsonlogger.JsonFormatter
    format: "{created} {message} {point}"
    style: "{"
handlers:
  metrics:
    class: logging.FileHandler
    formatter: metrics
    filename: metrics.jsonl
loggers:
  singer_sdk.metrics:
    level: INFO
    handlers: [ metrics ]
    propagate: no
```

This will send metrics to a `metrics.jsonl`:

```json
{"created": 1705709074.883021, "message": "METRIC", "point": {"type": "timer", "metric": "http_request_duration", "value": 0.501743, "tags": {"stream": "continents", "endpoint": "", "http_status_code": 200, "status": "succeeded"}}}
{"created": 1705709074.897184, "message": "METRIC", "point": {"type": "counter", "metric": "http_request_count", "value": 1, "tags": {"stream": "continents", "endpoint": ""}}}
{"created": 1705709074.897256, "message": "METRIC", "point": {"type": "timer", "metric": "sync_duration", "value": 0.7397160530090332, "tags": {"stream": "continents", "context": {}, "status": "succeeded"}}}
{"created": 1705709074.897292, "message": "METRIC", "point": {"type": "counter", "metric": "record_count", "value": 7, "tags": {"stream": "continents", "context": {}}}}
{"created": 1705709075.397254, "message": "METRIC", "point": {"type": "timer", "metric": "http_request_duration", "value": 0.392148, "tags": {"stream": "countries", "endpoint": "", "http_status_code": 200, "status": "succeeded"}}}
{"created": 1705709075.421888, "message": "METRIC", "point": {"type": "counter", "metric": "http_request_count", "value": 1, "tags": {"stream": "countries", "endpoint": ""}}}
{"created": 1705709075.422001, "message": "METRIC", "point": {"type": "timer", "metric": "sync_duration", "value": 0.5258760452270508, "tags": {"stream": "countries", "context": {}, "status": "succeeded"}}}
{"created": 1705709075.422047, "message": "METRIC", "point": {"type": "counter", "metric": "record_count", "value": 250, "tags": {"stream": "countries", "context": {}}}}
```

## For package developers

If you're developing a tap or target package and would like to customize its logging, you can call [`logging.config.dictConfig`](inv:python:py:function:#logging.config.dictConfig) with a logging [configuration dictionary](inv:python:std:label:#logging-config-dictschema)
in the main plugin module:

```python
# tap_example/tap.py

import logging.config

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {
            "some_package.some_module": {
                "level": "WARNING",
            },
        },
    },
)


class MyTap(Tap):
    ...
```
