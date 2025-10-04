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

### Structured logging

The Singer SDK supports structured JSON logging through the `StructuredFormatter`. This formatter emits logs as structured JSON objects.

#### Schema overview

The structured logging schema defines the following fields:

| Field | Type | Required | Description |
| ------------- | -------------- | -------- | --------------------------------------------------------------- |
| `level` | string | Yes | Log level (debug, info, warning, error, critical) |
| `pid` | integer | Yes | Process ID |
| `logger_name` | string | Yes | Name of the logger that emitted the log |
| `ts` | number | Yes | Timestamp of the log (Unix timestamp) |
| `thread_name` | string or null | Yes | Name of the thread that produced the log |
| `app_name` | string | Yes | Name of the application (e.g., "tap-github", "target-postgres") |
| `stream_name` | string or null | Yes | Name of the stream that produced the log |
| `message` | string | Yes | The log message |
| `extra` | object | Yes | Additional fields from the log record |
| `metric_info` | object | No | Metric information for METRIC logs |
| `exception` | object | No | Exception information for ERROR logs |

#### Exception structure

When an exception occurs, the `exception` field contains a structured representation of the exception with the following properties:

| Field | Type | Description |
| ----- | ---- | ----------- |
| `type` | string | The exception class name (e.g., "ValueError", "RuntimeError") |
| `module` | string | The module containing the exception class (e.g., "builtins") |
| `message` | string | The exception message |
| `traceback` | array | Array of traceback frames, each containing `filename`, `function`, and `lineno` |
| `cause` | object | The exception that caused this exception (when using `raise ... from ...`) |
| `context` | object | The exception context (when an exception occurs during exception handling) |

Example structured exception output:

```json
{
  "level": "error",
  "message": "Database operation failed",
  "exception": {
    "type": "DatabaseError",
    "module": "psycopg2.errors",
    "message": "connection to server was lost",
    "traceback": [
      {
        "filename": "/app/database.py",
        "function": "execute_query",
        "lineno": 42
      },
      {
        "filename": "/app/main.py",
        "function": "process_records",
        "lineno": 156
      }
    ],
    "cause": {
      "type": "ConnectionError",
      "module": "psycopg2.errors",
      "message": "server closed the connection unexpectedly"
    }
  }
}
```

### Metrics logging

The Singer SDK provides a logger named `singer_sdk.metrics` for logging [Singer metrics](./metrics.md). Metric log records contain an extra field `point` which is a dictionary containing the metric data. The `point` field is formatted as JSON by default.

To send metrics to a file in JSON format, you could use the following config:

```yaml
version: 1
disable_existing_loggers: false
formatters:
  metrics:
    (): pythonjsonlogger.jsonlogger.JsonFormatter
    format: "{created} {point}"
    style: "{"
handlers:
  metrics:
    class: logging.FileHandler
    formatter: metrics
    filename: metrics.jsonl
# Optionally, you can exclude metrics
filters:
  remove_events_stream_metrics:
    (): singer_sdk.metrics.MetricExclusionFilter
    tags:
      stream: events
loggers:
  singer_sdk.metrics:
    level: INFO
    handlers: [ metrics ]
    filters: [ remove_events_stream_metrics ]
    propagate: no
```

This will send metrics to a `metrics.jsonl`:

```json
{"created": 1705709074.883021, "point": {"type": "timer", "metric": "http_request_duration", "value": 0.501743, "tags": {"stream": "continents", "endpoint": "", "http_status_code": 200, "status": "succeeded"}}}
{"created": 1705709074.897184, "point": {"type": "counter", "metric": "http_request_count", "value": 1, "tags": {"stream": "continents", "endpoint": ""}}}
{"created": 1705709074.897256, "point": {"type": "timer", "metric": "sync_duration", "value": 0.7397160530090332, "tags": {"stream": "continents", "context": {}, "status": "succeeded"}}}
{"created": 1705709074.897292, "point": {"type": "counter", "metric": "record_count", "value": 7, "tags": {"stream": "continents", "context": {}}}}
{"created": 1705709075.397254, "point": {"type": "timer", "metric": "http_request_duration", "value": 0.392148, "tags": {"stream": "countries", "endpoint": "", "http_status_code": 200, "status": "succeeded"}}}
{"created": 1705709075.421888, "point": {"type": "counter", "metric": "http_request_count", "value": 1, "tags": {"stream": "countries", "endpoint": ""}}}
{"created": 1705709075.422001, "point": {"type": "timer", "metric": "sync_duration", "value": 0.5258760452270508, "tags": {"stream": "countries", "context": {}, "status": "succeeded"}}}
{"created": 1705709075.422047, "point": {"type": "counter", "metric": "record_count", "value": 250, "tags": {"stream": "countries", "context": {}}}}
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
