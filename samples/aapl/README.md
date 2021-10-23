# Tap sample with large catalog

This tap sample helps in evaluating the performance of catalog parsing and stream maps for taps with large schemas and records.

## Execution

```shell
poetry run python samples/aapl
```

Or if you want to trace the execution

```shell
poetry run viztracer samples/aapl/__main__.py
poetry run vizviewer result.json
```
