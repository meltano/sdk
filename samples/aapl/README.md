# Tap sample with large catalog

This tap sample helps in evaluating the performance of catalog parsing and stream maps for taps with large schemas and records.

## Execution

```shell
uv run python samples/aapl
```

Or if you want to trace the execution

```shell
uv run viztracer samples/aapl/__main__.py
uv run vizviewer result.json
```
