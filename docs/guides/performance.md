# Tap and target performance

The following sections provide different strategies to improve the performance of your tap or target.

## Use `BATCH` mode

See the [BATCH messages](/batch.md) page for more information.

## Use a different message writer or reader

Starting from version `0.45.0`, the Meltano Singer SDK supports different message writers and readers.

For example, to update your tap to use a message writer based on the [msgspec](https://github.com/jcrist/msgspec/) serialization library:

```python
from singer_sdk.contrib.msgspec import MsgSpecWriter


class MyTap(Tap):
    message_writer_class = MsgSpecWriter
```

To update your target:

```python
from singer_sdk.contrib.msgspec import MsgSpecReader


class MyTarget(Target):
    message_reader_class = MsgSpecReader
```

## Measuring performance

We've had success using [`viztracer`](https://github.com/gaogaotiantian/viztracer) to create flame graphs for SDK-based packages and find if there are any serious performance bottlenecks.

You can start doing the same in your package. Start by installing `viztracer`.

````{tab} Poetry
```console
$ poetry add --group dev viztracer
```
````

````{tab} uv
```console
$ uv add --group dev viztracer
```
````

Then simply run your package's CLI as normal, preceded by the `viztracer` command

````{tab} Poetry
```console
$ poetry run viztracer my-tap
$ poetry run viztracer -- my-target --config=config.json --input=messages.json
```
````

````{tab} uv
```console
$ uv run viztracer my-tap
$ uv run viztracer -- my-target --config=config.json --input=messages.json
```
````

That command will produce a `result.json` file which you can explore with the `vizviewer` tool.

````{tab} Poetry
```console
$ poetry run vizviewer result.json
```
````

````{tab} uv
```console
$ uv run vizviewer result.json
```
````

The output should look like this

![SDK Flame Graph](https://gitlab.com/meltano/sdk/uploads/07633ba1217de6eb1bb0e018133c608d/_write_record_message.png)

**Note**: Chrome seems to work best for running the `vizviewer` app.
