# Tap and target performance

The following sections provide different strategies to improve the performance of your tap or target.

## Use `BATCH` mode

See the [BATCH messages](/batch.md) page for more information.

## Use a different message writer or reader

Starting from version `0.45.0`, the Meltano Singer SDK supports different message writers and readers.

For example, to update your tap to use a message writer based the [msgspec](https://github.com/jcrist/msgspec/) serialization library:

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
