# Frequently Asked Questions

## What if I select the wrong option in the cookiecutter prompt?

Most likely you should delete the project and start over.

## What are all of the Property options?

The property types are documented in the [JSON Schema helper classes](./reference.rst).
However, if you're using an IDE such as VSCode, you should be able to set up the environment to give you autocompletion prompts or hints.
Ensure your interpreter is set to the local virtual environment if you've followed the [Dev Guide](./dev_guide.md).
Checkout this [gif](https://visualstudiomagazine.com/articles/2021/04/20/~/media/ECG/visualstudiomagazine/Images/2021/04/poetry.ashx) for how to change your interpreter.

### Handling credentials and other secrets in config

As of SDK version `0.13.0`, developers can use the `secret=True` indication in the `Property` class constructor to flag secrets such as API tokens and passwords. We recommend all developers use this option where applicable so that orchestrators may consider this designation when determining how to store the user's provided config.

## I'm having trouble getting the base class to **init**.

Ensure you're using the `super()` method to inherit methods from the base class.

```python
class MyStream(Stream):
    """Mystream stream class."""
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.conn...
```

## I'm seeing `Note: Progress is not resumable if interrupted.` in my state files

If the stream attribute [`is_sorted`](singer_sdk.Stream.is_sorted) (default: `False`) is not set to `True`, the records are assumed not to be monotonically increasing, i.e. there's no guarantee that newer records always come later and the tap has to run to completion so the state can safely reflect the largest replication value seen. If you know that the records are monotonically increasing, you can set `is_sorted` to `True` and the sync will be resumable if it's interrupted at any point and the state file will reflect this.

```python
class MyStream(Stream):
    is_sorted = True
```

## Why is my stream sync not resumed after it's interrupted?

For a stream to be resumable, it must meet these criteria:

1. **Use incremental replication** - The stream must have `replication_method = "INCREMENTAL"` and a `replication_key` defined
1. **Be declared as sorted** - The stream must have `is_sorted = True` (see below)
1. **Not use custom state partitioning** - Streams with custom `state_partitioning_keys` are not resumable

Full-table syncs and unsorted incremental streams cannot resume from interruptions. They must complete successfully before the state becomes valid.

## How do I declare my stream as sorted?

To enable resumability, set the `is_sorted` property to `True` in your stream class:

```python
class MyIncrementalStream(Stream):
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    is_sorted = True  # Records are sorted by replication_key
```

Only declare a stream as sorted if the source API guarantees that records are returned in ascending order by the replication key. This means each record's replication key value must be greater than or equal to the previous record's value.

## What happens if records arrive out of order in a sorted stream?

If you declare `is_sorted = True` but records arrive out of order, the SDK will raise an `InvalidStreamSortException` and the sync will fail immediately. This protects data integrity by ensuring you don't miss records.

For example, if a record with `updated_at = "2024-01-15"` arrives after a record with `updated_at = "2024-01-20"`, the SDK detects this violation and stops the sync.

To bypass this check (not recommended), set `check_sorted = False`:

```python
class MyStream(Stream):
    is_sorted = True
    check_sorted = False  # Disable out-of-order detection
```

However, disabling this check may result in data loss if records truly are out of order.
