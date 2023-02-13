# Stream State

The SDK automatically handles state management and bookmarks.

## Important Notice Regarding State

_Note: The descriptions on this page are intended for debugging purposes only. We **do not** recommend reading or writing state directly within your tap._

The SDK was designed to automatically handle the state implementations so that developers
don't have to. If you are building a tap with the SDK and you find a need to directly read or write to the stream's state objects, please open a ticket with a description of your use case and we will use your examples to support more advanced use cases in the future.

## Standard State Format

The Singer Spec does not dictate exactly how state needs to be tracked. However, the basic
structure is documented as follows:

```js
{
  "bookmarks": {
    "orders": {
      // 'orders' state stored here
    },
    "customers": {
      // 'customers' state stored here
    }
  }
}
```

## Per-Stream STATE Properties

Within each stream definition, the following are considered the minimal effective
implementation within the SDK:

1. `replication_key` and `replication_key_value` - These two keys together should designate
the incremental replication key (if applicable) and the highest value yet seen for this
property.

## State Message Frequency

The SDK will automatically generate and emit STATE messages according to the constant
`Stream.STATE_MSG_FREQUENCY`, which designates how many RECORD messages should be processed
before an updated STATE message should be emitted.

## Backwards Compatibility

While the exact specification of STATE messages may change between versions, it is
nevertheless important that in production scenarios, any STATE bookmarks from prior versions
of the tap should still be effective for incremental updates in newer versions.

For the purposes of backwards compatibility, developers may override `Tap.load_state(state)`
in order to translate legacy STATE formats to the updated minimal implementation documented
here.

## Partitioned State

The SDK implements a feature called [`state partitioning`](../partitioning.md) which allows
the same stream to be segmented by one or more partitioning indexes and their values. This
allows multiple bookmarks to be independently tracked for subsets of the total stream.
For instance, using state partitioning, you can track a separate stream bookmark for
records out of the `us_east` and `us_west` API endpoints, even if they are ultimately
being sent downstream to the same target table.

For streams which are partitioned, the SDK will automatically store their stream state
under a `partitions` key which exactly matches their `context`.

For [parent-child streams](../parent_streams.md), the SDK will automatically use the parent's context as the default
state partition.

## Record Duplication

The Singer Spec promises that each record in the source system will be processed successfully in the target _at least once_. This means that no record will ever be omitted from the stream or go missing, but it _does not_ guarantee that all records will be received _exactly once_.

For more information on causes and mitigations of record duplication, please see the [At Least Once](./at_least_once.md) implementation documentation.

## Advanced Use Cases

If some cases, the default behavior would create hundreds or millions of distinct bookmarks,
which ultimately would slow processing and could cause other averse affects. For child
streams with very high granularity parent streams (for instance, emoji reactions on
post comments), the default state partitioning granularity can be overridden by setting
`Stream.state_partitioning_keys`. By specifying a subset of keys to be used in
partitioning rather than the entire default context, you could store distinct bookmarks only
for each post (`[post_id]`) rather than the default parent context of one per post per
comment (`[post_id, comment_id]`).

### Partitioned State Example

In this hypothetical example, our upstream Orders API requires `Store ID` as input, and
as a result we have partitioned the `orders` stream into two partitions: one for store ID
`1` and one for store ID `2`. Splitting into two partitions allows us to automatically track
separate bookmarks for each store, ensuring that we always have proper incremental
replication key values for each one.

Note that in this example, we have a different `replication_key_value` for each partition
and each partition's state contains a unique `context` object to distinguish it from the
others.

```json
{
  "bookmarks": {
    "orders": {
      "partitions": [
        {
          "context": {
            "storeId": 1
          },
          "replication_key": "updatedAt",
          "replication_key_value": "2021-01-02T0:00:00Z"
        },
        {
          "context": {
            "storeId": 2
          },
          "replication_key": "updatedAt",
          "replication_key_value": "2021-01-01T00:00:00Z"
        }
      ]
    }
  }
}
```

## Replication Key is Singular

The SDK's implementation of `replication_key` is intentionally within the
framework of a _singular_ column comparison. Most of those use cases which previously
required multiple bookmarks can now be handled using the [partitioning](../partitioning.md)
feature.

While legacy taps have sometimes supported multiple replication key properties,
this is not yet a supported use case within the SDK. If your source requires multiple
bookmark keys, and if it does not align with the [partitioning](../partitioning.md) feature,
please open an issue with a detailed description of the intended use case.

## The Impact of Sorting on Incremental Sync

For incremental streams sorted by replication key, the replication key
values are tracked in the stream state and
emitted for each batch. Once the state message has been processed and emitted also
by the target, those records preceding the state message should be assumed to be fully
written by the target. In practice, this means that if a sorted stream is interrupted, the
tap may resume from the last successfully processed state message.

To enable resume after interruption, developers may set `is_sorted = True`
within the `Stream` class definition. If this is set, the SDK
will check each record and throw an `InvalidStreamSortException` if unsorted records are
detected during sync.

### Dealing with Unsorted Streams

There are some sources which are unable to send records sorted by their replication key,
even when there is a valid replication key. In these cases, the SDK
creates a separate `progress_tracking` object within the state dictionary. This is used to
track the max value seen for the `replication_key` during the current sync.

Unlike the replication key tracking for pre-sorted streams, the progress trackers will be
ignored (reset and wiped) for the purposes of resuming a failed sync operation. Only when
the sync reaches 100% completion will those progress markers be promoted to a valid
replication key bookmark for future sync operations.

This behavior is also the default for any streams which override `state_partitioning_keys`,
since iterating through multiple parent contexts or partitions will naturally emit
records in an unsorted manner.

### Replication Key Signposts

Signposts are a feature for incremental streams, where a maximum allowable value or
"signpost" is used to prevent the replication key bookmark from advancing beyond the
point where all records have been fully synced. This is especially important when streams
are unsorted, since the presence of _some_ records with timestamps during the sync operation
does not imply that we have _all_ records updated during the sync operation.

Signposts are enabled automatically for datetime replication keys, except when
`Stream.is_sorted` is explicitly set to `True`. Signposts can be created by developers for
non-timestamp replication keys (e.g. for `binlog` and `event_id` types) by overriding
`Stream.get_replication_key_signpost()`.

## Additional Singer State References

- [SDK Partitioning](../partitioning.md)
- [SDK Parent-Child Streams](../parent_streams.md)
- [Singer Spec: State Overview](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#state)
- [Singer Spec: Config and State](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md#state-file)
