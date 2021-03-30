# [Singer SDK Implementation Details](/.README.md) - Stream State

The Singer SDK automatically handles state management and bookmarks.

## Standard State Format

The Singer Spec does not dictate exactly how state needs to be tracked. However, the basic
structure is documented as follows:

```json
{
  "bookmarks": {
    "orders": {
      // ...
    },
    "customers": {
      // ...
    }
  }
}
```

## Per-Stream STATE Properties

Within each stream definition, the following are considered the minimal effective
implementation:

1. `replication_key` and `replication_key_value` - These two keys together should designate
the incremental replication key (if applicable) and the highest value yet seen for this
property.
1. `<replication-key-property-name>` - For compatibility reasons, we recommend also saving
the replication key name and value as a simple key value pair.

## State Message Frequency

The SDK will automatically generate and emit STATE messages according to the constant 
`Stream.STATE_MSG_FREQUENCY`, which designates how many RECORD messages should be processed
before an updated STATE message should be emitted.

## Backwards Compatibility

While the exact specification of STATE messages may change between versions, it is
nevertheless important that in production scenarios, any STATE bookmarks from prior versions
of the tap are still effective for incremental updates in newer versions. This purposes of 
backwards compatibility, developers may override `Tap.load_state(state) -> dict` in order to
transform legacy STATE formats to the updates minimal spec.

## See Also

- [Singer Spec: State Overview](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#state)
- [Singer Spec: Config and State](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md#state-file)
