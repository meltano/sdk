# Stream Partitioning

The Tap SDK supports stream partitioning, meaning a set of substreams
which each have their own state and their own distinct queryable domain.

## If you do not require partitioning

In general, developers can simply ignore the `context` arguments in methods like
`Stream.get_records()` if partitioning is not required.

## If you do want to utilize partitioning

To take advantage of partitioning, first override the `Stream.partitions` property,
returning a list of dictionaries, where each dictionary uniquely defines the construct of
a partition. For instance, a regionally partitioned stream may return the following:

`[{"region": "us-east"}, {"region": "us-west"}, ...]`

For any streams which define the `partitions` property, the individual partitions will be
passed one at a time through the `partition` argument of methods which reference the
partition, such as `Stream.get_records()`.

## If you are unsure if partitioning will be needed

If you are _unsure_ of whether the stream will be partitioned or not, you can always
pass along the `partition` argument to any other methods which accept it. This will
work regardless of whether partition is an actual partition context or `None`, meaning
no partition is specified.

When dealing with state, for example, developers may always call
`Stream.get_context_state(context)` even if `context` is not set.
The method will automatically return the state that is appropriate, either for the partition
or for the stream.

## See Also

- [Tap SDK State](./implementation/state.md)
- [Tap SDK Parent-Child Streams](./parent_streams.md)
- [Singer Spec: State Overview](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#state)
- [Singer Spec: Config and State](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md#state-file)
