# [Singer SDK Implementation Details](/.README.md) - Stream Partitioning

The Singer SDK supports stream paritioning, which is to say a set of substreams which each
can have their own STATE and their own distinct queryable domain.

## If you do not require partitioning

In general, developers can simply ignore the `partition` arguments in methods like
`Stream.get_records()` if partitioning is not required.

## If you do want to utilize partitioning

To take advantage of paritioning, first override the `Stream.partitions` property,
returning a list of dictionaries, where each dictionary uniquely defines the construct of
a partition. For instance, a regionally partitioned stream may return the following:

`[{"region": "us-east"}, {"region": "us-west"}, ...]`

For any streams which define the `partitions` property, the individual partitions will be
passed one at a time through the `partition` argument of methods which reference the 
partition, such as `Stream.get_records()`.

## If you are unsure if partitioning will be needed

If you are _unsure_ of whether the stream will be partitioned or not, you can always just
pass along the `paritition` argument to any other methods which accept it.

For example, developers may always call `Stream.get_stream_or_partition_state(partition)`,
which retreives a writable copy of the state for _either_ the stream (if `partition`
is `None`) or for the `partition` (if `partition` is not `None`).

## See Also

- [Singer SDK State](./state.md)
- [Singer Spec: State Overview](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#state)
- [Singer Spec: Config and State](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md#state-file)
