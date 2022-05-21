# [SDK Implementation Details](./README.md) - "At Least Once" Delivery Promise

The Since spec promises that each record in the source system will be processed successfully in the target _at least once_. This promises that no record will ever go missing or be omitted, but it _does not_ guarantee that all records will be received _exactly once_.

## Causes of Record Duplication

Record duplication can occur for two reasons: (1) 'greater than or equal' logic in bookmark comparisons, and (2) streams which are retried after interruption or execution failure.

### Cause #1: Greater-than-or-equal-to comparisons

According to the Singer spec, bookmark comparisons are performed on the basis of "greater than or equal to" logic. This ensures that every record arrives in the downstream target _at least once_ and no records are ever missed or omitted during replication. It does also mean that the last record streamed in one execution is likely to be the first record streamed in a subsequent execution.

### Cause #2: Stream interruption

Streams which are retried after failing often already have a subset of records already committed to the target system, but they have not yet received or processed a state message corresponding to those records. When the stream is retried, those records will be sent again to the target, resulting in duplication.

## Recommended Deduplication Strategies

There are basically two recommended approaches for dealing with record duplication.

### Strategy #1: Removing duplicates using primary keys in the target

Assuming that a primary key exists, most target implementation will simply use the primary key to merge newly received records with their prior versions, eliminating any risk of duplication in the destination dataset. However, for streams without primary keys and for implentations running in 'append only' mode, some amount of record duplication should be expected and planned for by the end user.

### Strategy #2: Removing duplicates using `dbt` transformations

For cases where the destination table _does not_ use primary keys, the most common way of resolving duplicates after they've landed in the downstream dataset is to apply a `ROW_NUMBER()` function in a tool like [dbt](https://www.getdbt.com). The `ROW_NUMBER()` function can caculate a `dedupe_rank` or `recency_rank` in the transformation layer, and then downstream queries can easily filter out any duplicates using the calculated rank.

## Future proposals to mitigate record duplication

There is a [feature proposal](https://gitlab.com/meltano/sdk/-/issues/162) for the SDK to store hashes of record data within the tap's state object and then dedupe that hash against new records prior to sending data downstream to the target. This would likely be an opt-in behavior for developers and/or users, and would come at a cost of increased size of the state object. If you are interested in contributing this feature, please see [this issue](https://gitlab.com/meltano/sdk/-/issues/162).

Note that while this proposal would resolve the issue of duplicates due to greater-than-or-equal-to comparison logic, the stream would still be subject ot record duplication from interrupted and retried streams. Thus, any implementations not using primary keys to dedupe data in the target will always need some plan for a deduplication strategy in their downstream processing.
