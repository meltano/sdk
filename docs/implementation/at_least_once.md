# "At Least Once" Delivery Promise

The Singer Spec promises that each record in the source system will be processed successfully in the target _at least once_. This promises that no record will ever go missing or be omitted, but it _does not_ guarantee that all records will be received _exactly once_.

## Causes of Record Duplication

Record duplication can occur for three reasons:

1. 'greater than or equal' logic in bookmark comparisons,
1. replication key signposts, as used in unsorted streams and parent-child streams, and
1. streams which are retried after interruption or execution failure.

### Cause #1: Greater-than-or-equal-to comparisons

According to the Singer spec, bookmark comparisons are performed on the basis of "greater than or equal to" logic. This ensures that every record arrives in the downstream target _at least once_ and no records are ever missed or omitted during replication. It does also mean that the last record streamed in one execution is likely to be the first record streamed in a subsequent execution.

### Cause #2: Replication Key Signposts

[Replication Key Signposts](./state.md#replication-key-signposts) are an internal and automatic feature of the SDK. Signposts are necessary in order to deliver the 'at least once' delivery promise for unsorted streams and parent-child streams. The function of a signpost is to ensure that bookmark keys do not advance past a point where we may have not synced all records, such as for unsorted or reverse-sorted streams. This feature also enables developers to override `state_partitioning_key`, which reduces the number of bookmarks needed to track state on parent-child streams with a large number of parent records.

In all applications, the signpost prevents the bookmark's value from advancing too far and prevents records from being skipped in future sync operations. We _intentionally_ do not advance the bookmark as far as the max replication key value from all records we've synced, with the knowledge that _some_ records with equal or lower replication key values may have not yet been synced. It follows then, that any records whose replication key is greater than the signpost value will necessarily be re-synced in the next execution, causing some amount of record duplication downstream.

### Cause #3: Stream interruption

Streams which are retried after failing often have a subset of records already committed to the target system at the time of interruption, but the target likely has not yet received or processed a state message corresponding to those records. When the stream is retried, any records not confirmed as having been received in the state message will be sent again to the target, resulting in duplication.

## Recommended Deduplication Strategies

There are two generally recommended approaches for dealing with record duplication.

### Strategy #1: Removing duplicates using primary keys in the target

Assuming that a primary key exists, most target implementation will simply use the primary key to merge newly received records with their prior versions, eliminating any risk of duplication in the destination dataset.

However, this approach will not work for streams that lack primary keys or in implementations running in pure 'append only' mode. For these cases, some amount of record duplication should be expected and planned for by the end user.

### Strategy #2: Removing duplicates using `dbt` transformations

For cases where the destination table _does not_ use primary keys, the most common way of resolving duplicates after they've landed in the downstream dataset is to apply a `ROW_NUMBER()` function in a tool like [dbt](https://www.getdbt.com). The `ROW_NUMBER()` function can calculate a `dedupe_rank` and/or a `recency_rank` in the transformation layer, and then downstream queries can easily filter out any duplicates using the calculated rank. Users can write these transformations by hand or leverage the [deduplicate-source](https://github.com/dbt-labs/dbt-utils#deduplicate-source) macro from the [dbt-utils](https://github.com/dbt-labs/dbt-utils) package.

#### Sample dedupe implementation using `dbt`:

Within a staging table model file `stg_widgets.sql` in `dbt`:

```sql+jinja
SELECT
    widget_id,
    widget_name,
    widget_desc,
    updated_date,
    ROW_NUMBER() OVER (
        PARTITION BY widget_id, widget_name, widget_desc
        ORDER BY updated_date DESC
    ) AS recency_rank, /* filter `recency_rank = 1` to get only latest records */
    ROW_NUMBER() OVER (
        PARTITION BY widget_id, widget_name, widget_desc, updated_date
        ORDER BY updated_date DESC
    ) AS dedupe_rank /* filter `dedupe_rank = 1` to get only unique records */
FROM {{ source('tap_widgets', 'widgets') }} AS raw
```

## Future proposals to mitigate record duplication

There is a [feature proposal](https://github.com/meltano/sdk/issues/161) for the SDK to optionally store record hashes within the tap's state object and then dedupe record hashes against new records prior to sending data downstream to the target. This would likely be an opt-in behavior for developers and/or users, and it would come at some small performance penalty, as well as a small cost of increased size of the state object. If you are interested in contributing this feature, please see [this issue](https://github.com/meltano/sdk/issues/161).

Note that while this future proposal may resolve the issue of duplicates due to signposts and greater-than-or-equal-to comparison logic, streams will still be subject to record duplication due to interrupted and retried sync operations. Thus, any implementations not using primary keys to dedupe data in the target will always need some plan for a deduplication strategy in their downstream data processing.
