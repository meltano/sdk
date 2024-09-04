# Incremental Replication

With incremental replication, a Singer tap emits only data that were created or updated since the previous import rather than the full table.

To support incremental replication, the tap must first define how its replication state will be tracked, e.g. the id of the newest record or the maximal update timestamp in the previous import.

You'll either have to manage your own [state file](https://hub.meltano.com/singer/spec#state-files-1), or use Meltano. The Singer SDK makes the tap state available through the [context object](./context_object.md) on subsequent runs. Using the state, the tap should then skip returning rows where the replication key comes _strictly before_ than previous maximal replication key value stored in the state.

## Example Code: Timestamp-Based Incremental Replication

```py
class CommentsStream(RESTStream):

    replication_key = "date_gmt"
    is_sorted = True

    schema = th.PropertiesList(
        th.Property("date_gmt", th.DateTimeType, description="date"),
    ).to_dict()

    def get_url_params(self, context, next_page_token):
        params = {}

        starting_date = self.get_starting_timestamp(context)
        if starting_date:
            params["after"] = starting_date.isoformat()

        if next_page_token is not None:
            params["page"] = next_page_token

        self.logger.info("QUERY PARAMS: %s", params)
        return params
```

1. First we inform the SDK of the `replication_key`, which automatically triggers incremental import mode.

2. Second, optionally, set `is_sorted` to true if the records are monotonically increasing (i.e. newer records always come later). With this setting, the sync will be resumable if it's interrupted at any point and the state file will reflect this. Otherwise, the tap has to run to completion so the state can safely reflect the largest replication value seen.

3. Last, we have to adapt the query to the remote system, in this example by adding a query parameter with the ISO timestamp.

   The [`get_starting_timestamp`](singer_sdk.Stream.get_starting_timestamp) method and the related [`get_starting_replication_key_value`](singer_sdk.Stream.get_starting_replication_key_value) method, are provided by the SDK and return the last replication key value seen in the previous run. If the tap is run for the first time and the value for the `start_date` setting is null, the method will return `None`.

```{note}
- The SDK will throw an error if records come out of order when `is_sorted` is true.
- Unlike a `primary_key`, a `replication_key` does not have to be unique
- In incremental replication, it is OK and usually recommended to resend rows where the replication key is equal to previous highest key. Targets are expected to update rows that are re-synced.
```

## Manually testing incremental import during development

To test the tap in standalone mode, manually create a state file and run the tap:

```shell
$ echo '{"bookmarks": {"documents": {"replication_key": "date_gmt", "replication_key_value": "2023-01-15T12:00:00.120000"}}}' > state_test.json

$ tap-my-example --config tap_config_test.json --state state_test.json
```

## Additional References

- [Tap SDK State](./implementation/state.md)
- [Context Object](./context_object.md)
- [Example tap with get_starting_replication_key_value](https://github.com/flexponsive/tap-eu-ted/blob/main/tap_eu_ted/client.py)
