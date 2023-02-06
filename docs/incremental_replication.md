# Incremental Replication

With incremental replication, a Singer tap emits only data that were created or updated since the previous import rather than the full table.

To support incremental replication, the tap must first define how its replication state will be tracked, e.g. the id of the newest record or the maximal update timestamp in the previous import. Meltano stores the state and makes it available through the context object on subsequent runs. Using the state, the tap should then skip returning rows where the replication key value is less than previous maximal replication key value stored in the state.

## Example Code: Timestamp-Based Incremental Replication

```py
class CommentsStream(RESTStream):

    replication_key = "date_gmt"
    is_sorted = True

    schema = th.PropertiesList(
        th.Property("date_gmt", th.DateTimeType, description="date"),
    ).to_dict()

    def get_url_params(
        self, context: dict | None, next_page_token
    ) -> dict[str, Any]:
        params = {}
        if starting_date := self.get_starting_timestamp(context):
            params["after"] = starting_date.isoformat()
        if next_page_token is not None:
            params["page"] = next_page_token
        self.logger.info(f"QUERY PARAMS: {params}")
        return params

    url_base = "https://example.com/wp-json/wp/v2/comments"
    authenticator = None
```

First we inform the SDK of the `replication_key`, which automatically triggers incremental import mode. Second, optionally, set `is_sorted` to true; with this setting, Singer will throw an error if a supposedly incremental import sends results older than the starting timestamp.

Last, we have to adapt the query to the remote system, in this example by adding a query parameter with the ISO timestamp.

Note:
- unlike a `primary_key`, a `replication_key` does not have to be unique
- in incremental replication, it is OK to resend rows where the replication key is equal to previous highest key.

## Manually testing incremental import during development

To test the tap stand-alone, manually create a state file and run the tap:

```shell
$ echo '{"bookmarks": {"documents": {"replication_key": "date_gmt", "replication_key_value": "2023-01-15T12:00:00.120000"}}}' > state_test.json

$ tap-my-example --config tap_config_test.json --state state_test.json
```

## Additional References

- [Tap SDK State](./implementation/state.md)
- [Context Object](./context_object.md)
- [Example tap with get_starting_replication_key_value](https://github.com/flexponsive/tap-eu-ted/blob/main/tap_eu_ted/client.py)