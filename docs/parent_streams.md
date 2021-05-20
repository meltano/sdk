# Parent-Child Streams

The Singer SDK supports parent-child streams, by which one stream type can be declared to
be a parent to another stream, and the child stream will automatically receive `context`
from a parent record each time the child stream is invoked.

## If you do want to utilize parent-child streams

1. Set `parent_stream_type` in the child-stream's class to the class of the parent.
2. Implement one of the below methods to pass context from the parent to the child:
   1. If using `get_records(context)` you can simply return a tuple instead of a `record`
      dictionary. A tuple return value will be interpreted by the SDK as
      `(record: dict, child_context: dict)`.
   1. Override `get_child_context(record, context: Dict) -> dict` to return a new
      child context object based on records and any existing context from the parent stream.
3. If the parent stream's replication key won't get updated when child items are changed,
   indicate this by adding `ignore_parent_replication_key = True` in the child stream
   class declaration.
4. If the number of _parent_ items is very large (thousands or tends of thousands), you can
   optionally set `state_partitioning_keys` on the child stream to specify a subset of context keys to use
   in state bookmarks. (When not set, the number of bookmarks will be equal to the number
   of parent items.)

## Example parent-child implementation

Here is an abbreviated example from the Gitlab sample (also in this repo) which uses the
above techniques.

```py
class EpicsStream(ProjectBasedStream):

    name = "epics"

    # ...

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "group_id": record["group_id"],
            "epic_id": record["id"],
            "epic_iid": record["iid"],
        }


class EpicIssuesStream(GitlabStream):

    name = "epic_issues"

    # EpicIssues streams should be invoked once per parent epic:
    parent_stream_type = EpicsStream  

    # Assume epics don't have `updated_at` incremented when issues are changed:
    ignore_parent_replication_keys = True

    # Path is auto-populated using parent context keys:
    path = "/groups/{group_id}/epics/{epic_iid}/issues"

    # ...

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in parameterization.
        
        The `context` dictionary provides the parent context from each iterated `Epic`.
        """
        result = super().get_url_params(context, next_page_token)
        if not context or "epic_id" not in context:
            raise ValueError("Cannot sync epic issues without already known epic IDs.")
        return result
```

## See Also

- [Singer SDK State](./implementation/state.md)
