# Parent-Child Streams

The Tap SDK supports parent-child streams, by which one stream type can be declared to
be a parent to another stream, and the child stream will automatically receive `context`
from a parent record each time the child stream is invoked.

## If you do want to utilize parent-child streams

1. Set `parent_stream_type` in the child-stream's class to the class of the parent.
2. Implement one of the below methods to pass context from the parent to the child:
   1. If using [`get_records`](singer_sdk.Stream.get_child_context) you can simply return a tuple instead of a `record`
      dictionary. A tuple return value will be interpreted by the SDK as
      `(record: dict, child_context: dict)`.
   2. Override [`get_child_context`](singer_sdk.Stream.get_child_context) to return a new
      child context object based on records and any existing context from the parent stream.
   3. If you need to sync more than one child stream per parent record, you can override
      [`generate_child_contexts`](singer_sdk.Stream.generate_child_contexts) to yield as many
      contexts as you need.
3. If the parent stream's replication key won't get updated when child items are changed,
   indicate this by adding `ignore_parent_replication_key = True` in the child stream
   class declaration.
4. If the number of _parent_ items is very large (thousands or tens of thousands), you can
   optionally set [`state_partitioning_keys`](singer_sdk.Stream.state_partitioning_keys) on the child stream to specify a subset of context keys to use
   in state bookmarks. (When not set, the number of bookmarks will be equal to the number
   of parent items.) If you do not wish to store any state bookmarks for the child stream, set[`state_partitioning_keys`](singer_sdk.Stream.state_partitioning_keys) to `[]`.

## Example parent-child implementation

Here is an abbreviated example from the Gitlab sample (also in this repo) which uses the
above techniques.
In this example, EpicIssuesStream is a child of EpicsStream.

```py
class GitlabStream(RESTStream):
    # Base stream definition with auth and pagination logic
    # This logic works for other base classes as well, including Stream, GraphQLStream, etc.


class EpicsStream(GitlabStream):

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
    # Note that this class inherits from the GitlabStream base class, and not from
    # the EpicsStream class.

    name = "epic_issues"

    # EpicIssues streams should be invoked once per parent epic:
    parent_stream_type = EpicsStream

    # Assume epics don't have `updated_at` incremented when issues are changed:
    ignore_parent_replication_keys = True

    # Path is auto-populated using parent context keys:
    path = "/groups/{group_id}/epics/{epic_iid}/issues"

    # ...
```

```{note}
All the keys in the `context` dictionary are added to the child's record, but they will be automatically removed if they are not present in the child's schema. If you wish to preserve these keys in the child's record, you must add them to the child's schema.
```

## Additional Parent-Child References

- [Singer State in SDK Taps](./implementation/state.md)
