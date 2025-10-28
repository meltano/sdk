# The Context Object

Many of the methods in the [Stream](classes/singer_sdk.Stream) class and its subclasses accept
a `context` parameter, which is a dictionary that contains information about the stream
partition or parent stream.

## Best practices for using context

- The context object MUST NOT contain any sensitive information, such as API keys or secrets.
  This is because the context is<br><br>

  1. sent to the target,
  1. stored in the state file and
  1. logged to the console as a tag in metrics and logs.<br><br>

- The context object SHOULD NOT be mutated during the stream's lifecycle. This is because the
  context is stored in the state file, and mutating it will cause the state file to be
  inconsistent with the actual state of the stream.

## Modifying the context dictionary

If you need to modify the context dictionary (for example, to remove large data payloads from
parent streams), you should override the
[`preprocess_context`](singer_sdk.Stream.preprocess_context) method. This is the
**only** appropriate place to modify the context.

The `preprocess_context` method is called at the very beginning of the sync operation, before
the context is frozen (made immutable). After this method returns, the context cannot be
modified for the remainder of the sync.

### Example: Removing large payloads from parent context

```python
from singer_sdk.streams import RESTStream


class TeamsStream(RESTStream):
    name = "teams"
    path = "/teams"
    primary_keys = ["id"]
    replication_key = "updated_at"

    def generate_child_contexts(self, record, context):
        yield {
          "team_id": record["id"],
          "member_ids": record["member_ids"],
        }


class TeamMembersStream(RESTStream):
    name = "team_members"
    path = "/teams/{team_id}/members"
    http_method = "POST"
    parent_stream_type = TeamsStream

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._member_ids = []

    def set_member_ids(self, member_id):
        self._member_ids = member_ids

    def prepare_request_payload(self, context, next_page_token):
        return {
            "ids": self._member_ids,
        }

    def preprocess_context(self, context):
        """Remove large data from parent context to reduce state size."""
        # Keep only the keys we need, discard the rest
        self.set_member_ids(context.pop("member_ids", [])
        return context
```
