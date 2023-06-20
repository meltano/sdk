from singer_sdk.target_base import Target
from singer_sdk.sinks import BatchSink

from tests.samples.test_target_csv import TargetMock
from singer_sdk import typing as th


class BatchSinkMock(BatchSink):
    """A mock Sink class."""

    name = "batch-sink-mock"

    def __init__(
        self,
        target: TargetMock,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ):
        """Create the Mock batch-based sink."""
        super().__init__(target, stream_name, schema, key_properties)
        self.target = target

    def process_record(self, record: dict, context: dict) -> None:
        pass

    def process_batch(self, context: dict) -> None:
        pass

class TargetMock(Target):
    """A mock Target class."""

    name = "target-mock"
    config_jsonschema = th.PropertiesList().to_dict()
    default_sink_class = BatchSinkMock

    def __init__(self, *args, **kwargs):
        """Create the Mock target sync."""
        super().__init__(*args, **kwargs)

def test_get_sink():
    input_schema = {
        "properties": {
            "id": {
                "type": [ "string", "null" ]
            },
            "col_ts": {
                "format": "date-time",
                "type": [ "string", "null" ]
            }
        }
    }
    key_properties = []
    target = TargetMock(config={"add_record_metadata": True})
    sink = BatchSinkMock(target, "foo", input_schema, key_properties)
    target._sinks_active["foo"] = sink
    sink_returned = target.get_sink("foo", schema=input_schema, key_properties=key_properties)
    assert sink_returned == sink
