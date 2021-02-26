"""Sample tap test for tap-parquet."""

from typing import List
from singer.schema import Schema

from singer_sdk import Tap, Stream
from singer_sdk.samples.sample_tap_parquet.parquet_tap_stream import (
    SampleTapParquetStream,
)
from singer_sdk.samples.sample_tap_parquet.parquet_globals import PLUGIN_NAME
from singer_sdk.helpers.typing import (
    PropertiesList,
    StringType,
    ComplexType,
    DateTimeType,
    BooleanType,
)


class SampleTapParquet(Tap):
    """Sample tap for Parquet."""

    name: str = PLUGIN_NAME
    config_jsonschema = PropertiesList(StringType("filepath")).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # TODO: automatically infer this from the parquet schema
        result: List[Stream] = []
        for tap_stream_id in ["ASampleTable"]:
            new_stream = SampleTapParquetStream(
                tap=self,
                name=tap_stream_id,
                schema=PropertiesList(
                    StringType("f0", required=True),
                    StringType("f1"),
                    StringType("f2"),
                ).to_dict(),
            )
            new_stream.primary_keys = ["f0"]
            new_stream.replication_key = "f0"
            result.append(new_stream)
        return result


# CLI Execution:

cli = SampleTapParquet.cli
