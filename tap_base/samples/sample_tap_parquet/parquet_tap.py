"""Sample tap test for tap-parquet."""

from typing import List
from singer.schema import Schema

from tap_base import Tap, Stream
from tap_base.samples.sample_tap_parquet.parquet_tap_stream import (
    SampleTapParquetStream,
)
from tap_base.samples.sample_tap_parquet.parquet_globals import (
    PLUGIN_NAME,
    ACCEPTED_CONFIG_OPTIONS,
    REQUIRED_CONFIG_SETS,
)


class SampleTapParquet(Tap):
    """Sample tap for Parquet."""

    name: str = PLUGIN_NAME
    accepted_config_keys = ACCEPTED_CONFIG_OPTIONS
    required_config_options = REQUIRED_CONFIG_SETS

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # TODO: automatically infer this from the parquet schema
        result: List[Stream] = []
        for tap_stream_id in ["ASampleTable"]:
            new_stream = SampleTapParquetStream(
                tap=self,
                name=tap_stream_id,
                schema=Schema(
                    properties={
                        "f0": Schema(type=["string", "None"]),
                        "f1": Schema(type=["string", "None"]),
                        "f2": Schema(type=["string", "None"]),
                    }
                ),
            )
            new_stream.primary_keys = ["f0"]
            new_stream.replication_key = "f0"
            result.append(new_stream)
        return result


# CLI Execution:

cli = SampleTapParquet.cli
