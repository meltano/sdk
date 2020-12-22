"""Sample tap test for tap-parquet."""

from typing import List
from singer.schema import Schema

from tap_base import TapBase, TapStreamBase
from tap_base.tests.sample_tap_parquet.parquet_tap_stream import SampleTapParquetStream


ACCEPTED_CONFIG_OPTIONS = ["filepath"]
REQUIRED_CONFIG_SETS = [["filepath"]]


class SampleTapParquet(TapBase):
    """Sample tap for Parquet."""

    name: str = "sample-tap-parquet"
    accepted_config_keys = ACCEPTED_CONFIG_OPTIONS
    required_config_options = REQUIRED_CONFIG_SETS
    default_stream_class = SampleTapParquetStream

    def discover_streams(self) -> List[TapStreamBase]:
        """Return a list of discovered streams."""
        # TODO: automatically infer this from the parquet schema
        result: List[TapStreamBase] = []
        for tap_stream_id in ["ASampleTable"]:
            new_stream = SampleTapParquetStream(
                config=self._config,
                state=self._state,
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

cli = SampleTapParquet.build_cli()
