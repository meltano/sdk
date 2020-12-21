"""Sample tap test for tap-parquet."""

from singer.schema import Schema

import click

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_parquet.parquet_tap_stream import SampleTapParquetStream


ACCEPTED_CONFIG_OPTIONS = ["filepath"]
REQUIRED_CONFIG_SETS = [["filepath"]]


class SampleTapParquet(TapBase):
    """Sample tap for Parquet."""

    name: str = "sample-tap-parquet"
    accepted_config_keys = ACCEPTED_CONFIG_OPTIONS
    required_config_options = REQUIRED_CONFIG_SETS
    default_stream_class = SampleTapParquetStream

    def discover_catalog_streams(self) -> None:
        """Return a dictionary of all streams."""
        # TODO: automatically infer this from the parquet schema
        for tap_stream_id in ["ASampleTable"]:
            schema = Schema(
                properties={
                    "f0": Schema(type=["string", "None"]),
                    "f1": Schema(type=["string", "None"]),
                    "f2": Schema(type=["string", "None"]),
                }
            )
            new_stream = SampleTapParquetStream(
                config=self._config,
                state=self._state,
                name=tap_stream_id,
                schema=schema,
            )
            new_stream.primary_keys = ["f0"]
            new_stream.replication_key = "f0"
            self._streams[tap_stream_id] = new_stream


# CLI Execution:


@click.option("--version", is_flag=True)
@click.option("--discover", is_flag=True)
@click.option("--config")
@click.option("--catalog")
@click.command()
def cli(
    discover: bool = False,
    config: str = None,
    catalog: str = None,
    version: bool = False,
):
    SampleTapParquet.cli(
        version=version, discover=discover, config=config, catalog=catalog
    )
