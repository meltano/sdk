"""Sample tap test for tap-parquet."""

from logging import Logger
from singer import metadata

from singer.schema import Schema
from tap_base.helpers import classproperty
from typing import Any, Dict, List, Optional, Set, Type

import click

from singer.catalog import CatalogEntry

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_parquet.tap_stream import SampleTapParquetStream


ACCEPTED_CONFIG_OPTIONS = ["filepath"]
REQUIRED_CONFIG_SETS = [["filepath"]]


class SampleTapParquet(TapBase):
    """Sample tap for Parquet."""

    @classproperty
    def plugin_name(cls) -> str:
        """Return the plugin name."""
        return "sample-tap-parquet"

    @classproperty
    def accepted_config_options(cls) -> List[str]:
        return ACCEPTED_CONFIG_OPTIONS

    @classproperty
    def required_config_sets(cls) -> List[List[str]]:
        return REQUIRED_CONFIG_SETS

    @classproperty
    def stream_class(cls) -> Type[SampleTapParquetStream]:
        """Return the stream class."""
        return SampleTapParquetStream

    def discover_catalog_streams(
        self, state: dict, config: dict, logger: Logger
    ) -> None:
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
                tap_stream_id=tap_stream_id,
                schema=schema,
                state=state,
                logger=logger,
                config=config,
            )
            new_stream.primary_key = ["f0"]
            new_stream.bookmark_key = "f0"
            self._streams[tap_stream_id] = new_stream


@click.option("--discover", is_flag=True)
@click.option("--config")
@click.option("--catalog")
@click.command()
def cli(discover: bool = False, config: str = None, catalog: str = None):
    SampleTapParquet.cli(discover=discover, config=config, catalog=catalog)
