"""Sample tap test for tap-parquet."""

from pathlib import Path

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_parquet.connection import SampleParquetConnection
from tap_base.tests.sample_tap_parquet.tap_stream import SampleTapParquetStream


PLUGIN_NAME = "sample-tap-parquet"
PLUGIN_VERSION_FILE = "./VERSION"
PLUGIN_CAPABILITIES = [
    "sync",
    "catalog",
    "discover",
    "state",
]
ACCEPTED_CONFIG = ["filepath"]
REQUIRED_CONFIG_SETS = [["filepath"]]


class SampleTapParquet(TapBase):
    """Sample tap for Parquet."""

    _conn: SampleParquetConnection

    def __init__(self, config: dict, state: dict = None) -> None:
        """Initialize the tap."""
        vers = Path(PLUGIN_VERSION_FILE).read_text()
        super().__init__(
            plugin_name=PLUGIN_NAME,
            version=vers,
            capabilities=PLUGIN_CAPABILITIES,
            accepted_options=ACCEPTED_CONFIG,
            option_set_requirements=REQUIRED_CONFIG_SETS,
            connection_class=SampleParquetConnection,
            stream_class=SampleTapParquetStream,
            config=config,
            state=state,
        )
