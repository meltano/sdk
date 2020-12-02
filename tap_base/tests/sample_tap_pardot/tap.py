"""Sample tap test for tap-pardot."""

from pathlib import Path

from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_pardot.connection import SampleTapPardotConnection
from tap_base.tests.sample_tap_pardot.stream import SampleTapPardotStream


PLUGIN_NAME = "sample-tap-pardot"
PLUGIN_VERSION_FILE = "./VERSION"
PLUGIN_CAPABILITIES = [
    "sync",
    "catalog",
    "discover",
    "state",
]
ACCEPTED_CONFIG = [
    "account",
    "dbname",
    "user",
    "password",
    "warehouse",
    "tables",
]
REQUIRED_CONFIG_SETS = [
    ["account", "dbname", "user", "password", "warehouse", "tables"]
]


class SampleTapPardot(TapBase):
    """Sample tap for Pardot."""

    def __init__(self, config: dict, state: dict = None) -> None:
        """Initialize the tap."""
        vers = Path(PLUGIN_VERSION_FILE).read_text()
        super().__init__(
            plugin_name=PLUGIN_NAME,
            version=vers,
            capabilities=PLUGIN_CAPABILITIES,
            accepted_options=ACCEPTED_CONFIG,
            option_set_requirements=REQUIRED_CONFIG_SETS,
            config=config,
            connection_class=SampleTapPardotConnection,
            state=state,
        )

    # Core plugin metadata:

    def create_stream(self, stream_id: str) -> SampleTapPardotStream:
        return SampleTapPardotStream(stream_id=stream_id, schema=None, properties=None)

    def initialize_stream_from_catalog(
        self,
        stream_id: str,
        friendly_name: str,
        schema: dict,
        metadata: dict,
        upstream_table_name: str,
    ) -> SampleTapPardotStream:
        """Return a tap stream object."""
        return SampleTapPardotStream(
            stream_id, friendly_name, schema, metadata, upstream_table_name
        )
