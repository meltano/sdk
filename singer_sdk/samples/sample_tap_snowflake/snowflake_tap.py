"""Sample tap test for tap-snowflake."""

from typing import List
from singer_sdk import Tap, Stream
from singer_sdk.samples.sample_tap_snowflake.snowflake_tap_stream import (
    SampleTapSnowflakeStream,
)
from singer_sdk.samples.sample_tap_snowflake.snowflake_globals import (
    PLUGIN_NAME,
    ACCEPTED_CONFIG_OPTIONS,
    REQUIRED_CONFIG_SETS,
)


class SampleTapSnowflake(Tap):
    """Sample tap for Snowflake."""

    name = PLUGIN_NAME
    accepted_config_keys = ACCEPTED_CONFIG_OPTIONS
    required_config_options = REQUIRED_CONFIG_SETS

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return SampleTapSnowflakeStream.from_discovery(tap=self)

    def load_streams(self) -> List[Stream]:
        """Load streams, skipping discovery if `input_catalog` is provided."""
        if not self.input_catalog:
            return sorted(self.discover_streams(),key=lambda x: x.name,)
        return SampleTapSnowflakeStream.from_input_catalog(tap=self)


# CLI Execution:

cli = SampleTapSnowflake.cli
