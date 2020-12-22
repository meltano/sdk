"""Sample tap test for tap-snowflake."""

from typing import List
from tap_base import TapBase, TapStreamBase
from tap_base.samples.sample_tap_snowflake.snowflake_tap_stream import (
    SampleTapSnowflakeStream,
)
from tap_base.samples.sample_tap_snowflake.snowflake_config import (
    PLUGIN_NAME,
    ACCEPTED_CONFIG_OPTIONS,
    REQUIRED_CONFIG_SETS,
)


class SampleTapSnowflake(TapBase):
    """Sample tap for Snowflake."""

    name = PLUGIN_NAME
    accepted_config_keys = ACCEPTED_CONFIG_OPTIONS
    required_config_options = REQUIRED_CONFIG_SETS
    default_stream_class = SampleTapSnowflakeStream

    def discover_streams(self) -> List[TapStreamBase]:
        """Return a list of discovered streams."""
        stream: SampleTapSnowflakeStream
        for stream in SampleTapSnowflakeStream.from_discovery(config=self._config):
            self._streams[stream.name] = stream


# CLI Execution:

cli = SampleTapSnowflake.build_cli()
