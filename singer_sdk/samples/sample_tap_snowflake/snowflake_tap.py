"""Sample tap test for tap-snowflake."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.helpers.typing import (
    ArrayType,
    PropertiesList,
    Property,
    StringType,
)
from singer_sdk.samples.sample_tap_snowflake.snowflake_tap_stream import (
    SampleTapSnowflakeStream,
)

PLUGIN_NAME = "sample-tap-snowflake"


class SampleTapSnowflake(Tap):
    """Sample tap for Snowflake."""

    name = PLUGIN_NAME
    config_jsonschema = PropertiesList(
        Property("account", StringType, required=True),
        Property("dbname", StringType, required=True),
        Property("warehouse", StringType, required=True),
        Property("user", StringType, required=True),
        Property("password", StringType, required=True),
        Property("tables", ArrayType(StringType)),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return SampleTapSnowflakeStream.from_discovery(tap=self)

    def load_streams(self) -> List[Stream]:
        """Load streams, skipping discovery if `input_catalog` is provided."""
        if not self.input_catalog:
            return sorted(
                self.discover_streams(),
                key=lambda x: x.name,
            )
        return SampleTapSnowflakeStream.from_input_catalog(tap=self)


# CLI Execution:

cli = SampleTapSnowflake.cli
