"""Sample target test for target-parquet."""

from tap_base.tests.sample_target_parquet.parquet_target_sink import (
    SampleParquetTargetSink,
)
from tap_base.target_base import TargetBase
from tap_base.tests.sample_target_parquet.parquet_target_globals import (
    PLUGIN_NAME,
    ACCEPTED_CONFIG_KEYS,
    REQUIRED_CONFIG_OPTIONS,
)


class SampleTargetParquet(TargetBase):
    """Sample target for Parquet."""

    name = PLUGIN_NAME
    accepted_config_keys = ACCEPTED_CONFIG_KEYS
    required_config_options = REQUIRED_CONFIG_OPTIONS
    default_sink_class = SampleParquetTargetSink
