"""Sample target test for target-parquet."""

from tap_base.target_base import TargetBase
from tap_base import typehelpers as th

from tap_base.samples.sample_target_parquet.parquet_target_sink import (
    SampleParquetTargetSink,
)
from tap_base.samples.sample_target_parquet.parquet_target_globals import PLUGIN_NAME


class SampleTargetParquet(TargetBase):
    """Sample target for Parquet."""

    name = PLUGIN_NAME
    config_jsonschema = th.PropertiesList(
        th.StringType("filepath", optional=True),
        th.StringType("file_naming_scheme", optional=True),
    ).to_dict()
    default_sink_class = SampleParquetTargetSink
