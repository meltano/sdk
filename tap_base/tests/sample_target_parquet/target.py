"""Sample target test for target-parquet."""

import copy
import json
import sys
import os

from jsonschema import Draft4Validator, FormatChecker
from pathlib import Path
from typing import Any, Dict, Iterable, List, Type

import pyarrow as pa
import pyarrow.parquet as pq
import singer

from tap_base.tests.sample_target_parquet.target_stream import SampleParquetTargetStream
from tap_base.tests.sample_tap_parquet.tap import SampleTapParquet
from tap_base.target_base import TargetBase

from tap_base.helpers import classproperty


ACCEPTED_CONFIG_OPTIONS = ["filepath"]
REQUIRED_CONFIG_SETS = [["filepath"]]


class SampleTargetParquet(TargetBase):
    """Sample target for Parquet."""

    @classproperty
    def plugin_name(cls) -> str:
        """Return the plugin name."""
        return "sample-target-parquet"

    @classproperty
    def accepted_config_options(cls) -> List[str]:
        return ACCEPTED_CONFIG_OPTIONS

    @classproperty
    def required_config_sets(cls) -> List[List[str]]:
        return REQUIRED_CONFIG_SETS

    @classproperty
    def stream_class(cls) -> Type[SampleParquetTargetStream]:
        """Return the stream class."""
        return SampleParquetTargetStream
