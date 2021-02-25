"""{{ cookiecutter.source_name }} tap class."""

from pathlib import Path
from typing import List
import click
from singer_sdk import TapBase
from singer_sdk.helpers.typing import (
    ArrayType,
    BooleanType,
    ComplexType,
    DateTimeType,
    IntegerType,
    PropertiesList,
    StringType,
)

# TODO: Import your custom stream types here:
from {{ cookiecutter.library_name }}.stream import (
    Tap{{ cookiecutter.source_name }}Stream,
    StreamA,
    StreamB,
)

PLUGIN_NAME = "{{ cookiecutter.tap_id }}"

# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    StreamA,
    StreamB,
]

class Tap{{ cookiecutter.source_name }}(TapBase):
    """{{ cookiecutter.source_name }} tap class."""

    name = "{{ cookiecutter.tap_id }}"
    config_jsonschema = PropertiesList(
        StringType("auth_token"),
        ArrayType("project_ids", StringType),
        DateTimeType("start_date"),
        StringType("api_url", optional=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

# CLI Execution:

cli = Tap{{ cookiecutter.source_name }}.cli
