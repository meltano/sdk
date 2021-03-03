"""{{ cookiecutter.source_name }} tap class."""

from pathlib import Path
from typing import List
import click
from singer_sdk import Tap, Stream
from singer_sdk.helpers.typing import (
    ArrayType,
    BooleanType,
    ComplexType,
    DateTimeType,
    IntegerType,
    NumberType,
    PropertiesList,
    Property,
    StringType,
)

# TODO: Import your custom stream types here:
from {{ cookiecutter.library_name }}.streams import (
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

class Tap{{ cookiecutter.source_name }}(Tap):
    """{{ cookiecutter.source_name }} tap class."""

    name = "{{ cookiecutter.tap_id }}"
    config_jsonschema = PropertiesList(
        StringType("auth_token", required=True),
        Property("project_ids", ArrayType(StringType), required=True),
        DateTimeType("start_date"),
        StringType("api_url"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

# CLI Execution:

cli = Tap{{ cookiecutter.source_name }}.cli
