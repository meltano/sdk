"""{{ cookiecutter.destination_name }} target class."""

from pathlib import Path
from typing import List

from singer_sdk.target_base import Target
from singer_sdk.sinks import Sink
from singer_sdk import typing as th

from {{ cookiecutter.library_name }}.sinks import (
    {{ cookiecutter.destination_name }}Sink,
)


class Target{{ cookiecutter.destination_name }}(Target):
    """Sample target for {{ cookiecutter.destination_name }}."""

    name = "{{ cookiecutter.target_id }}"
    config_jsonschema = th.PropertiesList(
        th.Property("filepath", th.StringType),
        th.Property("file_naming_scheme", th.StringType),
    ).to_dict()
    default_sink_class = {{ cookiecutter.destination_name }}Sink
