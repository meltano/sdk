"""{{ cookiecutter.destination_name }} target class."""

from __future__ import annotations

{%- set target_class = "SQLTarget" if cookiecutter.serialization_method == "SQL" else "Target" %}

from singer_sdk.target_base import {{ target_class }}
from singer_sdk import typing as th

from {{ cookiecutter.library_name }}.sinks import (
    {{ cookiecutter.destination_name }}Sink,
)


class Target{{ cookiecutter.destination_name }}({{ target_class }}):
    """Sample target for {{ cookiecutter.destination_name }}."""

    name = "{{ cookiecutter.target_id }}"
    config_jsonschema = th.PropertiesList(
        {%- if cookiecutter.serialization_method == "SQL" %}
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description="SQLAlchemy connection string",
        ),
        {%- else %}
        th.Property(
            "filepath",
            th.StringType,
            description="The path to the target output file"
        ),
        th.Property(
            "file_naming_scheme",
            th.StringType,
            description="The scheme with which output files will be named"
        ),
        {%- endif %}
    ).to_dict()

    default_sink_class = {{ cookiecutter.destination_name }}Sink


if __name__ == "__main__":
    Target{{ cookiecutter.destination_name }}.cli()
