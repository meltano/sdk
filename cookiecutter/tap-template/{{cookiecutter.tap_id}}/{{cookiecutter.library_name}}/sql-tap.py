"""{{ cookiecutter.source_name }} tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from {{ cookiecutter.library_name }}.client import {{ cookiecutter.source_name }}Stream


class Tap{{ cookiecutter.source_name }}(SQLTap):
    """Singer tap for {{ cookiecutter.source_name }}."""

    name = "{{ cookiecutter.tap_id }}"

    default_stream_class = {{ cookiecutter.source_name }}Stream

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList().to_dict()


if __name__ == "__main__":
    Tap{{ cookiecutter.source_name }}.cli()
