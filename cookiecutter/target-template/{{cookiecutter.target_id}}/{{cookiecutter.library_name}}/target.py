"""{{ cookiecutter.destination_name }} target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from {{ cookiecutter.library_name }}.sinks import (
    {{ cookiecutter.destination_name }}Sink,
)


class Target{{ cookiecutter.destination_name }}(Target):
    """Sample target for {{ cookiecutter.destination_name }}."""

    name = "{{ cookiecutter.target_id }}"
    config_jsonschema = th.PropertiesList(
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
    ).to_dict()
    default_sink_class = {{ cookiecutter.destination_name }}Sink

    def prepare_target(self) -> None:
        """Set up the target before running.

        This method is called before any messages are processed. It can be used
        to configure the target, open connections, etc.
        """
        pass  # TODO: Delete this method if not needed.

    def cleanup_target(self) -> None:
        """Clean up resources after running.

        This method is called at the end of processing messages, including
        after exceptions are thrown. It can be used to clean up resources
        opened during `prepare_target` such as connections.
        """
        pass  # TODO: Delete this method if not needed.
