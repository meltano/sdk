"""{{ cookiecutter.source_name }} tap class."""

from pathlib import Path
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

# TODO: Import your custom stream types here:
from {{ cookiecutter.library_name }}.streams import (
    {{ cookiecutter.source_name }}Stream,
{% if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}
    UsersStream,
    GroupsStream,
{% endif %}
)

{% if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    UsersStream,
    GroupsStream,
]
{% endif %}

class Tap{{ cookiecutter.source_name }}(Tap):
    """{{ cookiecutter.source_name }} tap class."""

    name = "{{ cookiecutter.tap_id }}"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = PropertiesList(
        Property("auth_token", StringType, required=True),
        Property("project_ids", ArrayType(StringType), required=True),
        Property("start_date", DateTimeType),
        Property("api_url", StringType, default="https://api.mysample.com"),
    ).to_dict()

{% if cookiecutter.stream_type in ("GraphQL", "REST", "Other") %}
    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
{% endif %}

# CLI Execution:

cli = Tap{{ cookiecutter.source_name }}.cli
