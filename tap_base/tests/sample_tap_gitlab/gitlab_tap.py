"""Sample tap test for tap-gitlab."""

import json
from logging import Logger
from tap_base.streams.rest import RESTStreamBase
from typing import Any, Dict, List, Optional, Type, Union
from pathlib import Path

from singer.schema import Schema

import click

from tap_base.helpers import classproperty
from tap_base.tap_base import TapBase
from tap_base.tests.sample_tap_gitlab.gitlab_rest_streams import GitlabStream
from tap_base.tests.sample_tap_gitlab.gitlab_graphql_streams import (
    GraphQLCurrentUserStream,
)


ACCEPTED_CONFIG_OPTIONS = ["auth_token", "project_ids", "start_date"]
REQUIRED_CONFIG_SETS = [["auth_token", "project_ids", "start_date"]]
SCHEMAS_DIR = "./tap_base/tests/sample_tap_gitlab/schemas"

STREAM_TYPES: Dict[
    Type[RESTStreamBase], Dict[str, Dict[str, Union[None, str, List[str]]]],
] = {
    GitlabStream: {
        "projects": {
            "url_suffix": "/projects/{project_id}?statistics=1",
            "primary_keys": ["id"],
            "replication_key": None,
        },
        "releases": {
            "url_suffix": "/projects/{project_id}/releases",
            "primary_keys": ["project_id", "commit_id", "tag_name"],
            "replication_key": None,
        },
        "issues": {
            "url_suffix": "/projects/{project_id}/issues?scope=all&updated_after={start_date}}",
            "primary_keys": ["id"],
            "replication_key": None,
        },
        "commits": {
            "url_suffix": "/projects/{project_id}/repository/commits?since={start_date}&with_stats=true",
            "primary_keys": ["id"],
            "replication_key": None,
        },
    },
}


class SampleTapGitlab(TapBase):
    """Sample tap for Gitlab."""

    name: str = "sample-tap-gitlab"
    accepted_config_keys = ACCEPTED_CONFIG_OPTIONS
    required_config_options = REQUIRED_CONFIG_SETS

    def discover_catalog_streams(self) -> None:
        """Initialize self._streams with a dictionary of all streams."""
        # Add REST Streams
        for stream_class, streams_dict in STREAM_TYPES.items():
            for stream_name, stream_def in streams_dict.items():
                schemafile = Path(SCHEMAS_DIR) / f"{stream_name}.json"
                schema = json.loads(Path(schemafile).read_text())
                new_stream = stream_class(
                    name=stream_name,
                    schema=schema,
                    state={},
                    config=self._config,
                    url_pattern=stream_def["url_suffix"],
                )
                new_stream.primary_keys = stream_def.get("primary_keys", None)
                new_stream.replication_key = stream_def.get("replication_key", None)
                self._streams[stream_name] = new_stream

        # Add GraphQL Streams
        new_stream = GraphQLCurrentUserStream(
            config=self._config, state={}
        )
        self._streams[new_stream.name] = new_stream


# CLI Execution:
# TODO: Move entirely to base class (https://gitlab.com/meltano/tap-base/-/issues/8)


@click.option("--version", is_flag=True)
@click.option("--discover", is_flag=True)
@click.option("--config")
@click.option("--catalog")
@click.command()
def cli(
    discover: bool = False,
    config: str = None,
    catalog: str = None,
    version: bool = False,
):
    SampleTapGitlab.cli(
        version=version, discover=discover, config=config, catalog=catalog
    )
