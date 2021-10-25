"""Infer schema tests."""
import os
from typing import List, Optional

import requests

from singer_sdk.streams.core import Stream, TapBaseClass
from singer_sdk.streams.rest import RESTStream
from singer_sdk.tap_base import Tap
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
)


class RestTestStreamNoSchema(RESTStream):
    """Test RESTful stream class."""

    name = "restful"
    path = "/example"
    url_base = "https://example.com"
    primary_keys = ['col1']
    schema = None

    def __init__(self, tap: TapBaseClass, max_inf_records=50):
        super().__init__(tap, max_inf_records=max_inf_records)

    def _request_with_backoff(
            self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        fake_response = requests.Response()
        fake_response._content = str.encode('[{"col1": "val1"}, {"col2": 10}]')
        return fake_response


class SimpleTestTapNoSchemaStream(Tap):
    """Test tap class."""

    name = "test-tap"
    settings_jsonschema = PropertiesList(Property("start_date", DateTimeType)).to_dict()

    def discover_streams(self) -> List[Stream]:
        """List all streams."""
        return [RestTestStreamNoSchema(self)]


def test_infer_schema_rest():
    """Validate infer_schema produces valid schema."""
    stream = RestTestStreamNoSchema(SimpleTestTapNoSchemaStream(), 50)

    assert stream.schema == {
        'properties': {'col1': {'type': 'string'}, 'col2': {'type': 'integer'}},
        'type': 'object',
    }


def test_infer_schema_rest_change_max():
    """Validate infer_schema produces valid schema."""
    stream = RestTestStreamNoSchema(SimpleTestTapNoSchemaStream(), 1)
    stream.max_inf_records = 1

    print(stream.schema)
    assert stream.schema == {
        'properties': {'col1': {'type': 'string'}},
        'required': ['col1'],
        'type': 'object',
    }
