"""Infer schema tests."""
from pathlib import Path
from typing import List, Optional

import requests

from singer_sdk.streams.core import Stream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.tap_base import Tap
from singer_sdk.typing import NumberType, PropertiesList, Property

resource_path = Path(__file__).parent / "resources"


def request_with_backoff():
    fake_response = requests.Response()
    fake_response._content = str.encode('[{"col1": "val1"}, {"col2": 10}]')
    return fake_response


# Tests


def test_infer_schema_rest_no_schema():
    """Validate providing no schema raises error."""

    class InferSchemaStream(RESTStream):
        """Test RESTful stream class."""

        name = "restful"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {}

    class InferSchemaTap(Tap):
        """Test tap class."""

        name = "test-tap"

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [InferSchemaStream(self)]

    try:
        InferSchemaTap()
        assert False
    except ValueError:
        assert True


def test_infer_schema_rest_path_and_record():
    """Validate providing both produces schema and writes to the schema_filepath."""

    class InferSchemaStream(RESTStream):
        """Test RESTful stream class."""

        name = "restful"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {}
        schema_filepath = resource_path  # todo: do not actually write
        schema_inference_record_count = 5

        def _request_with_backoff(
            self, prepared_request: requests.PreparedRequest, context: Optional[dict]
        ) -> requests.Response:
            return request_with_backoff()

    class InferSchemaTap(Tap):
        """Test tap class."""

        name = "test-tap"

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [InferSchemaStream(self)]

    tap = InferSchemaTap()
    stream = tap.streams["restful"]
    path = resource_path / "restful.json"

    assert stream.schema == {
        "properties": {"col1": {"type": "string"}, "col2": {"type": "integer"}},
        "type": "object",
    }
    assert path.exists()


def test_infer_schema_rest_records():
    """Validate providing count only produces schema without writing to any file."""

    class InferSchemaStream(RESTStream):
        """Test RESTful stream class."""

        name = "records_only"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {}
        schema_inference_record_count = 5

        def _request_with_backoff(
            self, prepared_request: requests.PreparedRequest, context: Optional[dict]
        ) -> requests.Response:
            return request_with_backoff()

    class InferSchemaTap(Tap):
        """Test tap class."""

        name = "test-tap"

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [InferSchemaStream(self)]

    tap = InferSchemaTap()
    stream = tap.streams["records_only"]
    path = resource_path / "records_only.json"

    assert stream.schema == {
        "properties": {"col1": {"type": "string"}, "col2": {"type": "integer"}},
        "type": "object",
    }
    assert not path.exists()


def test_rest_from_schema_path():
    """Validate providing _schema_filepath only produces schema from the file."""

    class InferSchemaStream(RESTStream):
        """Test RESTful stream class."""

        name = "restful"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {}
        schema_filepath = resource_path / "restful.json"

    class InferSchemaTap(Tap):
        """Test tap class."""

        name = "test-tap"

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [InferSchemaStream(self)]

    tap = InferSchemaTap()
    stream = tap.streams["restful"]

    assert stream.schema == {
        "properties": {"col1": {"type": "string"}, "col2": {"type": "integer"}},
        "type": "object",
    }


def test_rest_schema_provided():
    """Validate providing _schema_filepath only produces schema from the file."""

    class InferSchemaStream(RESTStream):
        """Test RESTful stream class."""

        name = "restful"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {
            "properties": {"this": {"type": "string"}, "that": {"type": "integer"}},
            "type": "object",
        }

    class InferSchemaTap(Tap):
        """Test tap class."""

        name = "test-tap"

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [InferSchemaStream(self)]

    tap = InferSchemaTap()
    stream = tap.streams["restful"]

    assert stream.schema == {
        "properties": {"this": {"type": "string"}, "that": {"type": "integer"}},
        "type": "object",
    }


def test_infer_schema_rest_limit_records():
    """Validate infer_schema can be limited by the record count setting."""

    class InferSchemaStream(RESTStream):
        """Test RESTful stream class."""

        name = "records_only"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {}
        schema_inference_record_count = 1

        def _request_with_backoff(
            self, prepared_request: requests.PreparedRequest, context: Optional[dict]
        ) -> requests.Response:
            return request_with_backoff()

    class InferSchemaTap(Tap):
        """Test tap class."""

        name = "test-tap"

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [InferSchemaStream(self)]

    tap = InferSchemaTap()
    stream = tap.streams["records_only"]

    print(stream.schema)
    assert stream.schema == {
        "properties": {"col1": {"type": "string"}},
        "required": ["col1"],
        "type": "object",
    }


def test_infer_schema_rest_records_by_config():
    """Validate providing count in config produces same result as other scenarios."""

    class InferSchemaStream(RESTStream):
        """Test RESTful stream class."""

        name = "records_only"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {}

        def _request_with_backoff(
            self, prepared_request: requests.PreparedRequest, context: Optional[dict]
        ) -> requests.Response:
            return request_with_backoff()

    class InferSchemaTap(Tap):
        """Test tap class."""

        name = "test-tap"

        settings_jsonschema = PropertiesList(
            Property("schema_inference_record_count", NumberType),
        ).to_dict()

        config = {"schema_inference_record_count": 1}

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [InferSchemaStream(self)]

    tap = InferSchemaTap()
    stream = tap.streams["records_only"]

    print(stream.schema)
    assert stream.schema == {
        "properties": {"col1": {"type": "string"}},
        "required": ["col1"],
        "type": "object",
    }
