"""Infer schema tests."""
import json
from pathlib import Path
from typing import List, Optional

import requests

from singer_sdk.streams.core import Stream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.tap_base import Tap
from singer_sdk.typing import NumberType, PropertiesList, Property

resource_path = Path(__file__).parent / "resources"

# test data structures

basic = [{"col1": "val1"}, {"col2": 10}]


def request_with_backoff(content=basic) -> requests.Response:
    """Mock request fucntion with variable data."""
    fake_response = requests.Response()
    fake_response._content = str.encode(json.dumps(content))
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

    assert stream.schema == {
        "properties": {"col1": {"type": "string"}},
        "required": ["col1"],
        "type": "object",
    }


def test_infer_schema_rest_records_by_config_stream():
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
            Property("schema_inference_record_count_records_only", NumberType),
            Property("schema_inference_record_count", NumberType),
        ).to_dict()

        config = {
            "schema_inference_record_count_records_only": 1,
            "schema_inference_record_count": 50,
        }

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [InferSchemaStream(self)]

    tap = InferSchemaTap()
    stream = tap.streams["records_only"]

    assert stream.schema == {
        "properties": {"col1": {"type": "string"}},
        "required": ["col1"],
        "type": "object",
    }


def test_infer_schema_child_stream():
    """Validate schema inference for child streams."""

    class ParentStream(RESTStream):
        """Test RESTful stream class."""

        name = "parent"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {}
        schema_inference_record_count = 2

        def _request_with_backoff(
            self, prepared_request: requests.PreparedRequest, context: Optional[dict]
        ) -> requests.Response:
            return request_with_backoff([{"id": 1}, {"value": "foo"}])

        def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
            """Return a context dictionary for child streams."""
            return {"parent_id": record["id"]}

    class ChildStream(RESTStream):
        """Test RESTful stream class."""

        parent_stream_type = ParentStream
        ignore_parent_replication_keys = True

        name = "child"
        path = "/example"
        url_base = "https://example.com"
        primary_keys = ["col1"]
        schema = {}
        schema_inference_record_count = 1

        def _request_with_backoff(
            self, prepared_request: requests.PreparedRequest, context: Optional[dict]
        ) -> requests.Response:
            return request_with_backoff(
                [
                    {"parent_id": 1, "toy": "bar"},
                    {"parent_id": 2, "toy": "spam"},
                ]
            )

    class InferSchemaTap(Tap):
        """Test tap class."""

        name = "test-tap"

        def discover_streams(self) -> List[Stream]:
            """List all streams."""
            return [ParentStream(self), ChildStream(self)]

    tap = InferSchemaTap()
    parent = tap.streams["parent"]
    child = tap.streams["child"]

    assert parent.schema == {
        "properties": {"id": {"type": "integer"}, "value": {"type": "string"}},
        "type": "object",
    }
    assert child.schema == {
        "properties": {"parent_id": {"type": "integer"}, "toy": {"type": "string"}},
        "required": ["parent_id", "toy"],
        "type": "object",
    }
