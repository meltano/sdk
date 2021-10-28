"""Pre-built test functions which can be applied to multiple taps."""

import json
import sys
import io

from collections import defaultdict
from dateutil import parser
from typing import Callable, List, Type, Any

from singer_sdk.tap_base import Tap
from singer_sdk.exceptions import MaxRecordsLimitException


def get_standard_tap_tests(tap_class: Type[Tap], config: dict = None) -> List[Callable]:
    """Return callable pytest which executes simple discovery and connection tests.

    Args:
        tap_class: TODO
        config: TODO

    Returns:
        TODO
    """

    def _test_cli_prints() -> None:
        # Initialize with basic config
        tap1: Tap = tap_class(config=config, parse_env_config=True)
        # Test CLI prints
        tap1.print_version()
        tap1.print_about()
        tap1.print_about(format="json")

    def _test_discovery() -> None:
        # Initialize with basic config
        tap1: Tap = tap_class(config=config, parse_env_config=True)
        # Test discovery
        tap1.run_discovery()
        catalog1 = tap1.catalog_dict
        # Reset and re-initialize with an input catalog
        tap1 = None  # type: ignore
        tap2: Tap = tap_class(config=config, parse_env_config=True, catalog=catalog1)
        assert tap2

    def _test_stream_connections() -> None:
        # Initialize with basic config
        tap1: Tap = tap_class(config=config, parse_env_config=True)
        tap1.run_connection_test()

    return [_test_cli_prints, _test_discovery, _test_stream_connections]


def get_standard_target_tests(
    target_class: Type[Tap],
    config: dict = None,
) -> List[Callable]:
    """Return callable pytest which executes simple discovery and connection tests.

    Args:
        target_class: TODO
        config: TODO

    Returns:
        TODO
    """
    return []


class StreamTestUtility(object):
    """
    This utility class enables developers to more easily test taps against
    live integrations. It provides some out-of-the-box tests that can be run
    against individual streams, and developers can leverage the output data for
    custom tests.

    It is intended to be used as a testing fixture to simplify data testing. For example:

    ```
    @pytest.fixture(scope="session")
    def stream_test_util():
        test_util = StreamTestUtility(MyTap, config={})
        test_util.run_sync()

        yield test_util

    def test_standard_tests_for_my_stream(test_util):
        test_util._test_stream_returns_at_least_one_record("my_stream")
        test_util._test_stream_catalog_attributes_in_records("my_stream")
        test_util._test_primary_keys("my_stream")
        test_util._test_stream_attribute_is_not_null("my_stream", "my_column")
    ```
    """

    schema_messages = []
    state_messages = []
    records = defaultdict(list)

    def __init__(
        self, tap_class: Type[Tap], config: dict = {}, stream_record_limit: int = 10, parse_env_config: bool = True
    ) -> None:
        """
        Initializes the test utility.

        Args:
            tap_class (Type[Tap]): Tap class to be tested
            config (dict, optional): Tap configuration for testing. Defaults to {}.
            stream_record_limit (int, optional): The max number of records a stream may emit before being stopped. Defaults to 10.
            parse_env_config (bool, optional): Whether to use env variables when initializing the tap. Defaults to True.
        """
        self.tap = tap_class(config=config, state=None, parse_env_config=parse_env_config)
        self.stream_record_limit = stream_record_limit

    @property
    def available_stream_tests(self):
        return [
            self._test_stream_returns_at_least_one_record,
            self._test_stream_catalog_schema_matches_records,
            self._test_stream_record_schema_matches_catalog,
            self._test_stream_primary_key
        ]

    @property
    def available_stream_attribute_tests(self):
        return [
            self._test_stream_attribute_is_not_null,
            self._test_stream_attribute_is_unique,
            self._test_stream_attribute_contains_accepted_values,
            self._test_stream_attribute_is_valid_timestamp
        ]

    def run_sync(self):
        stdout = self._exec_sync()
        records = self._clean_sync_output(stdout)
        self._parse_records(records)

    def _exec_sync(self) -> List[dict]:
        "Executes the sync and captures the records printed to stdout."
        output_buffer = io.StringIO()
        sys.stdout = output_buffer
        self._sync_all_streams()
        sys.stdout = sys.__stdout__
        return output_buffer.getvalue()

    def _sync_all_streams(self) -> bool:
        for stream in self.tap.streams.values():
            stream._MAX_RECORDS_LIMIT = self.stream_record_limit
            try:
                stream.sync()
            except MaxRecordsLimitException:
                pass
        return True

    def _clean_sync_output(self, raw_records):
        lines = raw_records.strip().split("\n")
        return [json.loads(ii) for ii in lines]

    def _parse_records(self, records: List[dict]) -> None:
        self.raw_messages = records
        for record in records:
            if record["type"] == "STATE":
                self.state_messages.append(record)
                continue
            if record["type"] == "SCHEMA":
                self.schema_messages.append(record)
                continue
            if record["type"] == "RECORD":
                stream_name = record["stream"]
                self.records[stream_name].append(record)
                continue
        return

    def _test_stream_returns_at_least_one_record(self, stream_name):
        "The full sync of the stream should have returned at least 1 record."
        record_count = len(self.records[stream_name])

        assert record_count > 0

    def _test_stream_catalog_schema_matches_records(self, stream_name):
        "The stream's first record should have a catalog identical to that defined."
        stream = self.tap.streams[stream_name]
        stream_catalog_keys = set(stream.schema["properties"].keys())
        stream_record_keys = set().union(
            *(d["record"].keys() for d in self.records[stream_name])
        )
        diff = stream_record_keys - stream_catalog_keys

        assert diff == set(), f"Fields in catalog but not in record: ({diff})"

    def _test_stream_record_schema_matches_catalog(self, stream_name):
        "The stream's first record should have a catalog identical to that defined."
        stream = self.tap.streams[stream_name]
        stream_catalog_keys = set(stream.schema["properties"].keys())
        stream_record_keys = set().union(
            *(d["record"].keys() for d in self.records[stream_name])
        )
        diff = stream_catalog_keys - stream_record_keys

        assert diff == set(), f"Fields in records but not in catalog: ({diff})"

    def _test_stream_primary_key(self, stream_name: str):
        "Test that all records for a stream's primary key are unique and non-null."
        primary_keys = self.tap.streams[stream_name].primary_keys
        records = self.records[stream_name]
        record_ids = []
        for r in self.records[stream_name]:
            id = (r[k] for k in primary_keys)
            record_ids.append(id)

        assert len(set(record_ids)) == len(records)
        assert all(all(k is not None for k in pk) for pk in record_ids)

    def _test_stream_attribute_contains_accepted_values(self, stream_name: str, attribute_name: str, accepted_values: List[Any]):
        "Test that a given attribute contains only accepted values."
        records = self.records[stream_name]

        assert all(r[attribute_name] in accepted_values for r in records)

    def _test_stream_attribute_is_unique(self, stream_name: str, attribute_name: str):
        "Test that a given attribute contains unique values, ignoring nulls."
        records = self.records[stream_name]
        values = [r[attribute_name] for r in records if r[attribute_name] is not None]

        assert len(set(values)) == len(values)

    def _test_stream_attribute_is_valid_timestamp(self, stream_name: str, attribute_name: str):
        "Test that a given attribute contains unique values, ignoring nulls."
        records = self.records[stream_name]
        values = [r[attribute_name] for r in records if r[attribute_name] is not None]

        assert all(parser.parse(v) for v in values)

    def _test_stream_attribute_is_not_null(self, stream_name: str, attribute_name: str):
        "Test that a given attribute does not contain any null values."
        records = self.records[stream_name]

        assert all(r[attribute_name] is not None for r in records)
