"""Pre-built test functions which can be applied to multiple taps."""

import json
import sys
import io

from pytest import CaptureFixture
from typing import Callable, List, Type

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


class TapIntegrationTestingUtility(object):
    """
    This utility class enables developers to more easily test taps against
    live integrations. It provides some out-of-the-box tests that can be run
    against individual streams, and developers can leverage the output data for
    custom tests.
    """

    def __init__(
        self, tap_class: Type[Tap], config: dict = {}, stream_record_limit: int = 10
    ) -> None:
        self.tap = tap_class(config=config)
        self.stream_record_limit = stream_record_limit
        self.run_sync()

    @property
    def available_tests(self):
        return [
            self._test_stream_returns_at_least_one_record,
            self._test_stream_catalog_attributes_in_records,
            self._test_stream_record_attributes_in_catalog,
        ]

    def run_sync(self):
        stdout = self._capture_sync_output()
        records = self._clean_sync_output(stdout)
        self._parse_records(records)

    def _capture_sync_output(self) -> List[dict]:
        """
        Run the tap sync and capture the records printed to stdout, either through
        a PyTest fixture (capsys) or through the standard `sys` utility.
        """
        output_buffer = io.StringIO()
        sys.stdout = output_buffer
        self.run_sync_all()
        sys.stdout = sys.__stdout__
        return output_buffer.getvalue()

    def _run_sync_all(self) -> bool:
        """Run connection test.

        Returns:
            True if the sync succeeded.
        """
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
        self.state_messages = [r for r in records if r["type"] == "STATE"]
        self.schema_messages = [r for r in records if r["type"] == "SCHEMA"]

        record_dict = {}
        for stream in self.tap.streams:
            record_dict[stream] = [
                r for r in records if r["type"] == "RECORD" and r["stream"] == stream
            ]
        self.records = record_dict
        return

    def _test_stream_returns_at_least_one_record(self, stream_name):
        "The full sync of the stream should have returned at least 1 record."
        record_count = len(self.records[stream_name])

        assert record_count > 0

    def _test_stream_catalog_attributes_in_records(self, stream_name):
        "The stream's first record should have a catalog identical to that defined."
        stream = self.tap.streams[stream_name]
        stream_catalog_keys = set(stream.schema["properties"].keys())
        stream_record_keys = set().union(
            *(d["record"].keys() for d in self.records[stream_name])
        )
        diff = stream_record_keys - stream_catalog_keys

        assert diff == set(), f"Fields in catalog but not in record: ({diff})"

    def _test_stream_record_attributes_in_catalog(self, stream_name):
        "The stream's first record should have a catalog identical to that defined."
        stream = self.tap.streams[stream_name]
        stream_catalog_keys = set(stream.schema["properties"].keys())
        stream_record_keys = set().union(
            *(d["record"].keys() for d in self.records[stream_name])
        )
        diff = stream_catalog_keys - stream_record_keys

        assert diff == set(), f"Fields in records but not in catalog: ({diff})"
