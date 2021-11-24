"""Utility object for running a tap and capturing sync output during testing."""

import json
import sys
import io

from collections import defaultdict
from typing import List, Type, Optional

from singer_sdk.tap_base import Tap
from singer_sdk.exceptions import MaxRecordsLimitException


class TapTestRunner(object):
    """Utility class to simplify tap testing.

    This utility class enables developers to more easily test taps against
    live integrations. Developers can leverage the data output from the sync to
    test assumptions about their tap schemas and the data output from the source
    system.

    The standard approach for testing is using the `get_standard_tap_tests`
    approach, which uses the TapTestRunner to generate a series of tests and
    run them via PyTest. In this case, no direct invocation of the runner object
    is required by the developer.

    The TapTestRunner can also be used to write custom tests against the tap output.
    The most convenient way to do this is to initialize the object as a fixture,
    then write specific tests against this.

    ```
    @pytest.fixture(scope="session")
    def test_runner():
        runner = TapTestRunner(
            tap_classTapSlack,
            tap_configconfig={},
            stream_record_limit=500
        )
        runner.run_discovery()
        runner.run_sync()

        yield runner

    def test_foo_stream_returns_500_records(test_runner):
        assert len(runner.records["foo"]) == 500
    ```
    """

    raw_messages = None
    schema_messages = []
    state_messages = []
    record_messages = []
    records = defaultdict(list)

    def __init__(
        self,
        tap_class: Type[Tap],
        tap_config: dict = {},
        tap_kwargs: dict = {"parse_env_config": True},
    ) -> None:
        """Initializes the test runner object.

        Args:
            tap_class (Type[Tap]): Tap class to be tested
            tap_config (dict): Tap configuration for testing
            tap_kwargs (dict): Default arguments to be passed to tap on create
        """
        self.tap_class = tap_class
        self.tap_config = tap_config
        self.default_tap_kwargs = tap_kwargs
        self.tap = self.create_new_tap()

    def create_new_tap(self, tap_kwargs: Optional[dict] = None) -> Type[Tap]:
        """Creates a new tap from the runner defaults.

        Args:
            tap_kwargs (dict, optional): [description]. Defaults to None.

        Returns:
            Type[Tap]: [description]
        """
        if not tap_kwargs:
            tap_kwargs = self.default_tap_kwargs
        tap = self.tap_class(config=self.tap_config, **tap_kwargs)
        return tap

    def run_sync(self, stream_record_limit: int = 100) -> None:
        """Runs a full tap sync, assigning output to the runner object.

        Args:
            stream_record_limit (int): Max number of records per stream
                to return. Defaults to 100.
        """
        stdout = self._execute_sync(stream_record_limit)
        records = self._clean_sync_output(stdout)
        self._parse_records(records)

    def run_discovery(self) -> None:
        """Run tap discovery."""
        self.tap.run_discovery()

    def _execute_sync(self, stream_record_limit: int) -> List[dict]:
        """Executes the sync and captures the records printed to stdout."""
        output_buffer = io.StringIO()
        sys.stdout = output_buffer
        self._sync_all_streams(stream_record_limit)
        sys.stdout = sys.__stdout__
        return output_buffer.getvalue()

    def _sync_all_streams(self, stream_record_limit: int) -> bool:
        """Syncs each selected stream of the tap."""
        self.tap._reset_state_progress_markers()
        self.tap._set_compatible_replication_methods()
        for stream in self.tap.streams.values():
            stream._MAX_RECORDS_LIMIT = stream_record_limit
            if not stream.selected and not stream.has_selected_descendents:
                self.tap.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue

            if stream.parent_stream_type:
                self.tap.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            try:
                stream.sync()
            except MaxRecordsLimitException:
                self.tap.logger.info(
                    "Reached max record limit (%s) for Stream %s",
                    stream_record_limit,
                    stream.name,
                )
                pass
            stream.finalize_state_progress_markers()
        self.tap.logger.info("Completed sync")
        return True

    def _clean_sync_output(self, raw_records: str) -> List[dict]:
        lines = raw_records.strip().split("\n")
        return [json.loads(ii) for ii in lines]

    def _parse_records(self, records: List[dict]) -> None:
        """Saves raw and parsed messages onto the runner object."""
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
                self.record_messages.append(record)
                self.records[stream_name].append(record["record"])
                continue
        return
