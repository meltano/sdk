"""Pre-built test functions which can be applied to multiple taps."""

import json
import sys
import io

from collections import defaultdict
from typing import List, Type, Any

from singer_sdk.tap_base import Tap
from singer_sdk.exceptions import MaxRecordsLimitException
from singer_sdk.testing.templates import TapTests, StreamTests, AttributeTests


class TapTestRunner(object):
    """
    This utility class enables developers to more easily test taps against
    live integrations. It provides some out-of-the-box tests that can be run
    against individual streams, and developers can leverage the output data for
    custom tests.

    It is intended to be used as a testing fixture to simplify data testing. For example:

    ```
    test_runner = TapTestRunner(TapSlack, SAMPLE_CONFIG, stream_record_limit=500)
    test_runner.run_discovery()
    test_runner.run_sync()

    pytest_params = test_runner.generate_built_in_tests()

    @pytest.fixture(scope="session")
    def test_util():
        yield test_runner

    @pytest.mark.parametrize("test_config", **pytest_params)
    def test_builtin_tap_tests(test_util, test_config):
        test_name, params = test_config
        test_func = test_util.available_tests[test_name]
        test_func(**params)
    ```
    """

    schema_messages = []
    state_messages = []
    record_messages = []
    records = defaultdict(list)

    def __init__(
        self,
        tap_class: Type[Tap],
        tap_config: dict = {},
        tap_creation_args: dict = {},
        stream_record_limit: int = 10,
    ) -> None:
        """
        Initializes the test utility.

        Args:
            tap_class (Type[Tap]): Tap class to be tested
            config (dict, optional): Tap configuration for testing. Defaults to {}.
            stream_record_limit (int, optional): The max number of records a stream may emit before being stopped. Defaults to 10.
            parse_env_config (bool, optional): Whether to use env variables when initializing the tap. Defaults to True.
        """
        self.tap_config = tap_config
        self.tap_class = tap_class
        self.tap = self.create_new_tap(**tap_creation_args)
        self.stream_record_limit = stream_record_limit

    def create_new_tap(self, **kwargs):
        tap = self.tap_class(config=self.tap_config, **kwargs)
        return tap

    def run_sync(self):
        stdout = self._execute_sync()
        records = self._clean_sync_output(stdout)
        self._parse_records(records)

    def run_discovery(self):
        self.tap.run_discovery()

    def generate_pytest_tests(self):
        tap_tests = self._generate_tap_tests()
        schema_tests = self._generate_schema_tests()
        attribute_tests = self._generate_attribute_tests()
        test_manifest = tap_tests + schema_tests + attribute_tests
        test_ids = [initialized_test.id for initialized_test in test_manifest]
        return {"argvalues": test_manifest, "ids": test_ids}

    def _execute_sync(self) -> List[dict]:
        "Executes the sync and captures the records printed to stdout."
        output_buffer = io.StringIO()
        sys.stdout = output_buffer
        self._sync_all_streams()
        sys.stdout = sys.__stdout__
        return output_buffer.getvalue()

    def _sync_all_streams(self) -> bool:
        """
        Rewrites the
        """
        self.tap._reset_state_progress_markers()
        self.tap._set_compatible_replication_methods()
        for stream in self.tap.streams.values():
            stream._MAX_RECORDS_LIMIT = self.stream_record_limit
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
                pass
            stream.finalize_state_progress_markers()
        self.tap.logger.info("Completed sync")
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
                self.record_messages.append(record)
                self.records[stream_name].append(record["record"])
                continue
        return

    def _generate_tap_tests(self):
        manifest = []
        params = dict(
            tap_class=self.tap_class,
            tap_config=self.tap_config,
            test_runner=self,
        )
        for test_name in self.tap.default_tests:
            test_class = TapTests[test_name].value
            initialized_test = test_class(**params)
            manifest.append(initialized_test)
        return manifest

    def _generate_schema_tests(self):
        manifest = []
        for stream in self.tap.streams.values():
            params = dict(
                test_runner=self,
                tap_class=self.tap_class,
                tap_init=self.tap,
                tap_config=self.tap_config,
                stream_name=stream.name,
            )
            for test_name in stream.default_tests:
                test_class = StreamTests[test_name].value
                initialized_test = test_class(**params)
                manifest.append(initialized_test)
        return manifest

    def _generate_attribute_tests(self):
        manifest = []
        for stream in self.tap.streams.values():
            schema = stream.schema
            for k, v in schema["properties"].items():
                params = dict(
                    test_runner=self, stream_name=stream.name, attribute_name=k
                )
                if v.get("required"):
                    test_class = AttributeTests.is_unique.value
                    manifest.append(test_class(**params))
                if v.get("format") == "date-time":
                    test_class = AttributeTests.is_datetime.value
                    manifest.append(test_class(**params))
                if not "null" in v.get("type", []):
                    test_class = AttributeTests.not_null.value
                    manifest.append(test_class(**params))
                if "boolean" in v.get("type", []):
                    test_class = AttributeTests.is_boolean.value
                    manifest.append(test_class(**params))
                if "integer" in v.get("type", []):
                    test_class = AttributeTests.is_integer.value
                    manifest.append(test_class(**params))
                if "object" in v.get("type", []):
                    test_class = AttributeTests.is_object.value
                    manifest.append(test_class(**params))
        return manifest
