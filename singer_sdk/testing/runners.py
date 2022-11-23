"""Utility object for running taps/targets, capturing sync output during testing."""
from __future__ import annotations

import io
import json
from collections import defaultdict
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import List, Optional, Type

from singer_sdk.exceptions import MaxRecordsLimitException
from singer_sdk.tap_base import Tap
from singer_sdk.target_base import Target


class SingerTestRunner:
    raw_messages = None
    schema_messages = []
    record_messages = []
    state_messages = []
    records = defaultdict(list)

    def __init__(
        self,
        singer_class: Type[Tap] | Type[Target],
        config: dict = {},
        **kwargs,
    ) -> None:
        """Initializes the test runner object.

        Args:
            singer_class (Type[Tap] | Type[Target]): Singer class to be tested.
            config (dict): Tap/Target configuration for testing.
            kwargs (dict): Default arguments to be passed to tap/target on create.
        """
        self.singer_class = singer_class
        self.config = config or {}
        self.default_kwargs = kwargs

    def create(self, kwargs: Optional[dict] = None) -> Type[Tap] | Type[Target]:
        """Creates a new tap/target from the runner defaults.

        Args:
            kwargs (dict, optional): [description]. Defaults to None.

        Returns:
            An instantiated Tap or Target.
        """
        if not kwargs:
            kwargs = self.default_kwargs
        return self.singer_class(config=self.config, **kwargs)


class TapTestRunner(SingerTestRunner):
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

    type = "tap"

    def __init__(
        self,
        tap_class: Type[Tap],
        config: dict = {},
        **kwargs,
    ) -> None:
        super().__init__(singer_class=tap_class, config=config or {}, **kwargs)

    @property
    def tap(self):
        return self.create()

    def run_discovery(self) -> str:
        """Run tap discovery."""
        return self.tap.run_discovery()

    def run_connection_test(self) -> bool:
        """Run tap connection test."""
        return self.tap.run_connection_test()

    def sync_all(self) -> None:
        """Runs a full tap sync, assigning output to the runner object."""
        stdout, stderr = self._execute_sync()
        records = self._clean_sync_output(stdout)
        self._parse_records(records)

    def _clean_sync_output(self, raw_records: str) -> List[dict]:
        lines = raw_records.strip().split("\n")
        return [json.loads(ii) for ii in lines]

    def _parse_records(self, records: List[dict]) -> None:
        """Saves raw and parsed messages onto the runner object."""
        self.raw_messages = records
        for record in records:
            if record:
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

    def _execute_sync(self) -> List[dict]:
        """Invokes a Tap object and return STDOUT and STDERR results in StringIO buffers.

        Returns:
            A 2-item tuple with StringIO buffers from the Tap's output: (stdout, stderr)
        """
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            self.tap.sync_all()
        stdout_buf.seek(0)
        stderr_buf.seek(0)
        return stdout_buf.read(), stderr_buf.read()


class TargetTestRunner(SingerTestRunner):
    """Utility class to simplify target testing."""

    type = "target"

    def __init__(
        self,
        target_class: Type[Target],
        config: dict = {},
        input_filepath: Path = None,
        input_io: io.StringIO | None = None,
        **kwargs,
    ) -> None:
        super().__init__(singer_class=target_class, config=config or {}, **kwargs)
        self.input_filepath = input_filepath
        self.input_io = input_io
        self._input = None

    @property
    def target(self):
        return self.create()

    @property
    def input(self):
        if self._input is None:
            if self.input_io:
                self._input = self.input_io.read()
            elif self.input_filepath:
                self._input = open(self.input_filepath, "r").readlines()
        return self._input

    @input.setter
    def input(self, value):
        self._input = value

    def sync_all(self, finalize: bool = True) -> None:
        """Runs a full tap sync, assigning output to the runner object."""
        target = self.create()
        stdout, stderr = self._execute_sync(
            target=target, input=self.input, finalize=finalize
        )
        self.stdout, self.stderr = (stdout.read(), stderr.read())
        self.state_messages.extend(self.stdout.split("\n"))

    def _execute_sync(
        self, target: Target, input: io.StringIO | None, finalize: bool = True
    ) -> tuple[io.StringIO, io.StringIO]:
        """Invoke the target with the provided input.

        Args:
            input: The input to process as if from STDIN.
            finalize: True to process as the end of stream as a completion signal; False to
                keep the sink operation open for further records.

        Returns:
            A 2-item tuple with StringIO buffers from the Target's output: (stdout, stderr)
        """
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()

        with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            if input is not None:
                target._process_lines(input)
            if finalize:
                target._process_endofpipe()

        stdout_buf.seek(0)
        stderr_buf.seek(0)
        return stdout_buf, stderr_buf
