"""Utility object for running taps/targets, capturing sync output during testing."""
from __future__ import annotations

import io
import json
from collections import defaultdict
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, List, Optional, Type, Union

from singer_sdk.tap_base import Tap
from singer_sdk.target_base import Target


class SingerTestRunner:
    """Base Singer Test Runner."""

    raw_messages: str | None = None
    schema_messages: List[dict] = []
    record_messages: List[dict] = []
    state_messages: List[dict] = []
    records: List[dict] = defaultdict(list)

    def __init__(
        self,
        singer_class: Union[Type[Tap], Type[Target]],
        config: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the test runner object.

        Args:
            singer_class (Type[Tap] | Type[Target]): Singer class to be tested.
            config (dict): Tap/Target configuration for testing.
            kwargs (dict): Default arguments to be passed to tap/target on create.
        """
        self.singer_class = singer_class
        self.config = config or {}
        self.default_kwargs = kwargs

    def create(self, kwargs: Optional[dict] = None) -> Union[Type[Tap], Type[Target]]:
        """Create a new tap/target from the runner defaults.

        Args:
            kwargs (dict, optional): [description]. Defaults to None.

        Returns:
            An instantiated Tap or Target.
        """
        if not kwargs:
            kwargs = self.default_kwargs
        return self.singer_class(config=self.config, **kwargs)


class TapTestRunner(SingerTestRunner):
    """Utility class to simplify tap testing."""

    type = "tap"

    def __init__(
        self,
        tap_class: Type[Tap],
        config: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Tap instance.

        Args:
            tap_class: Tap class to run.
            config: Config dict to pass to Tap class.
            kwargs: Default arguments to be passed to tap on create.
        """
        super().__init__(singer_class=tap_class, config=config or {}, **kwargs)

    @property
    def tap(self) -> Type[Tap]:
        """Get new Tap instance.

        Returns:
            A configured Tap instance.
        """
        return self.create()

    def run_discovery(self) -> str:
        """Run tap discovery.

        Returns:
            The catalog as a string.
        """
        return self.tap.run_discovery()

    def run_connection_test(self) -> bool:
        """Run tap connection test.

        Returns:
            True if connection test passes, else False.
        """
        return self.tap.run_connection_test()

    def sync_all(self) -> None:
        """Run a full tap sync, assigning output to the runner object."""
        stdout, stderr = self._execute_sync()
        messages = self._clean_sync_output(stdout)
        self._parse_records(messages)

    def _clean_sync_output(self, raw_records: str) -> List[dict]:
        """Clean sync output.

        Args:
            raw_records: String containing raw messages.

        Returns:
            A list of raw messages in dict form.
        """
        lines = raw_records.strip().split("\n")
        return [json.loads(ii) for ii in lines]

    def _parse_records(self, messages: List[dict]) -> None:
        """Save raw and parsed messages onto the runner object.

        Args:
            messages: A list of messages in dict form.
        """
        self.raw_messages = messages
        for message in messages:
            if message:
                if message["type"] == "STATE":
                    self.state_messages.append(message)
                    continue
                if message["type"] == "SCHEMA":
                    self.schema_messages.append(message)
                    continue
                if message["type"] == "RECORD":
                    stream_name = message["stream"]
                    self.record_messages.append(message)
                    self.records[stream_name].append(message["record"])
                    continue
        return

    def _execute_sync(self) -> List[dict]:
        """Invoke a Tap object and return STDOUT and STDERR results in StringIO buffers.

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
        config: Optional[dict] = None,
        input_filepath: Optional[Path] = None,
        input_io: Optional[io.StringIO] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize TargetTestRunner.

        Args:
            target_class: Target Class to instantiate.
            config: Config to pass to instantiated Target.
            input_filepath: (optional) Path to a singer file containing records, to pass
                to the Target during testing.
            input_io: (optional) StringIO containing raw records to pass to the Target
                during testing.
            kwargs: Default arguments to be passed to tap/target on create.
        """
        super().__init__(singer_class=target_class, config=config or {}, **kwargs)
        self.input_filepath = input_filepath
        self.input_io = input_io
        self._input = None

    @property
    def target(self) -> Type[Target]:
        """Get new Target instance.

        Returns:
            A configured Target instance.
        """
        return self.create()

    @property
    def input(self) -> List[str]:
        """Input messages to pass to Target.

        Returns:
            A list of raw input messages in string form.
        """
        if self._input is None:
            if self.input_io:
                self._input = self.input_io.read()
            elif self.input_filepath:
                self._input = open(self.input_filepath, "r").readlines()
        return self._input

    @input.setter
    def input(self, value: List[str]) -> None:
        self._input = value

    def sync_all(self, finalize: bool = True) -> None:
        """Run a full tap sync, assigning output to the runner object.

        Args:
            finalize: True to process as the end of stream as a completion signal;
                False to keep the sink operation open for further records.
        """
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
            target: Target to sync.
            input: The input to process as if from STDIN.
            finalize: True to process as the end of stream as a completion signal;
                False to keep the sink operation open for further records.

        Returns:
            A 2-item tuple with StringIO buffers from the Target's output:
                (stdout, stderr)
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
