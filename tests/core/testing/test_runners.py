"""Tests for singer_sdk.testing.runners module."""

from __future__ import annotations

import io
import json
import typing as t
from unittest.mock import Mock, patch

import pytest

from singer_sdk import Tap, Target
from singer_sdk.testing.config import SuiteConfig
from singer_sdk.testing.runners import SingerTestRunner, TapTestRunner, TargetTestRunner

if t.TYPE_CHECKING:
    from pathlib import Path


class TestSingerTestRunner:
    """Test SingerTestRunner base class functionality."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that SingerTestRunner cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            SingerTestRunner(Mock())

    def test_subclass_must_implement_sync_all(self):
        """Test that subclasses must implement sync_all method."""

        class IncompleteRunner(SingerTestRunner):
            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteRunner(Mock())

    def test_concrete_subclass_implementation(self):
        """Test that concrete subclass can be instantiated."""

        class ConcreteRunner(SingerTestRunner):
            def sync_all(self, **kwargs):
                pass

        mock_class = Mock()
        runner = ConcreteRunner(mock_class)

        assert runner.singer_class is mock_class
        assert runner.config == {}
        assert isinstance(runner.suite_config, SuiteConfig)
        assert runner.default_kwargs == {}


class TestTapTestRunner:
    """Test TapTestRunner functionality."""

    @pytest.fixture
    def mock_tap_class(self):
        """Create a mock Tap class for testing."""
        tap_class = Mock(spec=Tap)
        tap_class.__name__ = "MockTap"
        return tap_class

    @pytest.fixture
    def runner(self, mock_tap_class):
        """Create a TapTestRunner instance."""
        return TapTestRunner(
            tap_class=mock_tap_class,
            config={"test": "config"},
        )

    def test_init(self, mock_tap_class):
        """Test TapTestRunner initialization."""
        config = {"test": "config"}
        suite_config = SuiteConfig(max_records_limit=200)
        kwargs = {"custom_kwarg": "value"}

        runner = TapTestRunner(
            tap_class=mock_tap_class,
            config=config,
            suite_config=suite_config,
            **kwargs,
        )

        assert runner.singer_class is mock_tap_class
        assert runner.config == config
        assert runner.suite_config is suite_config
        assert runner.default_kwargs == kwargs

        # Check that collections are initialized
        assert runner.raw_messages == []
        assert runner.schema_messages == []
        assert runner.record_messages == []
        assert runner.state_messages == []
        assert len(runner.records) == 0

    def test_init_with_defaults(self, mock_tap_class):
        """Test TapTestRunner initialization with defaults."""
        runner = TapTestRunner(tap_class=mock_tap_class)

        assert runner.singer_class is mock_tap_class
        assert runner.config == {}
        assert isinstance(runner.suite_config, SuiteConfig)
        assert runner.default_kwargs == {}

    def test_new_tap(self, runner, mock_tap_class):
        """Test new_tap method."""
        mock_instance = Mock(spec=Tap)
        mock_tap_class.return_value = mock_instance

        tap = runner.new_tap()

        assert tap is mock_instance
        mock_tap_class.assert_called_once_with(
            config=runner.config,
            **runner.default_kwargs,
        )

    def test_create_with_custom_kwargs(self, runner: TapTestRunner, mock_tap_class):
        """Test create method with custom kwargs."""
        mock_instance = Mock(spec=Tap)
        mock_tap_class.return_value = mock_instance

        custom_kwargs = {"parse_env_config": True}
        tap = runner.create(custom_kwargs)

        assert tap is mock_instance
        mock_tap_class.assert_called_once_with(
            config=runner.config,
            parse_env_config=True,
        )

    def test_create_with_none_kwargs(self, runner: TapTestRunner, mock_tap_class):
        """Test create method with None kwargs uses defaults."""
        mock_instance = Mock(spec=Tap)
        mock_tap_class.return_value = mock_instance

        tap = runner.create(None)

        assert tap is mock_instance
        mock_tap_class.assert_called_once_with(
            config=runner.config,
            **runner.default_kwargs,
        )

    def test_run_discovery(self, runner: TapTestRunner):
        """Test run_discovery method."""
        mock_tap = Mock(spec=Tap)
        mock_tap.run_discovery.return_value = '{"test": "catalog"}'

        with patch.object(runner, "new_tap", return_value=mock_tap):
            result = runner.run_discovery()

        assert result == '{"test": "catalog"}'
        mock_tap.run_discovery.assert_called_once()

    def test_run_connection_test(self, runner: TapTestRunner):
        """Test run_connection_test method."""
        mock_tap = Mock(spec=Tap)
        mock_tap.run_connection_test.return_value = True

        with patch.object(runner, "new_tap", return_value=mock_tap):
            result = runner.run_connection_test()

        assert result is True
        mock_tap.run_connection_test.assert_called_once()

    def test_run_sync_dry_run(self, runner: TapTestRunner):
        """Test run_sync_dry_run method."""
        mock_tap = Mock(spec=Tap)
        mock_tap.run_sync_dry_run.return_value = True

        with patch.object(runner, "new_tap", return_value=mock_tap):
            result = runner.run_sync_dry_run()

        assert result is True
        mock_tap.run_sync_dry_run.assert_called_once_with(
            dry_run_record_limit=runner.suite_config.max_records_limit,
        )

    def test_clean_sync_output(self):
        """Test _clean_sync_output static method."""
        raw_output = """{"type": "SCHEMA", "stream": "test"}
{"type": "RECORD", "stream": "test", "record": {"id": 1}}

{"type": "STATE", "value": {"test": "state"}}"""

        result = TapTestRunner._clean_sync_output(raw_output)

        expected = [
            {"type": "SCHEMA", "stream": "test"},
            {"type": "RECORD", "stream": "test", "record": {"id": 1}},
            {"type": "STATE", "value": {"test": "state"}},
        ]
        assert result == expected

    def test_clean_sync_output_empty_lines(self):
        """Test _clean_sync_output with empty lines."""
        raw_output = """{"type": "SCHEMA", "stream": "test"}

{"type": "RECORD", "stream": "test", "record": {"id": 1}}
"""

        result = TapTestRunner._clean_sync_output(raw_output)

        expected = [
            {"type": "SCHEMA", "stream": "test"},
            {"type": "RECORD", "stream": "test", "record": {"id": 1}},
        ]
        assert result == expected

    def test_parse_records(self, runner: TapTestRunner):
        """Test _parse_records method."""
        messages = [
            {"type": "SCHEMA", "stream": "users", "schema": {"type": "object"}},
            {"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "John"}},
            {"type": "RECORD", "stream": "users", "record": {"id": 2, "name": "Jane"}},
            {"type": "STATE", "value": {"users": 2}},
            {"type": "RECORD", "stream": "orders", "record": {"id": 100, "user_id": 1}},
        ]

        runner._parse_records(messages)

        assert runner.raw_messages == messages
        assert len(runner.schema_messages) == 1
        assert len(runner.record_messages) == 3
        assert len(runner.state_messages) == 1

        assert len(runner.records["users"]) == 2
        assert len(runner.records["orders"]) == 1

        assert runner.records["users"][0] == {"id": 1, "name": "John"}
        assert runner.records["users"][1] == {"id": 2, "name": "Jane"}
        assert runner.records["orders"][0] == {"id": 100, "user_id": 1}

    def test_parse_records_with_none_message(self, runner: TapTestRunner):
        """Test _parse_records method with None messages."""
        messages: list[dict] = [
            {"type": "SCHEMA", "stream": "users", "schema": {"type": "object"}},
            {},
            {"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "John"}},
        ]

        runner._parse_records(messages)

        assert runner.raw_messages == messages
        assert len(runner.schema_messages) == 1
        assert len(runner.record_messages) == 1
        assert len(runner.records["users"]) == 1

    def test_execute_sync(self, runner):
        """Test _execute_sync method."""
        with (
            patch.object(runner, "run_sync_dry_run") as mock_sync,
            patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
            patch("sys.stderr", new_callable=io.StringIO) as mock_stderr,
        ):
            mock_stdout.write("test output")
            mock_stderr.write("test error")

            _stdout, _stderr = runner._execute_sync()

        mock_sync.assert_called_once()
        # Note: The actual output capture is complex due to buffer wrapping
        # We mainly test that the method completes without error

    def test_sync_all(self, runner):
        """Test sync_all method."""
        mock_stdout = '{"type": "RECORD", "stream": "test", "record": {"id": 1}}'
        mock_stderr = ""

        with (
            patch.object(
                runner,
                "_execute_sync",
                return_value=(mock_stdout, mock_stderr),
            ),
            patch.object(runner, "_parse_records") as mock_parse,
        ):
            runner.sync_all()

        mock_parse.assert_called_once()
        # Check that the parsed messages were from cleaned output
        call_args = mock_parse.call_args[0][0]
        assert isinstance(call_args, list)
        assert len(call_args) == 1
        assert call_args[0] == {
            "type": "RECORD",
            "stream": "test",
            "record": {"id": 1},
        }


class TestTargetTestRunner:
    """Test TargetTestRunner functionality."""

    @pytest.fixture
    def mock_target_class(self):
        """Create a mock Target class for testing."""
        target_class = Mock(spec=Target)
        target_class.__name__ = "MockTarget"
        return target_class

    @pytest.fixture
    def runner(self, mock_target_class):
        """Create a TargetTestRunner instance."""
        return TargetTestRunner(
            target_class=mock_target_class,
            config={"test": "config"},
        )

    def test_init(self, mock_target_class):
        """Test TargetTestRunner initialization."""
        config = {"test": "config"}
        suite_config = SuiteConfig(max_records_limit=200)
        input_io = io.StringIO('{"type": "RECORD"}')
        kwargs = {"custom_kwarg": "value"}

        runner = TargetTestRunner(
            target_class=mock_target_class,
            config=config,
            suite_config=suite_config,
            input_io=input_io,
            **kwargs,
        )

        assert runner.singer_class is mock_target_class
        assert runner.config == config
        assert runner.suite_config is suite_config
        assert runner.input_io is input_io
        assert runner.input_filepath is None
        assert runner.default_kwargs == kwargs

    def test_init_with_filepath(self, mock_target_class, tmp_path):
        """Test TargetTestRunner initialization with input filepath."""
        input_file = tmp_path / "input.jsonl"
        input_file.write_text('{"type": "RECORD", "stream": "test"}')

        runner = TargetTestRunner(
            target_class=mock_target_class,
            input_filepath=input_file,
        )

        assert runner.input_filepath == input_file
        assert runner.input_io is None

    def test_new_target(self, runner: TargetTestRunner, mock_target_class):
        """Test new_target method."""
        mock_instance = Mock(spec=Target)
        mock_target_class.return_value = mock_instance

        target = runner.new_target()

        assert target is mock_instance
        mock_target_class.assert_called_once_with(
            config=runner.config,
            **runner.default_kwargs,
        )

    def test_target_input_with_io(self, runner: TargetTestRunner, mock_target_class):
        """Test target_input property with StringIO input."""
        input_io = io.StringIO('{"type": "RECORD"}')
        runner = TargetTestRunner(
            target_class=mock_target_class,
            input_io=input_io,
        )

        assert runner.target_input is input_io

    def test_target_input_with_filepath(
        self, runner: TargetTestRunner, mock_target_class, tmp_path
    ):
        """Test target_input property with filepath input."""
        input_file = tmp_path / "input.jsonl"
        input_file.write_text('{"type": "RECORD", "stream": "test"}')

        runner = TargetTestRunner(
            target_class=mock_target_class,
            input_filepath=input_file,
        )

        # This will open the file
        target_input = runner.target_input
        content = target_input.read()
        assert content == '{"type": "RECORD", "stream": "test"}'
        target_input.close()

    def test_target_input_setter(self, runner: TargetTestRunner):
        """Test target_input property setter."""
        new_input = io.StringIO('{"type": "NEW_RECORD"}')
        runner.target_input = new_input

        assert runner._input is new_input
        assert runner.target_input is new_input

    def test_target_input_none_sources(
        self, runner: TargetTestRunner, mock_target_class
    ):
        """Test target_input property with no input sources."""
        runner = TargetTestRunner(target_class=mock_target_class)

        # This should not raise an error but will return None
        # The property will cast it, so we test the private attribute
        assert runner._input is None

    def test_execute_sync(self, runner: TargetTestRunner):
        """Test _execute_sync method."""
        mock_target = Mock(spec=Target)
        input_io = io.StringIO('{"type": "RECORD"}')

        stdout_buf, stderr_buf = runner._execute_sync(
            target=mock_target,
            target_input=input_io,
            finalize=True,
        )

        mock_target.process_lines.assert_called_once_with(input_io)
        mock_target.process_endofpipe.assert_called_once()

        # Check that buffers were created and positioned
        assert stdout_buf.tell() == 0
        assert stderr_buf.tell() == 0

    def test_execute_sync_no_finalize(self, runner: TargetTestRunner):
        """Test _execute_sync method without finalization."""
        mock_target = Mock(spec=Target)
        input_io = io.StringIO('{"type": "RECORD"}')

        _stdout_buf, _stderr_buf = runner._execute_sync(
            target=mock_target,
            target_input=input_io,
            finalize=False,
        )

        mock_target.process_lines.assert_called_once_with(input_io)
        mock_target.process_endofpipe.assert_not_called()

    def test_execute_sync_no_input(self, runner: TargetTestRunner):
        """Test _execute_sync method with no input."""
        mock_target = Mock(spec=Target)

        _stdout_buf, _stderr_buf = runner._execute_sync(
            target=mock_target,
            target_input=None,
            finalize=True,
        )

        mock_target.process_lines.assert_not_called()
        mock_target.process_endofpipe.assert_called_once()

    def test_sync_all(self, runner: TargetTestRunner):
        """Test sync_all method."""
        mock_target = Mock(spec=Target)
        mock_stdout = io.StringIO('{"type": "STATE"}')
        mock_stderr = io.StringIO("")

        # Set up input directly instead of patching the property
        runner.input_io = io.StringIO('{"type": "RECORD"}')

        with (
            patch.object(runner, "new_target", return_value=mock_target),
            patch.object(
                runner,
                "_execute_sync",
                return_value=(mock_stdout, mock_stderr),
            ),
        ):
            runner.sync_all(finalize=True)

        assert runner.stdout is not None
        assert runner.stderr is not None
        assert isinstance(runner.state_messages, list)
        assert len(runner.state_messages) == 1
        assert runner.state_messages[0] == {"type": "STATE"}

    def test_sync_all_no_finalize(self, runner: TargetTestRunner):
        """Test sync_all method without finalization."""
        mock_target = Mock(spec=Target)
        mock_stdout = io.StringIO("")
        mock_stderr = io.StringIO("")

        # Set up input directly instead of patching the property
        runner.input_io = io.StringIO('{"type": "RECORD"}')

        with (
            patch.object(runner, "new_target", return_value=mock_target),
            patch.object(
                runner,
                "_execute_sync",
                return_value=(mock_stdout, mock_stderr),
            ),
        ):
            runner.sync_all(finalize=False)

        # Should still complete successfully
        assert runner.stdout is not None
        assert runner.stderr is not None


class TestRunnerIntegration:
    """Integration tests for runners with more realistic scenarios."""

    def test_tap_runner_full_workflow(self):
        """Test a complete tap runner workflow."""
        # Create a minimal mock tap that behaves more realistically
        mock_tap_class = Mock(spec=Tap)
        mock_tap_instance = Mock(spec=Tap)
        mock_tap_class.return_value = mock_tap_instance

        # Configure realistic return values
        mock_tap_instance.run_discovery.return_value = '{"streams": []}'
        mock_tap_instance.run_connection_test.return_value = True
        mock_tap_instance.run_sync_dry_run.return_value = True

        config = {"api_key": "test_key"}
        suite_config = SuiteConfig(max_records_limit=100)

        runner = TapTestRunner(
            tap_class=mock_tap_class,
            config=config,
            suite_config=suite_config,
        )

        # Test discovery
        catalog = runner.run_discovery()
        assert catalog == '{"streams": []}'

        # Test connection
        connection_ok = runner.run_connection_test()
        assert connection_ok is True

        # Test dry run
        dry_run_ok = runner.run_sync_dry_run()
        assert dry_run_ok is True

        # Verify tap was created with correct config
        mock_tap_class.assert_called_with(config=config)

    def test_target_runner_with_real_input_data(self, tmp_path: Path):
        """Test target runner with realistic input data."""
        # Create test input file
        input_file = tmp_path / "test_input.jsonl"
        test_data = [
            {"type": "SCHEMA", "stream": "users", "schema": {"type": "object"}},
            {"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "John"}},
            {"type": "RECORD", "stream": "users", "record": {"id": 2, "name": "Jane"}},
            {"type": "STATE", "value": {"users": 2}},
        ]

        input_file.write_text("\n".join(json.dumps(record) for record in test_data))

        mock_target_class = Mock(spec=Target)
        mock_target_instance = Mock(spec=Target)
        mock_target_class.return_value = mock_target_instance

        runner = TargetTestRunner(
            target_class=mock_target_class,
            input_filepath=input_file,
            config={"database_url": "test://db"},
        )

        # Mock the sync execution to avoid actual target processing
        with patch.object(runner, "_execute_sync") as mock_execute:
            mock_stdout = io.StringIO('{"type": "STATE", "value": {"users": 2}}')
            mock_stderr = io.StringIO("")
            mock_execute.return_value = (mock_stdout, mock_stderr)

            runner.sync_all()

        # Verify the target was created and sync was executed
        mock_target_class.assert_called_once_with(
            config={"database_url": "test://db"},
        )
        mock_execute.assert_called_once()
