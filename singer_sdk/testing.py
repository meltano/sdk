"""Pre-built test functions which can be applied to multiple taps."""

import io
from contextlib import redirect_stderr, redirect_stdout

from typing import Callable, List, Optional, Tuple, Type

from singer_sdk.tap_base import Tap
from singer_sdk.target_base import Target


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


def tap_sync_test(tap: Tap) -> Tuple[io.StringIO, io.StringIO]:
    """Invokes a Tap object and return STDOUT and STDERR results in StringIO buffers.

    Args:
        tap: Any Tap object.

    Returns:
        A 2-item tuple with StringIO buffers from the Tap's output: (stdout, stderr)
    """
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()
    with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
        tap.sync_all()
    stdout_buf.seek(0)
    stderr_buf.seek(0)
    return stdout_buf, stderr_buf


def target_sync_test(
    target: Target, input: Optional[io.StringIO], finalize: bool = True
) -> Tuple[io.StringIO, io.StringIO]:
    """Invoke the target with the provided input.

    Args:
        target: Any Target object.
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


def tap_to_target_sync_test(
    tap: Tap, target: Target
) -> Tuple[io.StringIO, io.StringIO, io.StringIO, io.StringIO]:
    """Test and end-to-end sink from the tap to the target.

    Note: This method buffers all output from the tap in memory and should not be
    used with larger datasets. Also note that the target will physically write out the
    data. Cleanup afterwards should be handled by the caller, if necessary.

    Args:
        tap: Any Tap object.
        target: Any Target object.

    Returns:
        A 4-item tuple with the StringIO buffers:
        (tap_stdout, tap_stderr, target_stdout, target_stderr)
    """
    tap_stdout, tap_stderr = tap_sync_test(tap)
    target_stdout, target_stderr = target_sync_test(target, tap_stdout, finalize=True)

    return tap_stdout, tap_stderr, target_stdout, target_stderr
