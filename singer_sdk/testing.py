"""Pre-built test functions which can be applied to multiple taps."""

import io
from contextlib import redirect_stderr, redirect_stdout
from typing import Callable, List, Optional, Tuple, Type, cast

import singer_sdk._singerlib as singer
from singer_sdk.mapper_base import InlineMapper
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
        catalog1 = _get_tap_catalog(tap_class, config or {})
        # Reset and re-initialize with an input catalog
        tap2: Tap = tap_class(config=config, parse_env_config=True, catalog=catalog1)
        assert tap2

    def _test_stream_connections() -> None:
        # Initialize with basic config
        tap1: Tap = tap_class(config=config, parse_env_config=True)
        tap1.run_connection_test()

    def _test_pkeys_in_schema() -> None:
        """Verify that primary keys are actually in the stream's schema."""
        tap = tap_class(config=config, parse_env_config=True)
        for name, stream in tap.streams.items():
            pkeys = stream.primary_keys or []
            schema_props = set(stream.schema["properties"].keys())
            for pkey in pkeys:
                error_message = (
                    f"Coding error in stream '{name}': "
                    f"primary_key '{pkey}' is missing in schema"
                )
                assert pkey in schema_props, error_message

    def _test_state_partitioning_keys_in_schema() -> None:
        """Verify that state partitioning keys are actually in the stream's schema."""
        tap = tap_class(config=config, parse_env_config=True)
        for name, stream in tap.streams.items():
            sp_keys = stream.state_partitioning_keys or []
            schema_props = set(stream.schema["properties"].keys())
            for sp_key in sp_keys:
                assert sp_key in schema_props, (
                    f"Coding error in stream '{name}': state_partitioning_key "
                    f"'{sp_key}' is missing in schema"
                )

    def _test_replication_keys_in_schema() -> None:
        """Verify that the replication key is actually in the stream's schema."""
        tap = tap_class(config=config, parse_env_config=True)
        for name, stream in tap.streams.items():
            rep_key = stream.replication_key
            if rep_key is None:
                continue
            schema_props = set(stream.schema["properties"].keys())
            assert rep_key in schema_props, (
                f"Coding error in stream '{name}': replication_key "
                f"'{rep_key}' is missing in schema"
            )

    return [
        _test_cli_prints,
        _test_discovery,
        _test_stream_connections,
        _test_pkeys_in_schema,
        _test_state_partitioning_keys_in_schema,
        _test_replication_keys_in_schema,
    ]


def get_standard_target_tests(
    target_class: Type[Target],
    config: dict = None,
) -> List[Callable]:
    """Return callable pytest which executes simple discovery and connection tests.

    Args:
        target_class: The target class to test.
        config: A config dictionary for the tests.

    Returns:
        A list of callable tests.
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


def _get_tap_catalog(
    tap_class: Type[Tap], config: dict, select_all: bool = False
) -> dict:
    """Return a catalog dict by running discovery.

    Args:
        tap_class: the tap class to create.
        config: the config dict.
        select_all: True to automatically select all streams in the catalog.

    Returns:
        Catalog dict created by discovery.
    """
    # Initialize with basic config
    tap: Tap = tap_class(config=config, parse_env_config=True)
    # Test discovery
    tap.run_discovery()
    catalog_dict = tap.catalog_dict
    if select_all:
        return _select_all(catalog_dict)

    return catalog_dict


def _select_all(catalog_dict: dict) -> dict:
    """Return the catalog dict with all streams selected.

    Args:
        catalog_dict (dict): [description]

    Returns:
        dict: [description]
    """
    catalog = singer.Catalog.from_dict(catalog_dict)
    for catalog_entry in catalog.streams:
        catalog_entry.metadata.root.selected = True

    return cast(dict, catalog.to_dict())


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

    # Reset the tap's stdout buffer before returning
    tap_stdout.seek(0)

    return tap_stdout, tap_stderr, target_stdout, target_stderr


def sync_end_to_end(tap: Tap, target: Target, *mappers: InlineMapper) -> None:
    """Test and end-to-end sink from the tap to the target.

    Args:
        tap: Singer tap.
        target: Singer target.
        mappers: Zero or more inline mapper to apply in between the tap and target, in
            order.
    """
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()

    buf.seek(0)
    mapper_output = buf

    for mapper in mappers:
        buf = io.StringIO()
        with redirect_stdout(buf):
            mapper.listen(mapper_output)

        buf.seek(0)
        mapper_output = buf

    target.listen(mapper_output)
