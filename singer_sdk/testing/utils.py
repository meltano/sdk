"""Utility functions for building test sets for taps and targets."""

import warnings

from typing import Callable, List, Type

from singer_sdk.tap_base import Tap
from singer_sdk.target_base import Target
from singer_sdk.testing.templates import TapTests, StreamTests, AttributeTests
from singer_sdk.testing.runner import TapTestRunner


def get_standard_tap_tests(tap_class: Type[Tap], config: dict = None) -> List[Callable]:
    """Return callable pytest which executes simple discovery and connection tests.

    Args:
        tap_class: Class of Tap to be tested against.
        config: Configuration to pass to test tap(s).

    Returns:
        List of test functions that can be called directly.
    """
    warnings.warn(
        DeprecationWarning(
            "`get_standard_tap_tests` has been deprecated. "
            "Please use the `get_standard_tap_pytest_parameters` workflow instead."
        )
    )

    runner = TapTestRunner(tap_class, config)
    test_list = get_tap_tests(runner)
    return [t.run_test for t in test_list]


def get_standard_tap_pytest_parameters(
    tap_class: Type[Tap],
    tap_config: dict = {},
    tap_kwargs: dict = {},
    include_tap_tests: bool = True,
    include_stream_tests: bool = False,
    include_attribute_tests: bool = False,
    stream_record_limit: int = 50,
) -> dict:
    """Generates tap tests based on standard rules.

    Args:
        tap_class (Type[Tap]): Tap class.
        tap_config (dict): Configuration for tap. Defaults to None.
        tap_kwargs (dict): Default Keyword arguments to be passed during
            tap creation. Defaults to {}.
        include_tap_tests (bool): Generate tap-level tests.
        include_stream_tests (bool): Generate schema-level tests.
        include_attribute_tests (bool): Generate attribute-level tests.
        stream_record_limit (int): Max number of records to sync per stream.

    Returns:
        dict: a dictionary containing test classes that can be passed
            to `pytest.parametrize` to run built-in tap tests.
    """
    runner = TapTestRunner(tap_class, tap_config, tap_kwargs)
    runner.run_discovery()
    runner.run_sync(stream_record_limit=stream_record_limit)

    test_list = []
    if include_tap_tests:
        test_list.extend(get_tap_tests(runner))
    if include_stream_tests:
        test_list.extend(get_tap_schema_tests(runner))
    if include_attribute_tests:
        test_list.extend(get_tap_attribute_tests(runner))

    test_ids = [initialized_test.id for initialized_test in test_list]
    return {"argvalues": test_list, "ids": test_ids}


def get_standard_target_tests(
    target_class: Type[Target],
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


def get_tap_tests(
    test_runner: Type[TapTestRunner],
    selected_test_names: List[str] = ["cli_prints", "discovery", "stream_connection"],
) -> List:
    """
    [summary]

    Args:
        test_runner (Type[TapTestRunner]): [description]
        selected_test_names (List[str]): List of test names to create.
            Defaults to ["cli_prints", "discovery", "stream_connection"].

    Returns:
        List: List of initialized test objects.
    """
    test_list = []
    test_params = dict(
        tap_class=test_runner.tap_class,
        tap_config=test_runner.tap_config,
    )
    for test_name in selected_test_names:
        test_class = TapTests[test_name].value
        initialized_test = test_class(**test_params)
        test_list.append(initialized_test)
    return test_list


def get_tap_schema_tests(
    test_runner: Type[TapTestRunner],
    selected_test_names: List[str] = [
        "record_schema_matches_catalog",
        "returns_records",
        "primary_keys",
    ],
) -> List:
    """Returns an array of schema-level test objects."""
    test_list = []
    for stream in test_runner.tap.streams.values():
        test_params = dict(
            stream=stream,
            stream_name=stream.name,
            stream_records=test_runner.records[stream.name],
            tap_class=test_runner.tap_class,
            tap_init=test_runner.tap,
            tap_config=test_runner.tap_config,
        )
        for test_name in selected_test_names:
            test_class = StreamTests[test_name].value
            initialized_test = test_class(**test_params)
            test_list.append(initialized_test)
    return test_list


def get_tap_attribute_tests(
    test_runner: Type[TapTestRunner],
    selected_test_names: List[str] = [
        "is_unique",
        "is_datetime",
        "not_null",
        "is_boolean",
        "is_integer",
        "is_object",
    ],
) -> List:
    """Returns an array of attribute-level test objects."""
    test_list = []
    for stream in test_runner.tap.streams.values():
        schema = stream.schema
        for k, v in schema["properties"].items():
            test_params = dict(
                attribute_name=k,
                stream=stream,
                stream_name=stream.name,
                stream_records=test_runner.records[stream.name],
            )
            if v.get("required") and "is_unique" in selected_test_names:
                test_class = AttributeTests.is_unique.value
                test_list.append(test_class(**test_params))
            if v.get("format") == "date-time" and "is_datetime" in selected_test_names:
                test_class = AttributeTests.is_datetime.value
                test_list.append(test_class(**test_params))
            if "null" not in v.get("type", []) and "not_null" in selected_test_names:
                test_class = AttributeTests.not_null.value
                test_list.append(test_class(**test_params))
            if "boolean" in v.get("type", []) and "is_boolean" in selected_test_names:
                test_class = AttributeTests.is_boolean.value
                test_list.append(test_class(**test_params))
            if "integer" in v.get("type", []) and "is_integer" in selected_test_names:
                test_class = AttributeTests.is_integer.value
                test_list.append(test_class(**test_params))
            if "object" in v.get("type", []) and "is_object" in selected_test_names:
                test_class = AttributeTests.is_object.value
                test_list.append(test_class(**test_params))
    return test_list
