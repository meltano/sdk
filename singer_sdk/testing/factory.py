"""Test Class Factory."""

from __future__ import annotations

import io
from pathlib import Path, PurePath
from typing import Any, cast

import pytest

from singer_sdk import Tap, Target
from singer_sdk._singerlib.catalog import Catalog

from .config import SuiteConfig
from .runners import TapTestRunner, TargetTestRunner
from .suites import (
    tap_stream_attribute_tests,
    tap_stream_tests,
    tap_tests,
    target_tests,
)


def get_test_runner(
    tap_class: Tap | None = None,
    tap_kwargs: dict = {},
    target_class: Target | None = None,
    target_kwargs: dict = {},
    target_runner_kwargs: dict = {},
    suite_config: SuiteConfig | None = None,
):
    assert (
        tap_class or target_class
    ), "get_test_runner accepts either a `tap_class` or `target_class` argument."
    if tap_class:
        return TapTestRunner(
            tap_class=tap_class, tap_kwargs=tap_kwargs, suite_config=suite_config
        )
    elif target_class:
        return TargetTestRunner(
            target_class=target_class,
            target_kwargs=target_kwargs,
            target_runner_kwargs=target_runner_kwargs,
            suite_config=suite_config,
        )


def get_test_class(
    test_suites: list,
    tap_class: Tap | None = None,
    tap_kwargs: dict = {},
    target_class: Target | None = None,
    target_kwargs: dict | None = None,
    suite_config: SuiteConfig | None = None,
) -> object:
    """Construct a valid pytest test class from given suites.

    Args:
        test_runner: A Tap or Target test runner instance.
        test_suites: A list of Test Suits to apply.
        suite_config: SuiteConfig instance to pass to tests.

    Returns:
        A test class usable by pytest.
    """
    suite_config = suite_config or SuiteConfig()
    pytest_discovery_runner = get_test_runner(
        tap_class=tap_class,
        tap_kwargs=tap_kwargs,
        target_class=target_class,
        target_kwargs=target_kwargs,
        suite_config=suite_config,
    )

    class BaseTestClass:
        """Base test class."""

        params: dict = {}
        param_ids: dict = {}

        @pytest.fixture
        def config(self) -> SuiteConfig:
            return suite_config or SuiteConfig()

        @pytest.fixture
        def resource(self) -> Any:  # noqa: ANN401
            yield

        @pytest.fixture(scope="class")
        def runner(self) -> TapTestRunner | TargetTestRunner:
            # instantiate runner class
            test_runner = get_test_runner(
                tap_class=tap_class,
                tap_kwargs=tap_kwargs,
                target_class=target_class,
                target_kwargs=target_kwargs,
                suite_config=suite_config,
            )
            # populate runner class with cached records for use in tests
            test_runner.sync_all()
            return test_runner

    for suite in test_suites:
        # make sure given runner is of type TapTestRunner
        expected_runner_class = (  # type: ignore[valid-type]
            TapTestRunner
            if suite.kind in {"tap", "tap_stream", "tap_stream_attribute"}
            else TargetTestRunner
        )
        assert isinstance(pytest_discovery_runner, expected_runner_class), (
            f"Test suite of kind {suite.kind} passed, "
            f"but test runner if of type {type(pytest_discovery_runner)}."
        )
        pytest_discovery_runner = cast(
            expected_runner_class, pytest_discovery_runner  # type: ignore[valid-type]
        )

        if suite.kind in {"tap", "target"}:
            for TestClass in suite.tests:
                test = TestClass()
                test_name = f"test_{suite.kind}_{test.name}"
                setattr(BaseTestClass, f"test_{suite.kind}_{test.name}", test.run)

        if suite.kind in {"tap_stream", "tap_stream_attribute"}:
            streams = list(pytest_discovery_runner.tap.streams.values())

            if suite.kind == "tap_stream":
                params = [
                    {
                        "stream": stream,
                    }
                    for stream in streams
                ]
                param_ids = [stream.name for stream in streams]

                for TestClass in suite.tests:
                    test = TestClass()
                    test_name = f"test_{suite.kind}_{test.name}"
                    setattr(
                        BaseTestClass,
                        test_name,
                        test.run,
                    )
                    BaseTestClass.params[test_name] = params
                    BaseTestClass.param_ids[test_name] = param_ids

            if suite.kind == "tap_stream_attribute":
                for TestClass in suite.tests:
                    test = TestClass()
                    test_name = f"test_{suite.kind}_{test.name}"
                    test_params = []
                    test_ids = []
                    for stream in streams:
                        test_params.extend(
                            [
                                {
                                    "stream": stream,
                                    "attribute_name": property_name,
                                }
                                for property_name, property_schema in stream.schema[
                                    "properties"
                                ].items()
                                if TestClass.evaluate(
                                    stream=stream,
                                    property_name=property_name,
                                    property_schema=property_schema,
                                )
                            ]
                        )
                        test_ids.extend(
                            [
                                f"{stream.name}.{property_name}"
                                for property_name, property_schema in stream.schema[
                                    "properties"
                                ].items()
                                if TestClass.evaluate(
                                    stream=stream,
                                    property_name=property_name,
                                    property_schema=property_schema,
                                )
                            ]
                        )

                    if test_params:
                        setattr(
                            BaseTestClass,
                            test_name,
                            test.run,
                        )
                        BaseTestClass.params[test_name] = test_params
                        BaseTestClass.param_ids[test_name] = test_ids

    return BaseTestClass


def get_tap_test_class(
    tap_class: type[Tap],
    # tap args
    config: dict | PurePath | str | list[PurePath | str] | None = None,
    catalog: PurePath | str | dict | Catalog | None = None,
    state: PurePath | str | dict | None = None,
    parse_env_config: bool = True,
    validate_config: bool = True,
    # test args
    include_tap_tests: bool = True,
    include_stream_tests: bool = True,
    include_stream_attribute_tests: bool = True,
    custom_suites: list | None = None,
    suite_config: SuiteConfig | None = None,
) -> object:
    """Get Tap Test Class.

    Args:
        tap_class: Meltano Singer SDK Tap class to test.
        config: Config dict to use for testing.
        include_tap_tests: Include tap tests.
        include_stream_tests: Include Tap stream tests.
        include_stream_attribute_tests: Include Tap stream attribute tests.
        custom_suites: Custom test suites to add to standard tests.
        suite_config: SuiteConfig instance to pass to tests.
        kwargs: Keyword arguments to pass to the TapRunner.

    Returns:
        A test class usable by pytest.
    """
    suites = custom_suites or []
    if include_tap_tests:
        suites.append(tap_tests)
    if include_stream_tests:
        suites.append(tap_stream_tests)
    if include_stream_attribute_tests:
        suites.append(tap_stream_attribute_tests)

    return get_test_class(
        tap_class=tap_class,
        tap_kwargs={
            "config": config,
            "catalog": catalog,
            "state": state,
            "parse_env_config": parse_env_config,
            "validate_config": validate_config,
        },
        test_suites=suites,
        suite_config=suite_config or SuiteConfig(),
    )


def get_target_test_class(
    target_class: type[Target],
    # target args
    config: dict | PurePath | str | list[PurePath | str] | None = None,
    parse_env_config: bool = True,
    validate_config: bool = True,
    # target runner args
    input_filepath: Path | None = None,
    input_io: io.StringIO | None = None,
    # suite args
    include_target_tests: bool = True,
    custom_suites: list | None = None,
    suite_config: SuiteConfig | None = None,
) -> object:
    """Get Target Test Class.

    Args:
        target_class: Meltano Singer SDK Target class to test.
        config: Config dict to use for testing.
        custom_suites: Custom test suites to add to standard tests.
        suite_config: SuiteConfig instance to pass to tests.
        kwargs: Keyword arguments to pass to the TapRunner.

    Returns:
        A test class usable by pytest.
    """
    suites = custom_suites or []
    if include_target_tests:
        suites.append(target_tests)

    suite_config = suite_config or SuiteConfig()

    return get_test_class(
        target_class=target_class,
        target_kwargs={
            "config": config,
            "parse_env_config": parse_env_config,
            "validate_config": validate_config,
        },
        target_runner_kwargs={"input_filepath": input_filepath, "input_io": input_io},
        test_suites=suites,
        suite_config=suite_config,
    )
