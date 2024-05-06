"""Test Class Factory."""

from __future__ import annotations

import typing as t

import pytest

from .config import SuiteConfig
from .runners import TapTestRunner, TargetTestRunner
from .suites import (
    TestSuite,
    tap_stream_attribute_tests,
    tap_stream_tests,
    tap_tests,
    target_tests,
)

if t.TYPE_CHECKING:
    from singer_sdk import Stream, Tap, Target
    from singer_sdk.testing.templates import (
        AttributeTestTemplate,
        StreamTestTemplate,
        TapTestTemplate,
    )


class BaseTestClass:
    """Base test class."""

    params: dict[str, t.Any]
    param_ids: dict[str, list[str]]

    def __init_subclass__(cls, **kwargs: t.Any) -> None:
        """Initialize a subclass.

        Args:
            **kwargs: Keyword arguments.
        """
        # Add empty params and param_ids attributes to a direct subclass but not to
        # subclasses of subclasses
        if cls.__base__ == BaseTestClass:
            cls.params = {}
            cls.param_ids = {}


class TapTestClassFactory:
    """Factory for Tap Test Classes."""

    def __init__(
        self,
        tap_class: type[Tap],
        *,
        config: dict | None = None,
    ):
        """Initialize TapTestClassFactory.

        Args:
            tap_class: Tap class to be tested.
            config: Tap configuration for testing.
        """
        self.tap_class = tap_class
        self.config = config

    def new_test_class(
        self,
        *,
        include_tap_tests: bool = True,
        include_stream_tests: bool = True,
        include_stream_attribute_tests: bool = True,
        custom_suites: list | None = None,
        suite_config: SuiteConfig | None = None,
        **kwargs: t.Any,
    ) -> type[BaseTestClass]:
        """Get a new test class.

        Args:
            include_tap_tests: Include tap tests in the test class.
            include_stream_tests: Include stream tests in the test class.
            include_stream_attribute_tests:
                Include stream attribute tests in the test class.
            custom_suites: List of custom test suites to include in the test class.
            suite_config: SuiteConfig instance to be used when instantiating tests.
            kwargs: Default arguments to be passed to tap on create.

        Returns:
            A new test class.
        """
        # compile test suites
        suites = custom_suites or []
        if include_tap_tests:
            suites.append(tap_tests)
        if include_stream_tests:
            suites.append(tap_stream_tests)
        if include_stream_attribute_tests:
            suites.append(tap_stream_attribute_tests)

        # set default values
        if "parse_env_config" not in kwargs:
            kwargs["parse_env_config"] = True

        # create singleton test runner
        test_runner = TapTestRunner(
            tap_class=self.tap_class,
            config=self.config,
            suite_config=suite_config,
            **kwargs,
        )

        empty_test_class = self._get_empty_test_class(
            test_runner=test_runner,
            suite_config=suite_config,
        )
        return self._annotate_test_class(
            empty_test_class=empty_test_class,
            test_suites=suites,
            test_runner=test_runner,
        )

    def _get_empty_test_class(  # noqa: PLR6301
        self,
        test_runner: TapTestRunner,
        suite_config: SuiteConfig | None,
    ) -> type[BaseTestClass]:
        """Get an empty test class.

        Args:
            test_runner: Test runner to be used in the test class.
            suite_config: SuiteConfig instance to be used when instantiating tests.

        Returns:
            An empty test class.
        """

        class TapTestClass(BaseTestClass):
            """Tap Test Class."""

            @pytest.fixture
            def config(self) -> SuiteConfig:  # noqa: PLR6301
                return suite_config or SuiteConfig()

            @pytest.fixture
            def resource(self) -> t.Any:  # noqa: ANN401, PLR6301, PT004
                yield  # noqa: PT022

            @pytest.fixture(scope="class")
            def runner(self) -> TapTestRunner | TargetTestRunner:  # noqa: PLR6301
                # Populate runner class with cached records for use in tests
                test_runner.sync_all()
                return test_runner

        return TapTestClass

    def _annotate_test_class(
        self,
        empty_test_class: type[BaseTestClass],
        test_suites: list,
        test_runner: TapTestRunner,
    ) -> type[BaseTestClass]:
        """Annotate test class with test methods.

        Args:
            empty_test_class: Empty test class to be annotated.
            test_suites: List of test suites to include in the test class.
            test_runner: Test runner to be used in the test class.

        Returns:
            An annotated test class.
        """
        for suite in test_suites:
            if suite.kind == "tap":
                self._with_tap_tests(empty_test_class, suite)

            if suite.kind in {"tap_stream", "tap_stream_attribute"}:
                streams = list(test_runner.new_tap().streams.values())

                if suite.kind == "tap_stream":
                    self._with_stream_tests(empty_test_class, suite, streams)

                if suite.kind == "tap_stream_attribute":
                    self._with_stream_attribute_tests(empty_test_class, suite, streams)

        return empty_test_class

    def _with_tap_tests(  # noqa: PLR6301
        self,
        empty_test_class: type[BaseTestClass],
        suite: TestSuite[TapTestTemplate],
    ) -> None:
        for test_class in suite.tests:
            test = test_class()
            test_name = f"test_{suite.kind}_{test.name}"
            setattr(empty_test_class, test_name, test.run)

    def _with_stream_tests(  # noqa: PLR6301
        self,
        empty_test_class: type[BaseTestClass],
        suite: TestSuite[StreamTestTemplate],
        streams: list[Stream],
    ) -> None:
        params = [
            {
                "stream": stream,
            }
            for stream in streams
        ]
        param_ids = [stream.name for stream in streams]

        for test_class in suite.tests:
            test = test_class()
            test_name = f"test_{suite.kind}_{test.name}"
            setattr(
                empty_test_class,
                test_name,
                test.run,
            )
            empty_test_class.params[test_name] = params
            empty_test_class.param_ids[test_name] = param_ids

    def _with_stream_attribute_tests(  # noqa: PLR6301
        self,
        empty_test_class: type[BaseTestClass],
        suite: TestSuite[AttributeTestTemplate],
        streams: list[Stream],
    ) -> None:
        for test_class in suite.tests:
            test = test_class()
            test_name = f"test_{suite.kind}_{test.name}"
            test_params = []
            test_ids: list[str] = []
            for stream in streams:
                final_schema = stream.stream_maps[-1].transformed_schema["properties"]
                test_params.extend(
                    [
                        {
                            "stream": stream,
                            "attribute_name": prop_name,
                        }
                        for prop_name, prop_schema in final_schema.items()
                        if test_class.evaluate(
                            stream=stream,
                            property_name=prop_name,
                            property_schema=prop_schema,
                        )
                    ],
                )
                test_ids.extend(
                    [
                        f"{stream.name}.{prop_name}"
                        for prop_name, prop_schema in final_schema.items()
                        if test_class.evaluate(
                            stream=stream,
                            property_name=prop_name,
                            property_schema=prop_schema,
                        )
                    ],
                )

            if test_params:
                setattr(
                    empty_test_class,
                    test_name,
                    test.run,
                )
                empty_test_class.params[test_name] = test_params
                empty_test_class.param_ids[test_name] = test_ids


class TargetTestClassFactory:
    """Factory for Target Test Classes."""

    def __init__(self, target_class: type[Target], *, config: dict | None = None):
        """Initialize TargetTestClassFactory.

        Args:
            target_class: Target class to be tested.
            config: Config to be used when instantiating tests.
        """
        self.target_class = target_class
        self.config = config

    def new_test_class(
        self,
        *,
        custom_suites: list | None = None,
        suite_config: SuiteConfig | None = None,
        include_target_tests: bool = True,
        **kwargs: t.Any,
    ) -> type[BaseTestClass]:
        """Get a new Target test class.

        Args:
            custom_suites: List of custom test suites to include in the test class.
            suite_config: SuiteConfig instance to be used when instantiating tests.
            include_target_tests: Whether to include target tests in the test class.
            kwargs: Keyword arguments to be passed to the Target on run.

        Returns:
            A new Target test class.
        """
        # compile test suites
        suites = custom_suites or []
        if include_target_tests:
            suites.append(target_tests)

        # set default values
        if "parse_env_config" not in kwargs:
            kwargs["parse_env_config"] = True

        empty_test_class = self._get_empty_test_class(
            target_class=self.target_class,
            config=self.config,
            suite_config=suite_config,
            **kwargs,
        )
        return self._annotate_test_class(
            empty_test_class=empty_test_class,
            test_suites=suites,
        )

    def _get_empty_test_class(  # noqa: PLR6301
        self,
        target_class: type[Target],
        suite_config: SuiteConfig | None,
        config: dict | None = None,
        **kwargs: t.Any,
    ) -> type[BaseTestClass]:
        """Get an empty test class.

        Args:
            target_class: Target class to be tested.
            suite_config: SuiteConfig instance to be used when instantiating tests.
            config: Config to be used when instantiating tests.
            kwargs: Keyword arguments to be passed to the Target on run.

        Returns:
            An empty test class.
        """

        class TargetTestClass(BaseTestClass):
            """Target Test Class."""

            @pytest.fixture
            def config(self) -> SuiteConfig:  # noqa: PLR6301
                return suite_config or SuiteConfig()

            @pytest.fixture
            def resource(self) -> t.Any:  # noqa: ANN401, PLR6301, PT004
                yield  # noqa: PT022

            @pytest.fixture
            def runner(self) -> TargetTestRunner:  # noqa: PLR6301
                # Instantiate new runner class and populate records for use in tests
                return TargetTestRunner(
                    target_class=target_class,
                    config=config,
                    suite_config=suite_config,
                    **kwargs,
                )

        return TargetTestClass

    def _annotate_test_class(  # noqa: PLR6301
        self,
        empty_test_class: type[BaseTestClass],
        test_suites: list,
    ) -> type[BaseTestClass]:
        """Annotate test class with test methods.

        Args:
            empty_test_class: Empty test class to be annotated.
            test_suites: List of test suites to be included in the test class.

        Returns:
            Annotated test class.
        """
        for suite in test_suites:
            if suite.kind == "target":
                for test_class in suite.tests:
                    test = test_class()
                    test_name = f"test_{suite.kind}_{test.name}"
                    setattr(empty_test_class, test_name, test.run)

        return empty_test_class


def get_tap_test_class(
    tap_class: type[Tap],
    *,
    config: dict | None = None,
    include_tap_tests: bool = True,
    include_stream_tests: bool = True,
    include_stream_attribute_tests: bool = True,
    custom_suites: list | None = None,
    suite_config: SuiteConfig | None = None,
    **kwargs: t.Any,
) -> type[BaseTestClass]:
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
    factory = TapTestClassFactory(
        tap_class=tap_class,
        config=config,
    )
    return factory.new_test_class(
        custom_suites=custom_suites,
        suite_config=suite_config,
        include_tap_tests=include_tap_tests,
        include_stream_tests=include_stream_tests,
        include_stream_attribute_tests=include_stream_attribute_tests,
        **kwargs,
    )


def get_target_test_class(
    target_class: type[Target],
    *,
    config: dict | None = None,
    custom_suites: list | None = None,
    suite_config: SuiteConfig | None = None,
    include_target_tests: bool = True,
    **kwargs: t.Any,
) -> type[BaseTestClass]:
    """Get Target Test Class.

    Args:
        target_class: Meltano Singer SDK Target class to test.
        config: Config dict to use for testing.
        custom_suites: Custom test suites to add to standard tests.
        suite_config: SuiteConfig instance to pass to tests.
        include_target_tests: Include standard target tests.
        kwargs: Keyword arguments to pass to the TapRunner.

    Returns:
        A test class usable by pytest.
    """
    factory = TargetTestClassFactory(
        target_class=target_class,
        config=config,
    )
    return factory.new_test_class(
        custom_suites=custom_suites,
        suite_config=suite_config,
        include_target_tests=include_target_tests,
        **kwargs,
    )
