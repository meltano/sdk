"""Tests for singer_sdk.testing.factory module."""

from __future__ import annotations

import inspect
import typing as t
from unittest.mock import Mock, patch

import pytest

from singer_sdk import Stream, Tap, Target
from singer_sdk.testing.config import SuiteConfig
from singer_sdk.testing.factory import (
    BaseTestClass,
    StreamAttributeTestParams,
    TapTestClassFactory,
    TargetTestClassFactory,
    get_tap_test_class,
    get_target_test_class,
)
from singer_sdk.testing.suites import (
    SingerTestSuite,
    tap_stream_attribute_tests,
    tap_stream_tests,
    tap_tests,
    target_tests,
)
from singer_sdk.testing.templates import (
    AttributeTestTemplate,
    StreamTestTemplate,
    TapTestTemplate,
)

if t.TYPE_CHECKING:
    from collections.abc import Callable


def _filter_methods(cls: type, condition: Callable[[str, t.Any], bool]) -> set[str]:
    """Filter methods of a class by a condition."""
    return {name for name, method in cls.__dict__.items() if condition(name, method)}


def _filter_methods_by_prefix(cls: type, prefix: str) -> set[str]:
    """Filter methods of a class by a prefix."""
    return _filter_methods(cls, lambda name, _: name.startswith(prefix))


class TestBaseTestClass:
    """Test BaseTestClass behavior."""

    def test_base_test_class_direct_subclass_gets_params(self):
        """Test that direct subclasses get params and param_ids attributes."""

        class DirectSubclass(BaseTestClass):
            pass

        assert hasattr(DirectSubclass, "params")
        assert DirectSubclass.params == {}

    def test_base_test_class_nested_subclass_no_params(self):
        """Test that subclasses of subclasses don't get params attributes."""

        class DirectSubclass(BaseTestClass):
            pass

        class NestedSubclass(DirectSubclass):
            pass

        # Direct subclass should have params
        assert hasattr(DirectSubclass, "params")

        # Nested subclass should not get new params (inherits from direct)
        assert hasattr(NestedSubclass, "params")
        assert NestedSubclass.params is DirectSubclass.params


class TestTapTestClassFactory:
    """Test TapTestClassFactory functionality."""

    @pytest.fixture
    def mock_tap_class(self):
        """Create a mock Tap class for testing."""
        tap_class = Mock(spec=Tap)
        tap_class.__name__ = "MockTap"
        return tap_class

    @pytest.fixture
    def factory(self, mock_tap_class):
        """Create a TapTestClassFactory instance."""
        return TapTestClassFactory(tap_class=mock_tap_class, config={"test": "config"})

    def test_init(self, mock_tap_class):
        """Test TapTestClassFactory initialization."""
        config = {"test": "config"}
        factory = TapTestClassFactory(tap_class=mock_tap_class, config=config)

        assert factory.tap_class is mock_tap_class
        assert factory.config == config

    def test_init_without_config(self, mock_tap_class):
        """Test TapTestClassFactory initialization without config."""
        factory = TapTestClassFactory(tap_class=mock_tap_class)

        assert factory.tap_class is mock_tap_class
        assert factory.config is None

    @patch("singer_sdk.testing.factory.TapTestRunner")
    def test_new_test_class_default_suites(
        self,
        mock_runner_class,
        factory: TapTestClassFactory,
    ):
        """Test new_test_class creates class with default test suites."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        # Mock streams for stream tests with proper stream_maps structure
        mock_stream = Mock(spec=Stream)
        mock_stream.name = "test_stream"
        mock_stream_map = Mock()
        mock_stream_map.transformed_schema = {"properties": {"id": {"type": "integer"}}}
        mock_stream.stream_maps = [mock_stream_map]

        mock_tap = Mock()
        mock_tap.streams = {"test_stream": mock_stream}
        mock_runner.new_tap.return_value = mock_tap

        test_class = factory.new_test_class()

        assert issubclass(test_class, BaseTestClass)
        test_methods = _filter_methods_by_prefix(test_class, "test_tap_")
        assert len(test_methods) > 0

        # Verify TapTestRunner was called with correct parameters
        mock_runner_class.assert_called_once_with(
            tap_class=factory.tap_class,
            config=factory.config,
            suite_config=None,
            parse_env_config=True,
        )

    @patch("singer_sdk.testing.factory.TapTestRunner")
    def test_new_test_class_custom_kwargs(
        self,
        mock_runner_class,
        factory: TapTestClassFactory,
    ):
        """Test new_test_class with custom kwargs."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        mock_stream = Mock(spec=Stream)
        mock_stream.name = "test_stream"
        mock_stream_map = Mock()
        mock_stream_map.transformed_schema = {"properties": {"id": {"type": "integer"}}}
        mock_stream.stream_maps = [mock_stream_map]

        mock_tap = Mock()
        mock_tap.streams = {"test_stream": mock_stream}
        mock_runner.new_tap.return_value = mock_tap

        suite_config = SuiteConfig()
        custom_kwarg = "custom_value"

        test_class = factory.new_test_class(
            suite_config=suite_config,
            custom_kwarg=custom_kwarg,
            parse_env_config=False,
        )

        assert issubclass(test_class, BaseTestClass)
        test_methods = _filter_methods_by_prefix(test_class, "test_tap_")
        assert len(test_methods) > 0

        mock_runner_class.assert_called_once_with(
            tap_class=factory.tap_class,
            config=factory.config,
            suite_config=suite_config,
            parse_env_config=False,
            custom_kwarg=custom_kwarg,
        )

    @patch("singer_sdk.testing.factory.TapTestRunner")
    def test_new_test_class_exclude_tests(
        self,
        mock_runner_class,
        factory: TapTestClassFactory,
    ):
        """Test new_test_class with some test types excluded."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        test_class = factory.new_test_class(
            include_tap_tests=False,
            include_stream_tests=False,
            include_stream_attribute_tests=False,
        )

        assert issubclass(test_class, BaseTestClass)
        test_methods = _filter_methods_by_prefix(test_class, "test_tap_")
        assert len(test_methods) == 0

    @patch("singer_sdk.testing.factory.TapTestRunner")
    def test_new_test_class_custom_suites(
        self,
        mock_runner_class,
        factory: TapTestClassFactory,
    ):
        """Test new_test_class with custom test suites."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        class CustomTapTest(TapTestTemplate):
            name = "custom_tap_test"

            def test(self) -> None: ...

        class CustomStreamTest(StreamTestTemplate):
            name = "custom_stream_test"

            def test(self) -> None: ...

        class CustomAttributeTestOne(AttributeTestTemplate):
            name = "custom_attribute_test_1"

            def test(self) -> None: ...

        class CustomAttributeTestTwo(AttributeTestTemplate):
            name = "custom_attribute_test_2"

            def test(self) -> None: ...

        custom_suite = SingerTestSuite(
            kind="tap",
            tests=[
                CustomTapTest,
                CustomStreamTest,
                CustomAttributeTestOne,
                CustomAttributeTestTwo,
            ],
        )

        test_class = factory.new_test_class(
            custom_suites=[custom_suite],
            include_tap_tests=False,
            include_stream_tests=False,
            include_stream_attribute_tests=False,
        )

        assert issubclass(test_class, BaseTestClass)
        assert len(_filter_methods_by_prefix(test_class, "test_tap_")) == 4

        tap_test = test_class.test_tap_custom_tap_test
        signature = inspect.signature(tap_test)
        assert set(signature.parameters) == {"config", "resource", "runner"}

        stream_test = test_class.test_tap_custom_stream_test
        signature = inspect.signature(stream_test)
        assert set(signature.parameters) == {"config", "resource", "runner", "stream"}

        attribute_test_1 = test_class.test_tap_custom_attribute_test_1
        signature = inspect.signature(attribute_test_1)
        assert set(signature.parameters) == {
            "config",
            "resource",
            "runner",
            "stream",
            "attribute_name",
        }

        attribute_test_2 = test_class.test_tap_custom_attribute_test_2
        signature = inspect.signature(attribute_test_2)
        assert set(signature.parameters) == {
            "config",
            "resource",
            "runner",
            "stream",
            "attribute_name",
        }

    def test_get_empty_test_class(self, factory: TapTestClassFactory):
        """Test _get_empty_test_class creates proper fixtures."""
        mock_runner = Mock()
        suite_config = SuiteConfig()

        test_class = factory._get_empty_test_class(
            test_runner=mock_runner,
            suite_config=suite_config,
        )

        # Check that the class has the expected fixtures
        assert hasattr(test_class, "config")
        assert hasattr(test_class, "resource")
        assert hasattr(test_class, "runner")

        # Test that config fixture returns the suite_config
        # Note: We can't directly call fixture methods, but we can
        # check the method exists
        config_source = inspect.getsource(test_class.config)
        assert "suite_config" in config_source or "SuiteConfig()" in config_source

    def test_get_empty_test_class_no_suite_config(self, factory: TapTestClassFactory):
        """Test _get_empty_test_class with no suite config."""
        mock_runner = Mock()

        test_class = factory._get_empty_test_class(
            test_runner=mock_runner,
            suite_config=None,
        )

        # Check that the class has the expected fixture methods
        assert hasattr(test_class, "config")
        assert hasattr(test_class, "resource")
        assert hasattr(test_class, "runner")

        # Check fixture source to verify default SuiteConfig creation
        config_source = inspect.getsource(test_class.config)
        assert "SuiteConfig()" in config_source

    @patch("singer_sdk.testing.factory.TapTestRunner")
    def test_with_tap_tests(
        self,
        mock_runner_class,
        factory: TapTestClassFactory,
    ):
        """Test _with_tap_tests adds test methods correctly."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        # Create empty test class
        test_class = factory._get_empty_test_class(mock_runner, None)

        # Apply tap tests
        factory._with_tap_tests(test_class, tap_tests)

        # Check that test methods were added
        for test_template_class in tap_tests.tests:
            test_template = test_template_class()
            expected_method_name = f"test_tap_{test_template.name}"
            assert hasattr(test_class, expected_method_name)

    @patch("singer_sdk.testing.factory.TapTestRunner")
    def test_with_stream_tests(
        self,
        mock_runner_class,
        factory: TapTestClassFactory,
    ):
        """Test _with_stream_tests adds parameterized test methods."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        # Create mock streams
        stream1 = Mock(spec=Stream)
        stream1.name = "stream1"
        stream2 = Mock(spec=Stream)
        stream2.name = "stream2"
        streams = [stream1, stream2]

        # Create empty test class
        test_class = factory._get_empty_test_class(mock_runner, None)

        # Apply stream tests
        factory._with_stream_tests(test_class, tap_stream_tests, streams)

        # Check that test methods and parameters were added
        for test_template_class in tap_stream_tests.tests:
            test_template = test_template_class()
            expected_method_name = f"test_tap_stream_{test_template.name}"
            assert hasattr(test_class, expected_method_name)
            assert expected_method_name in test_class.params

            # Check parameters
            params = test_class.params[expected_method_name]
            assert len(params) == 2
            assert [param.id for param in params] == ["stream1", "stream2"]

    @patch("singer_sdk.testing.factory.TapTestRunner")
    def test_with_stream_attribute_tests(
        self,
        mock_runner_class,
        factory: TapTestClassFactory,
    ):
        """Test _with_stream_attribute_tests adds attribute test methods."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        # Create mock stream with schema
        mock_stream = Mock(spec=Stream)
        mock_stream.name = "test_stream"

        # Mock stream_maps and schema
        mock_stream_map = Mock()
        mock_stream_map.transformed_schema = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            }
        }
        mock_stream.stream_maps = [mock_stream_map]

        streams = [mock_stream]

        # Create empty test class
        test_class = factory._get_empty_test_class(mock_runner, None)

        # Mock the evaluate method to return True for some properties
        with patch.object(
            tap_stream_attribute_tests.tests[0], "evaluate", return_value=True
        ):
            factory._with_stream_attribute_tests(
                test_class, tap_stream_attribute_tests, streams
            )

        # Check that test methods were added
        for test_template_class in tap_stream_attribute_tests.tests:
            test_template = test_template_class()
            expected_method_name = f"test_tap_stream_attribute_{test_template.name}"

            if expected_method_name in test_class.params:
                assert hasattr(test_class, expected_method_name)
                params = test_class.params[expected_method_name]

                # Should have parameters for each property that passed evaluate
                assert len(params) > 0

                # Assert that each param's id matches the expected format and values
                # contains correct stream and property
                for param in params:
                    stream, prop = param.id.split(".", 1)
                    assert isinstance(param.values, StreamAttributeTestParams)
                    assert param.values.stream.name == stream
                    assert param.values.attribute_name == prop


class TestTargetTestClassFactory:
    """Test TargetTestClassFactory functionality."""

    @pytest.fixture
    def mock_target_class(self):
        """Create a mock Target class for testing."""
        target_class = Mock(spec=Target)
        target_class.__name__ = "MockTarget"
        return target_class

    @pytest.fixture
    def factory(self, mock_target_class):
        """Create a TargetTestClassFactory instance."""
        return TargetTestClassFactory(
            target_class=mock_target_class, config={"test": "config"}
        )

    def test_init(self, mock_target_class):
        """Test TargetTestClassFactory initialization."""
        config = {"test": "config"}
        factory = TargetTestClassFactory(target_class=mock_target_class, config=config)

        assert factory.target_class is mock_target_class
        assert factory.config == config

    def test_init_without_config(self, mock_target_class):
        """Test TargetTestClassFactory initialization without config."""
        factory = TargetTestClassFactory(target_class=mock_target_class)

        assert factory.target_class is mock_target_class
        assert factory.config is None

    @patch("singer_sdk.testing.factory.TargetTestRunner")
    def test_new_test_class_default_suites(
        self,
        mock_runner_class,
        factory: TargetTestClassFactory,
    ):
        """Test new_test_class creates class with default test suites."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        test_class = factory.new_test_class()

        assert issubclass(test_class, BaseTestClass)
        test_methods = _filter_methods_by_prefix(test_class, "test_target_")
        assert len(test_methods) > 0

    @patch("singer_sdk.testing.factory.TargetTestRunner")
    def test_new_test_class_custom_kwargs(
        self,
        mock_runner_class,
        factory: TargetTestClassFactory,
    ):
        """Test new_test_class with custom kwargs."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        suite_config = SuiteConfig()
        custom_kwarg = "custom_value"

        test_class = factory.new_test_class(
            suite_config=suite_config,
            custom_kwarg=custom_kwarg,
            parse_env_config=False,
        )
        assert issubclass(test_class, BaseTestClass)
        test_methods = _filter_methods_by_prefix(test_class, "test_target_")
        assert len(test_methods) > 0

    @patch("singer_sdk.testing.factory.TargetTestRunner")
    def test_new_test_class_exclude_tests(
        self,
        mock_runner_class,
        factory: TargetTestClassFactory,
    ):
        """Test new_test_class with target tests excluded."""
        mock_runner = Mock()
        mock_runner_class.return_value = mock_runner

        test_class = factory.new_test_class(include_target_tests=False)

        assert issubclass(test_class, BaseTestClass)
        test_methods = _filter_methods_by_prefix(test_class, "test_target_")
        assert len(test_methods) == 0

    def test_get_empty_test_class(self, factory: TargetTestClassFactory):
        """Test _get_empty_test_class creates proper fixtures."""
        suite_config = SuiteConfig()
        config = {"test": "config"}

        test_class = factory._get_empty_test_class(
            target_class=factory.target_class,
            suite_config=suite_config,
            config=config,
            custom_kwarg="value",
        )

        # Check that the class has the expected fixtures
        assert hasattr(test_class, "config")
        assert hasattr(test_class, "resource")
        assert hasattr(test_class, "runner")

        # Verify the config fixture behavior
        config_source = inspect.getsource(test_class.config)
        assert "suite_config" in config_source

    def test_get_empty_test_class_no_suite_config(
        self,
        factory: TargetTestClassFactory,
    ):
        """Test _get_empty_test_class with no suite config."""
        test_class = factory._get_empty_test_class(
            target_class=factory.target_class,
            suite_config=None,
        )

        # Check that the class has the expected fixture methods
        assert hasattr(test_class, "config")
        assert hasattr(test_class, "resource")
        assert hasattr(test_class, "runner")

        # Check fixture source to verify default SuiteConfig creation
        config_source = inspect.getsource(test_class.config)
        assert "SuiteConfig()" in config_source

    def test_annotate_test_class(self, factory: TargetTestClassFactory):
        """Test _annotate_test_class adds test methods correctly."""
        # Create empty test class
        test_class = factory._get_empty_test_class(
            target_class=factory.target_class,
            suite_config=None,
        )

        # Apply target tests
        annotated_class = factory._annotate_test_class(
            empty_test_class=test_class,
            test_suites=[target_tests],
        )

        # Check that test methods were added
        for test_template_class in target_tests.tests:
            test_template = test_template_class()
            expected_method_name = f"test_target_{test_template.name}"
            assert hasattr(annotated_class, expected_method_name)


class TestConvenienceFunctions:
    """Test the convenience functions get_tap_test_class and get_target_test_class."""

    @pytest.fixture
    def mock_tap_class(self):
        """Create a mock Tap class for testing."""
        tap_class = Mock(spec=Tap)
        tap_class.__name__ = "MockTap"
        return tap_class

    @pytest.fixture
    def mock_target_class(self):
        """Create a mock Target class for testing."""
        target_class = Mock(spec=Target)
        target_class.__name__ = "MockTarget"
        return target_class

    @patch("singer_sdk.testing.factory.TapTestClassFactory")
    def test_get_tap_test_class(
        self,
        mock_factory_class,
        mock_tap_class,
    ):
        """Test get_tap_test_class function."""
        mock_factory = Mock()
        mock_test_class = Mock()
        mock_factory.new_test_class.return_value = mock_test_class
        mock_factory_class.return_value = mock_factory

        config = {"test": "config"}
        suite_config = SuiteConfig()
        custom_suites = []

        result = get_tap_test_class(
            tap_class=mock_tap_class,
            config=config,
            include_tap_tests=True,
            include_stream_tests=False,
            include_stream_attribute_tests=True,
            custom_suites=custom_suites,
            suite_config=suite_config,
            custom_kwarg="value",
        )

        # Verify factory was created correctly
        mock_factory_class.assert_called_once_with(
            tap_class=mock_tap_class,
            config=config,
        )

        # Verify new_test_class was called correctly
        mock_factory.new_test_class.assert_called_once_with(
            custom_suites=custom_suites,
            suite_config=suite_config,
            include_tap_tests=True,
            include_stream_tests=False,
            include_stream_attribute_tests=True,
            custom_kwarg="value",
        )

        assert result is mock_test_class

    @patch("singer_sdk.testing.factory.TargetTestClassFactory")
    def test_get_target_test_class(self, mock_factory_class, mock_target_class):
        """Test get_target_test_class function."""
        mock_factory = Mock()
        mock_test_class = Mock()
        mock_factory.new_test_class.return_value = mock_test_class
        mock_factory_class.return_value = mock_factory

        config = {"test": "config"}
        suite_config = SuiteConfig()
        custom_suites = []

        result = get_target_test_class(
            target_class=mock_target_class,
            config=config,
            custom_suites=custom_suites,
            suite_config=suite_config,
            include_target_tests=False,
            custom_kwarg="value",
        )

        # Verify factory was created correctly
        mock_factory_class.assert_called_once_with(
            target_class=mock_target_class,
            config=config,
        )

        # Verify new_test_class was called correctly
        mock_factory.new_test_class.assert_called_once_with(
            custom_suites=custom_suites,
            suite_config=suite_config,
            include_target_tests=False,
            custom_kwarg="value",
        )

        assert result is mock_test_class
