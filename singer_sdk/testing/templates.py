"""Tap and Target Test Templates."""

from __future__ import annotations

import contextlib
import typing as t
import warnings
from pathlib import Path

from singer_sdk.helpers._compat import resources
from singer_sdk.testing import target_test_streams

if t.TYPE_CHECKING:
    from singer_sdk.streams import Stream

    from .config import SuiteConfig
    from .runners import TapTestRunner, TargetTestRunner


class TestTemplate:
    """Each Test class requires one or more of the following arguments.

    Args:
        runner (SingerTestRunner): The singer runner for this test.

    Possible Args:
        stream (obj, optional): Initialized stream object to be tested.
        stream_name (str, optional): Name of the stream to be tested.
        attribute_name (str, optional): Name of the attribute to be tested.

    Raises:
        ValueError: [description]
        NotImplementedError: [description]
        NotImplementedError: [description]
    """

    name: str | None = None
    plugin_type: str | None = None

    @property
    def id(self) -> str:  # noqa: A003
        """Test ID.

        Raises:
            NotImplementedError: if not implemented.
        """
        msg = "ID not implemented."
        raise NotImplementedError(msg)

    def setup(self) -> None:
        """Test setup, called before `.test()`.

        This method is useful for preparing external resources (databases, folders etc.)
        before test execution.

        Raises:
            NotImplementedError: if not implemented.
        """
        msg = "Setup method not implemented."
        raise NotImplementedError(msg)

    def test(self) -> None:
        """Main Test body, called after `.setup()` and before `.validate()`."""
        self.runner.sync_all()

    def validate(self) -> None:
        """Test validation, called after `.test()`.

        This method is particularly useful in Target tests, to validate that records
        were correctly written to external systems.

        Raises:
            NotImplementedError: if not implemented.
        """
        msg = "Method not implemented."
        raise NotImplementedError(msg)

    def teardown(self) -> None:
        """Test Teardown.

        This method is useful for cleaning up external resources
        (databases, folders etc.) after test completion.

        Raises:
            NotImplementedError: if not implemented.
        """
        msg = "Method not implemented."
        raise NotImplementedError(msg)

    def run(
        self,
        config: SuiteConfig,
        resource: t.Any,
        runner: TapTestRunner | TargetTestRunner,
    ) -> None:
        """Test main run method.

        Args:
            config: SuiteConfig instance, to use for test.
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap or Target runner instance, to use with this test.

        Raises:
            ValueError: if Test instance does not have `name` and `type` properties.
        """
        if not self.name or not self.plugin_type:
            msg = "Test must have 'name' and 'type' properties."
            raise ValueError(msg)

        self.config = config
        self.resource = resource
        self.runner = runner

        with contextlib.suppress(NotImplementedError):
            self.setup()

        try:
            self.test()
            with contextlib.suppress(NotImplementedError):
                self.validate()

        finally:
            with contextlib.suppress(NotImplementedError):
                self.teardown()


class TapTestTemplate(TestTemplate):
    """Base Tap test template."""

    plugin_type = "tap"

    @property
    def id(self) -> str:  # noqa: A003
        """Test ID.

        Returns:
            Test ID string.
        """
        return f"tap__{self.name}"

    def run(  # type: ignore[override]
        self,
        config: SuiteConfig,
        resource: t.Any,
        runner: TapTestRunner,
    ) -> None:
        """Test main run method.

        Args:
            config: SuiteConfig instance, to use for test.
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap or Target runner instance, to use with this test.
        """
        self.tap = runner.new_tap()
        super().run(config, resource, runner)


class StreamTestTemplate(TestTemplate):
    """Base Tap Stream test template."""

    plugin_type = "stream"
    required_kwargs: t.ClassVar[list[str]] = ["stream"]

    @property
    def id(self) -> str:  # noqa: A003
        """Test ID.

        Returns:
            Test ID string.
        """
        return f"{self.stream.name}__{self.name}"

    def run(  # type: ignore[override]
        self,
        config: SuiteConfig,
        resource: t.Any,
        runner: TapTestRunner,
        stream: Stream,
    ) -> None:
        """Test main run method.

        Args:
            config: SuiteConfig instance, to use for test.
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap runner instance, to use with this test.
            stream: A Tap Stream instance, to use with this test.
        """
        self.stream = stream
        self.stream_records = runner.records[stream.name]
        super().run(config, resource, runner)


class AttributeTestTemplate(TestTemplate):
    """Base Tap Stream Attribute template."""

    plugin_type = "attribute"

    @property
    def id(self) -> str:  # noqa: A003
        """Test ID.

        Returns:
            Test ID string.
        """
        return f"{self.stream.name}__{self.attribute_name}__{self.name}"

    def run(  # type: ignore[override]
        self,
        config: SuiteConfig,
        resource: t.Any,
        runner: TapTestRunner,
        stream: Stream,
        attribute_name: str,
    ) -> None:
        """Test main run method.

        Args:
            config: SuiteConfig instance, to use for test.
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap runner instance, to use with this test.
            stream: A Tap Stream instance, to use with this test.
                to use with this test.
            attribute_name: The name of the attribute to test.
        """
        self.stream = stream
        self.stream_records = runner.records[stream.name]
        self.attribute_name = attribute_name
        super().run(config, resource, runner)

    @property
    def non_null_attribute_values(self) -> list[t.Any]:
        """Extract attribute values from stream records.

        Returns:
            A list of attribute values (excluding None values).
        """
        values = [
            r[self.attribute_name]
            for r in self.stream_records
            if r.get(self.attribute_name) is not None
        ]
        if not values:
            warnings.warn(
                UserWarning("No records were available to test."),
                stacklevel=2,
            )
        return values

    @classmethod
    def evaluate(
        cls,
        stream: Stream,  # noqa: ARG003
        property_name: str,  # noqa: ARG003
        property_schema: dict,  # noqa: ARG003
    ) -> bool:
        """Determine if this attribute test is applicable to the given property.

        Args:
            stream: Parent Stream of given attribute.
            property_name: Name of given attribute.
            property_schema: JSON Schema of given property, in dict form.

        Raises:
            NotImplementedError: if not implemented.
        """
        msg = (
            "The 'evaluate' method is required for attribute tests, but not "
            "implemented."
        )
        raise NotImplementedError(msg)


class TargetTestTemplate(TestTemplate):
    """Base Target test template."""

    plugin_type = "target"

    def run(  # type: ignore[override]
        self,
        config: SuiteConfig,
        resource: t.Any,
        runner: TargetTestRunner,
    ) -> None:
        """Test main run method.

        Args:
            config: SuiteConfig instance, to use for test.
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap runner instance, to use with this test.
        """
        self.target = runner.new_target()
        super().run(config, resource, runner)

    @property
    def id(self) -> str:  # noqa: A003
        """Test ID.

        Returns:
            Test ID string.
        """
        return f"target__{self.name}"


class TargetFileTestTemplate(TargetTestTemplate):
    """Base Target File Test Template.

    Use this when sourcing Target test input from a .singer file.
    """

    def run(  # type: ignore[override]
        self,
        config: SuiteConfig,
        resource: t.Any,
        runner: TargetTestRunner,
    ) -> None:
        """Test main run method.

        Args:
            config: SuiteConfig instance, to use for test.
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap runner instance, to use with this test.
        """
        # get input from file
        if getattr(self, "singer_filepath", None):
            assert Path(
                self.singer_filepath,
            ).exists(), f"Singer file {self.singer_filepath} does not exist."
            runner.input_filepath = self.singer_filepath
        super().run(config, resource, runner)

    @property
    def singer_filepath(self) -> Path:
        """Get path to singer JSONL formatted messages file.

        Files will be sourced from `./target_test_streams/<test name>.singer`.

        Returns:
            The expected Path to this tests singer file.
        """
        return resources.files(target_test_streams).joinpath(f"{self.name}.singer")  # type: ignore[no-any-return]
