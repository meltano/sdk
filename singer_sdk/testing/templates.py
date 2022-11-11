import contextlib
import os
import warnings
from pathlib import Path
from typing import Any, List


class TestTemplate:
    """
    Each Test class requires one or more of the following arguments.

    Args:
        runner (SingerTestRunner): The singer runner for this test.

    Possible Args:
        stream (obj, optional): Initialized stream object to be tested.
        stream_name (str, optional): Name of the stream to be tested.
        stream_records (list[obj]): Array of records output by the stream sync.
        attribute_name (str, optional): Name of the attribute to be tested.

    Raises:
        ValueError: [description]
        NotImplementedError: [description]
        NotImplementedError: [description]
    """

    name: str = None
    type: str = None
    required_kwargs: List[str] = []

    def __init__(self, runner, **kwargs):
        if not self.name or not self.type:
            raise ValueError("Test must have 'name' and 'type' properties.")

        self.runner = runner
        for p in self.required_kwargs:
            setattr(self, p, kwargs[p])

    @property
    def id(self):
        raise NotImplementedError("Method not implemented.")

    def setup(self):
        raise NotImplementedError("Method not implemented.")

    def test(self):
        self.runner.sync_all()

    def validate(self):
        raise NotImplementedError("Method not implemented.")

    def teardown(self):
        raise NotImplementedError("Method not implemented.")

    def run(self):
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
    type = "tap"

    def __init__(self, runner, **kwargs):
        super().__init__(runner=runner, **kwargs)
        self.tap = self.runner.tap

    @property
    def id(self):
        return f"tap__{self.name}"


class StreamTestTemplate(TestTemplate):
    type = "stream"
    required_kwargs = ["stream", "stream_name", "stream_records"]

    @property
    def id(self):
        return f"{self.stream_name}__{self.name}"


class AttributeTestTemplate(TestTemplate):
    type = "attribute"
    required_kwargs = ["stream_records", "stream_name", "attribute_name"]

    @property
    def id(self):
        return f"{self.stream_name}__{self.attribute_name}__{self.name}"

    @property
    def non_null_attribute_values(self) -> List[Any]:
        """Helper function to extract attribute values from stream records."""
        values = [
            r[self.attribute_name]
            for r in self.stream_records
            if r.get(self.attribute_name) is not None
        ]
        if not values:
            warnings.warn(UserWarning("No records were available to test."))
        return values


class TargetTestTemplate(TestTemplate):
    type = "target"

    def __init__(self, runner, **kwargs):
        super().__init__(runner=runner, **kwargs)
        self.target = self.runner.target

    @property
    def id(self):
        return f"target__{self.name}"


class TargetFileTestTemplate(TargetTestTemplate):
    """Target Test Template"""

    def __init__(self, runner, **kwargs):
        super().__init__(runner=runner, **kwargs)
        # set the runners input file according to test template singer file path
        if getattr(self, "singer_filepath", None):
            assert Path(
                self.singer_filepath
            ).exists(), f"Singer file {self.singer_filepath} does not exist."
            self.runner.input_filepath = self.singer_filepath

    @property
    def singer_filepath(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_singer_filepath = os.path.join(current_dir, "target_test_streams")
        return os.path.join(base_singer_filepath, f"{self.name}.singer")
