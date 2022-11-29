# Testing Taps & Targets

The Meltano SDK includes suites of standard tests for both Taps and Targets to help you get started.
These suites cover most common cases out-of-the-box, and tests are added to the standard suites as new errors are encountered by users in their deployments.

## Test Framework

The Meltano SDK test framework consists of 4 main components:

1. A runner class (`TapTestRunner` and `TargetTestRunner`), responsible for executing Taps/Targets and capturing their output.
1. A suite dataclass, containing a list of tests.
1. A test template classes (`TapTestTemplate`, `StreamTestTemplate`, `AttributeTestTemplate` and `TargetTestTemplate`), with methods to `.setup()`, `.test()`, `.validate()` and `.teardown()` (called in that order using `.run()`).
1. A `get_test_class` factory method, to take a runner and a list of suits and return a `pytest` test class.

## Example Usage

If you created your Tap/Target using the provided cookiecutter templates, you will find the following snippets in `<library_name>/tests/<library_name>_core.py`.

### Testing Taps

```python
import datetime

from singer_sdk.testing import (
    TapTestRunner,
    get_test_class,
    pytest_generate_tests # pytest hook function, required for standard tests
)
from singer_sdk.testing.suites import (
    tap_stream_attribute_tests,
    tap_stream_tests,
    tap_tests,
)

from example.tap import TapExample

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
}


# Run standard built-in tap tests from the SDK:
TestTapExample = get_test_class(
    test_runner=TapTestRunner(tap_class=TapExample, config=SAMPLE_CONFIG),
    test_suites=[tap_tests, tap_stream_tests, tap_stream_attribute_tests],
)
```

### Testing Targets

```python
import pytest
from typing import Dict, Any

from singer_sdk.testing import TargetTestRunner, get_test_class
from singer_sdk.testing.suites import target_tests

from example.target import TargetExample

SAMPLE_CONFIG: Dict[str, Any] = {
    # TODO: Initialize minimal target config
}

# Run standard built-in target tests from the SDK:
StandardTargetTests = get_test_class(
    test_runner=TargetTestRunner(
        target_class=TargetTargetExample, config=SAMPLE_CONFIG
    ),
    test_suites=[target_tests],
)


class TestTargetExample(StandardTargetTests):
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        yield "resource"
```

## Writing New Tests

Writing new tests is as easy as subclassing the appropriate class.
Check out [`singer_sdk/testing/tap_tests.py`](https://github.com/meltano/sdk/tree/main/singer_sdk/testing/tap_tests.py) and [`singer_sdk/testing/target_tests.py`](https://github.com/meltano/sdk/tree/main/singer_sdk/testing/target_tests.py) for inspiration.

```python
class TapCLIPrintsTest(TapTestTemplate):
    "Test that the tap is able to print standard metadata."
    name = "cli_prints"

    def test(self):
        self.tap.print_version()
        self.tap.print_about()
        self.tap.print_about(format="json")
```

Once you have created some tests, add them to a suite:

```python
my_custom_tap_tests = TestSuite(
    type="tap", tests=[TapCLIPrintsTest]
)
```

This suite can now be passed to `get_test_class` along with any other suites, to generate your custom test class.

If your new test covers a common or general case, consider contributing to the standard test library via a pull request to [meltano/sdk](https://github.com/meltano/sdk).
