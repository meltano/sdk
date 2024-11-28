# Testing Taps & Targets

The Meltano SDK includes suites of standard tests for both Taps and Targets to help you get started.
These suites cover most common cases out-of-the-box, and tests are added to the standard suites as new errors are encountered by users in their deployments.

## Test Framework

The Meltano SDK test framework consists of 4 main components:

1. A runner class (`TapTestRunner` and `TargetTestRunner`), responsible for executing Taps/Targets and capturing their output.
1. A suite dataclass, containing a list of tests.
1. A test template classes (`TapTestTemplate`, `StreamTestTemplate`, `AttributeTestTemplate` and `TargetTestTemplate`), with methods to `.setup()`, `.test()`, `.validate()` and `.teardown()` (called in that order using `.run()`).
1. {func}`get_tap_test_class <singer_sdk.testing.get_tap_test_class>` and {func}`get_target_test_class <singer_sdk.testing.get_target_test_class>` factory methods. These wrap a `get_test_class` factory method, which takes a runner and a list of suites and return a `pytest` test class.

## Example Usage

If you created your Tap/Target using the provided cookiecutter templates, you will find the following snippets in `tests/test_core.py`.

### Testing Taps

```python
import datetime

from singer_sdk.testing import get_tap_test_class

from example.tap import TapExample

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
}


# Run standard built-in tap tests from the SDK:
TestTapExample = get_tap_test_class(
    tap_class=TapExample,
    config=SAMPLE_CONFIG
)
```

### Testing Targets

```python
import pytest
from typing import Dict, Any

from singer_sdk.testing import get_target_test_class

from example.target import TargetExample

SAMPLE_CONFIG: Dict[str, Any] = {
    # TODO: Initialize minimal target config
}

# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetExample,
    config=SAMPLE_CONFIG
)


class TestTargetExample(StandardTargetTests):
    """Standard Target Tests."""

    @pytest.fixture()
    def resource(self):
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        yield "resource"
```

## Configuring Tests

Test suite behaviors can be configured by passing a {func}`SuiteConfig <singer_sdk.testing.SuiteConfig>` instance to the `get_test_class` functions:

```python
from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_stackexchange.tap import TapStackExchange

SAMPLE_CONFIG = {
    "site": "stackoverflow",
    "tags": [
        "meltano",
        "singer-io",
    ],
    "metrics_log_level": "debug",
}

TEST_SUITE_CONFIG = SuiteConfig(
    ignore_no_records_for_streams=["tag_synonyms"]
)

TestTapStackExchange = get_tap_test_class(
    tap_class=TapStackExchange, config=SAMPLE_CONFIG, suite_config=TEST_SUITE_CONFIG
)
```

Check out [the reference](#reference) for more information on the available configuration options.

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
    kind="tap", tests=[TapCLIPrintsTest]
)
```

This suite can now be passed to {func}`get_tap_test_class <singer_sdk.testing.get_tap_test_class>` or {func}`get_target_test_class <singer_sdk.testing.get_target_test_class>` in a list of `custom_suites` along with any other suites, to generate your custom test class.

If your new test covers a common or general case, consider contributing to the standard test library via a pull request to [meltano/sdk](https://github.com/meltano/sdk).

## Reference

```{eval-rst}
.. autofunction:: singer_sdk.testing.get_tap_test_class
```

```{eval-rst}
.. autofunction:: singer_sdk.testing.get_target_test_class
```

```{eval-rst}
.. autoclass:: singer_sdk.testing.SuiteConfig
    :members:
```
