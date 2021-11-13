from typing import Callable, List, Type, Any, Tuple

from singer_sdk.tap_base import Tap
from singer_sdk.testing.templates import TapTests, StreamTests, AttributeTests
from singer_sdk.testing.runner import TapTestRunner


def get_standard_tap_tests(tap_class: Type[Tap], config: dict = None) -> List[Callable]:
    """Return callable pytest which executes simple discovery and connection tests.

    Args:
        tap_class: TODO
        config: TODO

    Returns:
        TODO
    """

    runner = TapTestRunner(tap_class, config)
    runner.run_discovery()
    runner.run_sync()

    tap_tests = _generate_tap_tests(runner)
    schema_tests = _generate_schema_tests(runner)
    attribute_tests = _generate_attribute_tests(runner)
    test_manifest = tap_tests + schema_tests + attribute_tests

    return [t for t in test_manifest]


def get_standard_tap_tests_for_pytest(tap_class, config):
    test_manifest = get_standard_tap_tests(tap_class, config)
    test_ids = [initialized_test.id for initialized_test in test_manifest]
    return {"argvalues": test_manifest, "ids": test_ids}


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


def _generate_tap_tests(test_runner: TapTestRunner):
    manifest = []
    default_tests = ["cli_prints", "discovery", "stream_connection"]
    params = dict(
        tap_class=test_runner.tap_class,
        tap_config=test_runner.tap_config,
    )
    for test_name in default_tests:
        test_class = TapTests[test_name].value
        initialized_test = test_class(**params)
        manifest.append(initialized_test)
    return manifest


def _generate_schema_tests(test_runner):
    manifest = []
    default_tests = [
        "record_schema_matches_catalog",
        "returns_records",
        "primary_keys",
    ]
    for stream in test_runner.tap.streams.values():
        params = dict(
            tap_class=test_runner.tap_class,
            tap_init=test_runner.tap,
            tap_config=test_runner.tap_config,
            stream_name=stream.name,
            stream_object=stream,
            stream_records=test_runner.records[stream.name],
        )
        for test_name in default_tests:
            test_class = StreamTests[test_name].value
            initialized_test = test_class(**params)
            manifest.append(initialized_test)
    return manifest


def _generate_attribute_tests(test_runner):
    manifest = []
    for stream in test_runner.tap.streams.values():
        schema = stream.schema
        for k, v in schema["properties"].items():
            params = dict(
                stream_records=test_runner.records[stream.name],
                stream_object=stream,
                stream_name=stream.name,
                attribute_name=k,
            )
            if v.get("required"):
                test_class = AttributeTests.is_unique.value
                manifest.append(test_class(**params))
            if v.get("format") == "date-time":
                test_class = AttributeTests.is_datetime.value
                manifest.append(test_class(**params))
            if not "null" in v.get("type", []):
                test_class = AttributeTests.not_null.value
                manifest.append(test_class(**params))
            if "boolean" in v.get("type", []):
                test_class = AttributeTests.is_boolean.value
                manifest.append(test_class(**params))
            if "integer" in v.get("type", []):
                test_class = AttributeTests.is_integer.value
                manifest.append(test_class(**params))
            if "object" in v.get("type", []):
                test_class = AttributeTests.is_object.value
                manifest.append(test_class(**params))
    return manifest
