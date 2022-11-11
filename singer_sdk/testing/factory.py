from functools import partial

from .runners import SingerTestRunner


def _get_test_by_name(test_suite, test_name, test_runner, test_params={}):
    TestClass = next(
        (TestClass for TestClass in test_suite.tests if TestClass.name == test_name)
    )
    return TestClass(runner=test_runner, **test_params)


def get_test_class(
    test_runner: SingerTestRunner, test_suites: list, base_test_class: object = None
):
    class BaseTestClass:
        """Base test class."""

    test_class = base_test_class or BaseTestClass

    for suite in test_suites:
        if suite.type == "tap":
            for TestClass in suite.tests:
                test = TestClass(runner=test_runner)
                setattr(test_class, f"test_{suite.type}_{test.name}", test.run)

        if suite.type in {"tap_stream", "tap_stream_attribute"}:
            # get catalog and records
            test_runner.run_discovery()
            test_runner.sync_all()

            if suite.type == "tap_stream":

                for stream in test_runner.tap.streams.values():
                    test_params = {
                        "stream": stream,
                        "stream_name": stream.name,
                        "stream_records": test_runner.records[stream.name],
                    }
                    for TestClass in suite.tests:
                        test = TestClass(runner=test_runner, **test_params)
                        test_name = f"test_{suite.type}_{stream.name}_{test.name}"
                        setattr(test_class, test_name, test.run)

            if suite.type == "tap_stream_attribute":

                for stream in test_runner.tap.streams.values():
                    schema = stream.schema
                    for k, v in schema["properties"].items():
                        test_params = dict(
                            attribute_name=k,
                            stream=stream,
                            stream_name=stream.name,
                            stream_records=test_runner.records[stream.name],
                        )
                        get_test = partial(
                            _get_test_by_name,
                            test_suite=suite,
                            test_runner=test_runner,
                            test_params=test_params,
                        )

                        if v.get("required"):
                            test = get_test(test_name="is_unique")
                            test_name = (
                                f"test_{suite.type}_{stream.name}_{k}_{test.name}"
                            )
                            setattr(test_class, test_name, test.run)

                        if v.get("format") == "date-time":
                            test = get_test(test_name="is_datetime")
                            test_name = (
                                f"test_{suite.type}_{stream.name}_{k}_{test.name}"
                            )
                            setattr(test_class, test_name, test.run)

                        if "null" not in v.get("type", []):
                            test = get_test(test_name="not_null")
                            test_name = (
                                f"test_{suite.type}_{stream.name}_{k}_{test.name}"
                            )
                            setattr(test_class, test_name, test.run)

                        if "boolean" in v.get("type", []):
                            test = get_test(test_name="is_boolean")
                            test_name = (
                                f"test_{suite.type}_{stream.name}_{k}_{test.name}"
                            )
                            setattr(test_class, test_name, test.run)

                        if "integer" in v.get("type", []):
                            test = get_test(test_name="is_integer")
                            test_name = (
                                f"test_{suite.type}_{stream.name}_{k}_{test.name}"
                            )
                            setattr(test_class, test_name, test.run)

                        if "object" in v.get("type", []):
                            test = get_test(test_name="is_object")
                            test_name = (
                                f"test_{suite.type}_{stream.name}_{k}_{test.name}"
                            )
                            setattr(test_class, test_name, test.run)

    return test_class
