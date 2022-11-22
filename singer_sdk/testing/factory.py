from __future__ import annotations

import pytest

from .runners import SingerTestRunner


def pytest_generate_tests(metafunc):
    # called once per each test function
    if metafunc.cls:
        if hasattr(metafunc.cls, "params"):
            funcarglist = metafunc.cls.params.get(metafunc.definition.name)
            funcargids = (
                metafunc.cls.param_ids.get(metafunc.definition.name)
                if hasattr(metafunc.cls, "param_ids")
                else None
            )
            if funcarglist:
                argnames = funcarglist[0].keys()
                metafunc.parametrize(
                    ",".join(argnames),
                    [[funcargs[name] for name in argnames] for funcargs in funcarglist],
                    ids=funcargids,
                )


def get_test_class(
    test_runner: SingerTestRunner,
    test_suites: list,
    base_test_class: object = None,
):
    test_runner.sync_all()

    class BaseTestClass:
        """Base test class."""

        params = {}
        param_ids = {}

        @pytest.fixture
        def resource(self):
            yield

        @pytest.fixture(scope="class")
        def runner(self):
            return test_runner

    test_factory_class = base_test_class or BaseTestClass

    for suite in test_suites:

        if suite.type in {"tap", "target"}:
            for TestClass in suite.tests:
                test = TestClass()
                test_name = f"test_{suite.type}_{test.name}"
                setattr(test_factory_class, f"test_{suite.type}_{test.name}", test.run)

        if suite.type == "tap_stream":

            params = [
                {
                    "stream": stream,
                    "stream_records": test_runner.records[stream.name],
                }
                for stream in test_runner.tap.streams.values()
            ]
            param_ids = [stream.name for stream in test_runner.tap.streams.values()]

            for TestClass in suite.tests:
                test = TestClass()
                test_name = f"test_{suite.type}_{test.name}"
                setattr(
                    test_factory_class,
                    test_name,
                    test.run,
                )
                test_factory_class.params[test_name] = params
                test_factory_class.param_ids[test_name] = param_ids

        if suite.type == "tap_stream_attribute":

            for TestClass in suite.tests:
                test = TestClass()
                test_name = f"test_{suite.type}_{test.name}"
                test_params = []
                test_ids = []
                for stream in test_runner.tap.streams.values():
                    test_params.extend(
                        [
                            {
                                "stream": stream,
                                "stream_records": test_runner.records[stream.name],
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
                        test_factory_class,
                        test_name,
                        test.run,
                    )
                    test_factory_class.params[test_name] = test_params
                    test_factory_class.param_ids[test_name] = test_ids

    return test_factory_class
