"""Pytest Plugin."""

from __future__ import annotations

import pytest

from singer_sdk.testing.factory import BaseTestClass


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Pytest Hook, responsible for parameterizing tests.

    Called once per each test function, this hook will check if the function name is
    registered in the parent classes 'params' dict, and if so will parameterize
    the given test function with the values therein.

    Args:
        metafunc: Pytest MetaFunc instance, representing a test function or method.
    """
    if metafunc.cls is not None and issubclass(metafunc.cls, BaseTestClass):
        name = metafunc.definition.name
        func_arg_list = metafunc.cls.params.get(name)
        func_arg_ids = metafunc.cls.param_ids.get(name)

        if func_arg_list:
            markers = []
            if "tap_stream_" in name and "attribute" not in name:
                markers.append(pytest.mark.singer_stream)
            elif "tap_stream_attribute_" in name:
                markers.append(pytest.mark.singer_stream_attribute)

            arg_names = list(func_arg_list[0].keys())
            parameters = [
                pytest.param(
                    *tuple(func_args[name] for name in arg_names),
                    marks=markers,
                )
                for func_args in func_arg_list
            ]
            metafunc.parametrize(
                ",".join(arg_names),
                parameters,
                ids=func_arg_ids,
            )
