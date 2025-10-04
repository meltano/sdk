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
    if (
        metafunc.cls is not None
        and issubclass(metafunc.cls, BaseTestClass)
        and (params := metafunc.cls.params.get(metafunc.definition.name))
    ):
        parameters = [pytest.param(*param.values, id=param.id) for param in params]
        metafunc.parametrize(params[0].values._fields, parameters)
