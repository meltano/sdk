from __future__ import annotations

import pytest

from singer_sdk.batch import import_batch_dependency
from singer_sdk.exceptions import (
    ConfigurationError,
    MissingDependencyError,
    SingerSDKError,
)


def test_missing_dependency_error_hierarchy() -> None:
    assert issubclass(MissingDependencyError, ConfigurationError)
    assert issubclass(MissingDependencyError, SingerSDKError)
    assert issubclass(MissingDependencyError, ImportError)


def test_missing_dependency_error_message() -> None:
    with pytest.raises(MissingDependencyError) as exc_info:
        import_batch_dependency(
            "not_a_real_package_xyz",
            feature="Doing a thing",
            install="singer-sdk[thing]",
        )

    message = str(exc_info.value)
    assert "Doing a thing requires the 'not_a_real_package_xyz' package" in message
    assert "pip install 'singer-sdk[thing]'" in message


def test_import_batch_dependency_installed() -> None:
    assert import_batch_dependency("json", feature="x").dumps
