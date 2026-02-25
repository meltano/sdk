from __future__ import annotations

from importlib import metadata

SDK_PACKAGE_NAME = "singer_sdk"


def get_package_version(package: str) -> str:
    """Return the package version number.

    Args:
        package: The package name.

    Returns:
        The package version number or "[could not be detected]" if the package is not
        found.
    """
    try:
        version = metadata.version(package)
    except metadata.PackageNotFoundError:
        version = "[could not be detected]"

    return version


def get_sdk_version() -> str:
    """Return the SDK version number.

    Returns:
        The SDK version number.
    """
    return get_package_version(SDK_PACKAGE_NAME)
