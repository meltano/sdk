import platform
import site
from pathlib import Path

import pytest

from singer_sdk.helpers._compat import get_project_distribution, metadata


def test_get_project_distribution():
    """Test `get_project_distribution()`.

    click is a representative example of a distributed package from our dependency tree.
    Any similar singer_sdk dependency could be used in stead.
    """
    if platform.system() == "Windows":
        pytest.xfail("Doesn't pass on windows.")

    site_package_paths = site.getsitepackages()
    site_package_path = next(
        pth for pth in site_package_paths if Path(pth).parts[-1] == "site-packages"
    )
    singer_sdk_dependency_path = Path(site_package_path) / "click" / "__init__.py"
    discovered_dst = get_project_distribution(singer_sdk_dependency_path)
    assert discovered_dst
    assert discovered_dst.name == "click"
