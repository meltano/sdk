import site
from pathlib import Path

from singer_sdk.helpers._compat import get_project_distribution, metadata


def test_get_project_distribution():
    """Test `get_project_distribution()`.

    PyGithub is a representative example of the case where distribution name
    does not match the package name. Any similar singer_sdk dependency could
    be used in stead.
    """
    site_package_paths = site.getsitepackages()
    singer_sdk_dependency_path = Path(site_package_paths[0]) / "github" / "__init__.py"
    discovered_dst = get_project_distribution(singer_sdk_dependency_path)
    assert discovered_dst
    assert discovered_dst.name == "PyGithub"
