#!/usr/bin/env python
from pathlib import Path
import shutil


PACKAGE_PATH = Path("{{cookiecutter.library_name}}")


if __name__ == "__main__":
    # Rename stream type client and delete others
    target = PACKAGE_PATH / "client.py"
    raw_client_py = PACKAGE_PATH / "{{cookiecutter.stream_type|lower}}-client.py"
    raw_client_py.rename(target)

    for client_py in PACKAGE_PATH.rglob("*-client.py"):
        client_py.unlink()

    # Select appropriate tap.py based on stream type
    tap_target = PACKAGE_PATH / "tap.py"
    if "{{ cookiecutter.stream_type }}" == "SQL":
        sql_tap_py = PACKAGE_PATH / "sql-tap.py"
        sql_tap_py.rename(tap_target)
    else:
        non_sql_tap_py = PACKAGE_PATH / "non-sql-tap.py"
        non_sql_tap_py.rename(tap_target)

    # Clean up remaining tap template files
    for tap_py in PACKAGE_PATH.rglob("*-tap.py"):
        tap_py.unlink()

    if "{{ cookiecutter.stream_type }}" != "REST":
        shutil.rmtree(PACKAGE_PATH.joinpath("schemas"), ignore_errors=True)

    if "{{ cookiecutter.auth_method }}" not in ("OAuth2", "JWT"):
        PACKAGE_PATH.joinpath("auth.py").unlink()

    if "{{ cookiecutter.stream_type }}" == "SQL":
        PACKAGE_PATH.joinpath("streams.py").unlink()

    # Handle license selection
    license_choice = "{{ cookiecutter.license }}"
    if license_choice == "Apache-2.0":
        Path("LICENSE-Apache-2.0").rename("LICENSE")
        Path("LICENSE-MIT").unlink()
    elif license_choice == "MIT":
        Path("LICENSE-MIT").rename("LICENSE")
        Path("LICENSE-Apache-2.0").unlink()
    elif license_choice == "None":
        Path("LICENSE-Apache-2.0").unlink()
        Path("LICENSE-MIT").unlink()

    if "{{ cookiecutter.include_ci_files }}" != "GitHub":
        shutil.rmtree(".github")

    if "{{ cookiecutter.ide }}" != "VSCode":
        shutil.rmtree(".vscode", ignore_errors=True)
