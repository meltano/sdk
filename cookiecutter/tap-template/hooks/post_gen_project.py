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

    if "{{ cookiecutter.stream_type }}" != "REST":
        shutil.rmtree(PACKAGE_PATH.joinpath("schemas"), ignore_errors=True)

    if "{{ cookiecutter.auth_method }}" not in ("OAuth2", "JWT"):
        PACKAGE_PATH.joinpath("auth.py").unlink()

    if "{{ cookiecutter.stream_type }}" == "SQL":
        PACKAGE_PATH.joinpath("streams.py").unlink()

    if "{{ cookiecutter.license }}" == "None":
        Path("LICENSE").unlink()

    if "{{ cookiecutter.include_ci_files }}" != "GitHub":
        shutil.rmtree(".github")

    if "{{ cookiecutter.ide }}" != "VSCode":
        shutil.rmtree(".vscode", ignore_errors=True)
