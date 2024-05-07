#!/usr/bin/env python
from pathlib import Path
import shutil


BASE_PATH = Path("{{cookiecutter.library_name}}")


if __name__ == "__main__":
    if "{{ cookiecutter.license }}" != "Apache-2.0":
        Path("LICENSE").unlink()

    if "{{ cookiecutter.include_ci_files }}" != "GitHub":
        shutil.rmtree(Path(".github"))

    if "{{ cookiecutter.ide }}" != "VSCode":
        shutil.rmtree(".vscode", ignore_errors=True)
