#!/usr/bin/env python
from pathlib import Path
import shutil


BASE_PATH = Path("{{cookiecutter.library_name}}")


if __name__ == "__main__":
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
        shutil.rmtree(Path(".github"))

    if "{{ cookiecutter.ide }}" != "VSCode":
        shutil.rmtree(".vscode", ignore_errors=True)
