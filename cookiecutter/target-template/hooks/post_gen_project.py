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

    # Choose the appropriate sinks file based on serialization method
    serialization_method = "{{ cookiecutter.serialization_method }}"

    if serialization_method == "Per record":
        source_file = BASE_PATH / "sinks_record.py"
    elif serialization_method == "Per batch":
        source_file = BASE_PATH / "sinks_batch.py"
    elif serialization_method == "SQL":
        source_file = BASE_PATH / "sinks_sql.py"
    else:
        valid_methods = ["Per record", "Per batch", "SQL"]
        msg = (
            f"Unknown serialization method: {serialization_method}. "
            f"Valid methods are: {', '.join(valid_methods)}"
        )
        raise ValueError(msg)

    # Copy the appropriate sinks file to sinks.py
    target_file = BASE_PATH / "sinks.py"
    shutil.copy2(source_file, target_file)

    # Clean up the unused sink files
    for template in ["sinks_record.py", "sinks_batch.py", "sinks_sql.py"]:
        (BASE_PATH / template).unlink(missing_ok=True)
