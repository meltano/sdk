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

    # Choose the appropriate sinks file based on serialization method
    serialization_method = "{{ cookiecutter.serialization_method }}"

    if serialization_method == "Per record":
        source_file = BASE_PATH / "sinks_record.py"
    elif serialization_method == "Per batch":
        source_file = BASE_PATH / "sinks_batch.py"
    elif serialization_method == "SQL":
        source_file = BASE_PATH / "sinks_sql.py"
    else:
        raise ValueError(f"Unknown serialization method: {serialization_method}")

    # Copy the appropriate sinks file to sinks.py
    target_file = BASE_PATH / "sinks.py"
    shutil.copy2(source_file, target_file)

    # Clean up the unused sink files
    (BASE_PATH / "sinks_record.py").unlink(missing_ok=True)
    (BASE_PATH / "sinks_batch.py").unlink(missing_ok=True)
    (BASE_PATH / "sinks_sql.py").unlink(missing_ok=True)
