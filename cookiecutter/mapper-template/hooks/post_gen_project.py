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

    if {{ cookiecutter.include_agents_md }}:
        # Generate both AGENTS.md (for ecosystem) and CLAUDE.md (for Claude Code)
        agents_md = Path("AGENTS.md")
        if agents_md.exists():
            shutil.copy2(agents_md, "CLAUDE.md")
    else:
        Path("AGENTS.md").unlink(missing_ok=True)
        Path("CLAUDE.md").unlink(missing_ok=True)
