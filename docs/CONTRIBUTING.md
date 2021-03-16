# Contributing to the Singer SDK

## Local Developer Setup

1. Install VS Code (or similar):
    - `choco install vscode` (Windows) or `brew install visual-studio-code` (Mac/Linux)
2. Install `Python` extension in VS Code.
3. Install `black` python auto-formatter, `pytest` test framework, and `poetry` package manager:
    - `pipx install black`
    - `pipx install pytest`
    - `pipx install poetry`
4. Configure VS Code Settings:
    ```json
    {
        //...
        "python.formatting.provider": "black",
        "python.testing.unittestEnabled": false,
        "python.testing.nosetestsEnabled": false,
        "python.testing.pytestEnabled": true,
        "python.testing.pytestArgs": [
            "tests"
        ],
        "[python]": {
            "editor.rulers": [89],
            "editor.formatOnSave": true,
            "editor.formatOnSaveMode": "file",
        }       
    }
    ```
5. Set intepreter to match poetry's virtualenv:
    - Run `poetry install` from the project root.
    - Run `poetry shell` and copy the path from command output.
    - In VS Code, run `Python: Select intepreter` and paste the intepreter path when prompted.
