# Contributing to the Singer SDK

## Local Developer Setup

1. If you are using VS Code, make sure you have also installed the `Python` extension.
2. Ensure you have the correct test library, formatters, and linters installed:
    - `pipx install poetry`
    - `pipx install pytest`
    - `pipx install black`
    - `pipx install flake8`
    - `pipx install pydocstyle`
3. Configure Linting and Formatting Settings:
    - We use `pytest` for testing (`poetry run pytest`)
    - We use `black`, `flake8`, and `pydocstyle` as CI linting tests.
    - Linting with `mypy` will become a CI test in the future.
    - The project-wide max line length is `89`.
    - In the future we will add support for linting
      [pre-commit hooks](https://gitlab.com/meltano/singer-sdk/-/issues/12) as well.
4. Set intepreter to match poetry's virtualenv:
    - Run `poetry install` from the project root.
    - Run `poetry shell` and copy the path from command output.
    - In VS Code, run `Python: Select intepreter` and paste the intepreter path when prompted.
