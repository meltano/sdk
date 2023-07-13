"""Nox configuration."""

from __future__ import annotations

import glob
import os
import shutil
import sys
import tempfile
from pathlib import Path
from textwrap import dedent

import nox

try:
    from nox_poetry import Session, session
except ImportError:
    message = f"""\
    Nox failed to import the 'nox-poetry' package.
    Please install it using the following command:
    {sys.executable} -m pip install nox-poetry"""
    raise SystemExit(dedent(message)) from None

RUFF_OVERRIDES = """\
extend = "./pyproject.toml"
extend-ignore = ["TD002", "TD003"]
"""

package = "singer_sdk"
python_versions = ["3.11", "3.10", "3.9", "3.8"]
main_python_version = "3.10"
locations = "singer_sdk", "tests", "noxfile.py", "docs/conf.py"
nox.options.sessions = (
    "mypy",
    "tests",
    "doctest",
    "test_cookiecutter",
)
test_dependencies = [
    "coverage[toml]",
    "pytest",
    "pytest-snapshot",
    "pytest-durations",
    "freezegun",
    "pandas",
    "pyarrow",
    "requests-mock",
    # Cookiecutter tests
    "black",
    "cookiecutter",
    "PyYAML",
    "darglint",
    "flake8",
    "flake8-annotations",
    "flake8-docstrings",
    "mypy",
]


@session(python=python_versions)
def mypy(session: Session) -> None:
    """Check types with mypy."""
    args = session.posargs or ["singer_sdk"]
    session.install(".")
    session.install(
        "mypy",
        "pytest",
        "importlib-resources",
        "sqlalchemy2-stubs",
        "types-jsonschema",
        "types-python-dateutil",
        "types-pytz",
        "types-requests",
        "types-simplejson",
        "types-PyYAML",
    )
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@session(python=python_versions)
def tests(session: Session) -> None:
    """Execute pytest tests and compute coverage."""
    session.install(".[s3]")
    session.install(*test_dependencies)

    try:
        session.run(
            "coverage",
            "run",
            "--parallel",
            "-m",
            "pytest",
            "-v",
            "--durations=10",
            *session.posargs,
            env={
                "SQLALCHEMY_WARN_20": "1",
            },
        )
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@session(python=main_python_version)
def update_snapshots(session: Session) -> None:
    """Update pytest snapshots."""
    args = session.posargs or ["-m", "snapshot"]

    session.install(".")
    session.install(*test_dependencies)
    session.run("pytest", "--snapshot-update", *args)


@session(python=python_versions)
def doctest(session: Session) -> None:
    """Run examples with xdoctest."""
    if session.posargs:
        args = [package, *session.posargs]
    else:
        args = [package]
        if "FORCE_COLOR" in os.environ:
            args.append("--xdoctest-colored=1")

    session.install(".")
    session.install("pytest", "xdoctest[colors]")
    session.run("pytest", "--xdoctest", *args)


@session(python=main_python_version)
def coverage(session: Session) -> None:
    """Generate coverage report."""
    args = session.posargs or ["report", "-m"]

    session.install("coverage[toml]")

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@session(name="docs", python=main_python_version)
def docs(session: Session) -> None:
    """Build the documentation."""
    args = session.posargs or ["docs", "build", "-W"]
    if not session.posargs and "FORCE_COLOR" in os.environ:
        args.insert(0, "--color")

    session.install(".[docs]")

    build_dir = Path("build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-build", *args)


@session(name="docs-serve", python=main_python_version)
def docs_serve(session: Session) -> None:
    """Build the documentation."""
    args = session.posargs or [
        "--open-browser",
        "--watch",
        ".",
        "--ignore",
        "**/.nox/*",
        "docs",
        "build",
        "-W",
    ]
    session.install(".[docs]")

    build_dir = Path("build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-autobuild", *args)


@nox.parametrize("replay_file_path", glob.glob("./e2e-tests/cookiecutters/*.json"))
@session(python=main_python_version)
def test_cookiecutter(session: Session, replay_file_path) -> None:
    """Uses the tap template to build an empty cookiecutter.

    Runs the lint task on the created test project.
    """
    cc_build_path = tempfile.gettempdir()
    folder_base_path = "./cookiecutter"

    target_folder = (
        "tap-template"
        if Path(replay_file_path).name.startswith("tap")
        else "target-template"
    )
    tap_template = Path(folder_base_path + "/" + target_folder).resolve()
    replay_file = Path(replay_file_path).resolve()

    if not Path(tap_template).exists():
        return

    if not Path(replay_file).is_file():
        return

    sdk_dir = Path(Path(tap_template).parent).parent
    cc_output_dir = Path(replay_file_path).name.replace(".json", "")
    cc_test_output = cc_build_path + "/" + cc_output_dir

    if Path(cc_test_output).exists():
        session.run("rm", "-fr", cc_test_output, external=True)

    session.install(".")
    session.install("cookiecutter", "pythonsed")

    session.run(
        "cookiecutter",
        "--replay-file",
        str(replay_file),
        str(tap_template),
        "-o",
        cc_build_path,
    )
    session.chdir(cc_test_output)

    with Path("ruff.toml").open("w") as ruff_toml:
        ruff_toml.write(RUFF_OVERRIDES)

    session.run(
        "pythonsed",
        "-i.bak",
        's|singer-sdk =.*|singer-sdk = \\{ path = "'
        + str(sdk_dir)
        + '", develop = true \\}|',
        "pyproject.toml",
    )
    session.run("poetry", "lock", external=True)
    session.run("poetry", "install", external=True)

    session.run("git", "init", external=True)
    session.run("git", "add", ".", external=True)
    session.run("pre-commit", "run", "--all-files", external=True)
