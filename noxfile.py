"""Nox configuration."""

from __future__ import annotations

import os
import shutil
import sys
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

package = "singer_sdk"
python_versions = ["3.10", "3.9", "3.8", "3.7"]
main_python_version = "3.10"
locations = "singer_sdk", "tests", "noxfile.py", "docs/conf.py"
nox.options.sessions = (
    "mypy",
    "tests",
    "doctest",
)


@session(python=python_versions)
def mypy(session: Session) -> None:
    """Check types with mypy."""
    args = session.posargs or ["singer_sdk"]
    session.install(".")
    session.install(
        "mypy",
        "sqlalchemy2-stubs",
        "types-python-dateutil",
        "types-requests",
    )
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@session(python=python_versions)
def tests(session: Session) -> None:
    """Execute pytest tests and compute coverage."""
    session.install(".")
    session.install(
        "coverage[toml]",
        "pytest",
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
    )

    try:
        session.run(
            "coverage",
            "run",
            "--parallel",
            "-m",
            "pytest",
            "-x",
            "-v",
            *session.posargs,
        )
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


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
