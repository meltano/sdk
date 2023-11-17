"""Nox configuration."""

from __future__ import annotations

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
extend-ignore = ["TD002", "TD003", "FIX002"]
"""

COOKIECUTTER_REPLAY_FILES = list(Path("./e2e-tests/cookiecutters").glob("*.json"))

package = "singer_sdk"
python_versions = ["3.11", "3.10", "3.9", "3.8", "3.7"]
main_python_version = "3.10"
locations = "singer_sdk", "tests", "noxfile.py", "docs/conf.py"
nox.options.sessions = (
    "mypy",
    "tests",
    "benches",
    "doctest",
    "test_cookiecutter",
)
test_dependencies = [
    "coverage[toml]",
    "duckdb",
    "duckdb-engine",
    "pytest",
    "pytest-benchmark",
    "pytest-durations",
    "pytest-snapshot",
    "pyarrow",
    "requests-mock",
    "time-machine",
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

    sqlalchemy_version = os.environ.get("SQLALCHEMY_VERSION")
    if sqlalchemy_version:
        # Bypass nox-poetry use of --constraint so we can install a version of
        # SQLAlchemy that doesn't match what's in poetry.lock.
        session.poetry.session.install(  # type: ignore[attr-defined]
            f"sqlalchemy=={sqlalchemy_version}",
        )

    try:
        session.run(
            "coverage",
            "run",
            "--parallel",
            "-m",
            "pytest",
            "-v",
            "--durations=10",
            "--benchmark-skip",
            *session.posargs,
        )
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@session(python=main_python_version)
def benches(session: Session) -> None:
    """Run benchmarks."""
    session.install(".[s3]")
    session.install(*test_dependencies)
    sqlalchemy_version = os.environ.get("SQLALCHEMY_VERSION")
    if sqlalchemy_version:
        # Bypass nox-poetry use of --constraint so we can install a version of
        # SQLAlchemy that doesn't match what's in poetry.lock.
        session.poetry.session.install(  # type: ignore[attr-defined]
            f"sqlalchemy=={sqlalchemy_version}",
        )
    session.run(
        "pytest",
        "--benchmark-only",
        "--benchmark-json=output.json",
        *session.posargs,
    )


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


@nox.parametrize("replay_file_path", COOKIECUTTER_REPLAY_FILES)
@session(python=main_python_version)
def test_cookiecutter(session: Session, replay_file_path) -> None:
    """Uses the tap template to build an empty cookiecutter.

    Runs the lint task on the created test project.
    """
    cc_build_path = tempfile.gettempdir()
    folder_base_path = "./cookiecutter"

    if Path(replay_file_path).name.startswith("tap"):
        folder = "tap-template"
    elif Path(replay_file_path).name.startswith("target"):
        folder = "target-template"
    else:
        folder = "mapper-template"
    template = Path(folder_base_path + "/" + folder).resolve()
    replay_file = Path(replay_file_path).resolve()

    if not Path(template).exists():
        return

    if not Path(replay_file).is_file():
        return

    sdk_dir = Path(Path(template).parent).parent
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
        str(template),
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


@session(name="version-bump")
def version_bump(session: Session) -> None:
    """Run commitizen."""
    session.install(
        "commitizen",
        "commitizen-version-bump @ git+https://github.com/meltano/commitizen-version-bump.git@main",
    )
    default_args = [
        "--changelog",
        "--files-only",
        "--check-consistency",
        "--changelog-to-stdout",
    ]
    args = session.posargs or default_args

    session.run(
        "cz",
        "bump",
        *args,
    )
