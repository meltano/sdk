"""Nox configuration."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import nox

nox.needs_version = ">=2024.4.15"
nox.options.default_venv_backend = "uv|virtualenv"

RUFF_OVERRIDES = """\
extend = "./pyproject.toml"

[lint]
extend-ignore = ["TD002", "TD003", "FIX002"]
"""

COOKIECUTTER_REPLAY_FILES = list(Path("./e2e-tests/cookiecutters").glob("*.json"))

package = "singer_sdk"
python_versions = [
    "3.13",
    "3.12",
    "3.11",
    "3.10",
    "3.9",
]
main_python_version = "3.13"
locations = "singer_sdk", "tests", "noxfile.py", "docs/conf.py"
nox.options.sessions = (
    "mypy",
    "tests",
    "benches",
    "doctest",
    "test_cookiecutter",
)

dependency_groups = nox.project.load_toml("pyproject.toml")["dependency-groups"]
test_dependencies: list[str] = dependency_groups["dev"]
typing_dependencies: list[str] = dependency_groups["typing"]


@nox.session(python=main_python_version)
def mypy(session: nox.Session) -> None:
    """Check types with mypy."""
    args = session.posargs or ["singer_sdk"]
    session.install(".[faker,jwt,parquet,s3,testing]")
    session.install(*typing_dependencies)
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@nox.session(python=python_versions)
def tests(session: nox.Session) -> None:
    """Execute pytest tests and compute coverage."""
    extras = [
        "faker",
        "jwt",
        "parquet",
        "s3",
    ]

    session.install(f".[{','.join(extras)}]")
    session.install(*test_dependencies)

    env = {"COVERAGE_CORE": "sysmon"} if session.python == "3.12" else {}

    try:
        session.run(
            "coverage",
            "run",
            "--parallel",
            "-m",
            "pytest",
            "--durations=10",
            "--benchmark-skip",
            *session.posargs,
            env=env,
        )
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@nox.session(python=main_python_version)
def benches(session: nox.Session) -> None:
    """Run benchmarks."""
    session.install(".[jwt,s3]")
    session.install(*test_dependencies)
    session.run(
        "pytest",
        "--benchmark-only",
        "--benchmark-json=output.json",
        *session.posargs,
    )


@nox.session(name="deps", python=main_python_version)
def dependencies(session: nox.Session) -> None:
    """Check issues with dependencies."""
    session.install(".[docs,faker,jwt,parquet,s3,ssh,testing]")
    session.install("deptry")
    session.run("deptry", "singer_sdk", *session.posargs)


@nox.session(python=main_python_version)
def update_snapshots(session: nox.Session) -> None:
    """Update pytest snapshots."""
    args = session.posargs or ["-m", "snapshot"]

    session.install(".[faker,jwt,parquet]")
    session.install(*test_dependencies)
    session.run("pytest", "--snapshot-update", *args)


@nox.session(python=python_versions)
def doctest(session: nox.Session) -> None:
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


@nox.session(python=main_python_version)
def coverage(session: nox.Session) -> None:
    """Generate coverage report."""
    args = session.posargs or ["report", "-m"]

    session.install("coverage[toml]")

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@nox.session(name="docs", python=main_python_version)
def docs(session: nox.Session) -> None:
    """Build the documentation."""
    args = session.posargs or ["docs", "build", "-W"]
    if not session.posargs and "FORCE_COLOR" in os.environ:
        args.insert(0, "--color")

    session.install(".[docs]")

    build_dir = Path("build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-build", *args)


@nox.session(name="docs-serve", python=main_python_version)
def docs_serve(session: nox.Session) -> None:
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
    session.install(".[docs]", "sphinx-autobuild")

    build_dir = Path("build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-autobuild", *args)


@nox.parametrize("replay_file_path", COOKIECUTTER_REPLAY_FILES)
@nox.session(python=main_python_version)
def test_cookiecutter(session: nox.Session, replay_file_path: str) -> None:
    """Uses the tap template to build an empty cookiecutter.

    Runs the lint task on the created test project.
    """
    cc_build_path = Path(tempfile.gettempdir())
    folder_base_path = Path("./cookiecutter")
    replay_file = Path(replay_file_path).resolve()

    if replay_file.name.startswith("tap"):
        folder = "tap-template"
    elif replay_file.name.startswith("target"):
        folder = "target-template"
    else:
        folder = "mapper-template"
    template = folder_base_path.joinpath(folder).resolve()

    if not template.exists():
        return

    if not replay_file.is_file():
        return

    sdk_dir = template.parent.parent
    cc_output_dir = replay_file.name.replace(".json", "")
    cc_test_output = cc_build_path.joinpath(cc_output_dir)

    if cc_test_output.exists():
        session.run("rm", "-fr", str(cc_test_output), external=True)

    session.install(".")
    session.install("cookiecutter", "pythonsed")

    session.run(
        "cookiecutter",
        "--replay-file",
        str(replay_file),
        str(template),
        "-o",
        str(cc_build_path),
    )
    session.chdir(cc_test_output)

    with Path("ruff.toml").open("w", encoding="utf-8") as ruff_toml:
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

    session.run("git", "init", "-b", "main", external=True)
    session.run("git", "add", ".", external=True)
    session.run("pre-commit", "run", "--all-files", external=True)


@nox.session(name="version-bump")
def version_bump(session: nox.Session) -> None:
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


@nox.session(name="api")
def api_changes(session: nox.Session) -> None:
    """Check for API changes."""
    args = [
        "griffe",
        "check",
        "singer_sdk",
    ]

    if session.posargs:
        args.append(f"-a={session.posargs[0]}")

    if "GITHUB_ACTIONS" in os.environ:
        args.append("-f=github")

    session.run(*args, external=True)
