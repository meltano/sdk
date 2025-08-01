"""Nox configuration."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import nox

nox.needs_version = ">=2025.2.9"
nox.options.default_venv_backend = "uv"

RUFF_OVERRIDES = """\
extend = "./pyproject.toml"

[lint]
extend-ignore = ["TD002", "TD003", "FIX002"]
"""

COOKIECUTTER_REPLAY_FILES = list(Path("./e2e-tests/cookiecutters").glob("*.json"))
PYPROJECT = nox.project.load_toml()
UV_SYNC_COMMAND = (
    "uv",
    "sync",
    "--locked",
    "--no-dev",
)

package = "singer_sdk"
main_python = Path(".python-version").read_text(encoding="utf-8").rstrip()
python_versions = nox.project.python_versions(PYPROJECT)
locations = "singer_sdk", "tests", "noxfile.py", "docs/conf.py"
nox.options.sessions = [
    f"mypy-{main_python}",
    f"tests-{main_python}",
    "doctest",
    "deps",
    "docs",
]


def _install_env(session: nox.Session) -> dict[str, str]:
    """Get the environment variables for the install command.

    Args:
        session: The Nox session.

    Returns:
        The environment variables.
    """
    env = {
        "UV_PROJECT_ENVIRONMENT": session.virtualenv.location,
    }
    if isinstance(session.python, str):
        env["UV_PYTHON"] = session.python

    return env


@nox.session(python=[python_versions[0], main_python])
def mypy(session: nox.Session) -> None:
    """Check types with mypy."""
    default_locations = [
        "packages",
        "samples",
        "singer_sdk",
    ]
    args = session.posargs or default_locations
    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=packages",
        "--group=typing",
        "--all-extras",
        env=_install_env(session),
    )
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


def _run_pytest(session: nox.Session, *pytest_args: str, coverage: bool = True) -> None:
    """Run pytest with the given arguments."""
    args = ["pytest", *pytest_args]

    if coverage:
        args = ["coverage", "run", "--parallel", "-m", *args]

    try:
        session.run(*args)
    finally:
        if coverage and session.interactive:
            session.notify("coverage", posargs=[])


@nox.session(python=python_versions, tags=["test"])
def tests(session: nox.Session) -> None:
    """Execute pytest tests and compute coverage."""
    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=testing",
        "--extra=faker",
        "--extra=jwt",
        "--extra=s3",
        env=_install_env(session),
    )

    _run_pytest(
        session,
        "--durations=10",
        "--benchmark-skip",
        "--ignore=tests/contrib",
        "--ignore=tests/benchmarks",
        "--ignore=tests/external",
        "--ignore=tests/packages",
        *session.posargs,
    )


@nox.session(
    name="test-external",
    python=[python_versions[0], main_python],
    tags=["test"],
)
def test_external(session: nox.Session) -> None:
    """Execute pytest tests and compute coverage."""
    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=packages",
        "--group=testing",
        "--all-extras",
        env=_install_env(session),
    )

    _run_pytest(
        session,
        "--durations=10",
        "-m",
        "external",
        *session.posargs,
    )


@nox.session(
    name="test-contrib",
    python=[python_versions[0], main_python],
    tags=["test"],
)
def test_contrib(session: nox.Session) -> None:
    """Execute pytest tests and compute coverage."""
    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=testing",
        "--all-extras",
        env=_install_env(session),
    )

    _run_pytest(
        session,
        "--durations=10",
        "--benchmark-skip",
        "--ignore=tests/benchmarks",
        "--ignore=tests/external",
        "--ignore=tests/packages",
        "-m",
        "contrib",
        *session.posargs,
    )


@nox.session(
    name="test-packages",
    python=[python_versions[0], main_python],
    tags=["test"],
)
def test_packages(session: nox.Session) -> None:
    """Execute pytest tests and compute coverage."""
    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=packages",
        "--group=testing",
        "--all-extras",
        env=_install_env(session),
    )

    _run_pytest(
        session,
        "--durations=10",
        "--benchmark-skip",
        "--ignore=tests/benchmarks",
        "--ignore=tests/contrib",
        "--ignore=tests/external",
        "-m",
        "packages",
        *session.posargs,
    )


@nox.session(name="test-lowest", python=python_versions[0], tags=["test"])
def test_lowest_requirements(session: nox.Session) -> None:
    """Test the package with the lowest dependency versions."""
    install_env = _install_env(session)

    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=testing",
        "--all-extras",
        env=install_env,
    )
    tmpdir = session.create_tmp()

    session.run_install(
        "uv",
        "pip",
        "compile",
        "pyproject.toml",
        f"--python={session.python}",
        "--group=testing",
        "--all-extras",
        "--universal",
        "--resolution=lowest-direct",
        f"-o={tmpdir}/requirements.txt",
        env=install_env,
    )

    session.install("-r", f"{tmpdir}/requirements.txt", env=install_env)
    _run_pytest(
        session,
        "--benchmark-skip",
        "--ignore=tests/contrib",
        "--ignore=tests/benchmarks",
        "--ignore=tests/external",
        "--ignore=tests/packages",
        *session.posargs,
        coverage=False,
    )


@nox.session(tags=["test"])
def benches(session: nox.Session) -> None:
    """Run benchmarks."""
    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=testing",
        "--all-extras",
        env=_install_env(session),
    )
    _run_pytest(
        session,
        "--benchmark-only",
        "--benchmark-json=output.json",
        "--ignore=tests/contrib",
        "--ignore=tests/external",
        "--ignore=tests/packages",
        *session.posargs,
        coverage=False,
    )


@nox.session()
def coverage(session: nox.Session) -> None:
    """Generate coverage report."""
    args = session.posargs or ["report", "-m"]

    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=testing",
        env=_install_env(session),
    )

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@nox.session(name="deps")
def dependencies(session: nox.Session) -> None:
    """Check issues with dependencies."""
    extras = [
        "faker",
        "jwt",
        "msgspec",
        "parquet",
        "s3",
        "ssh",
        "testing",
    ]

    session.install("deptry")
    session.run_install(
        *UV_SYNC_COMMAND,
        "--inexact",
        *(f"--extra={extra}" for extra in extras),
        env=_install_env(session),
    )
    session.install("deptry")
    session.run("deptry", "singer_sdk", *session.posargs)


@nox.session(name="snap")
def update_snapshots(session: nox.Session) -> None:
    """Update pytest snapshots."""
    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=testing",
        "--group=packages",
        "--all-extras",
        env=_install_env(session),
    )
    _run_pytest(
        session,
        "--snapshot-update",
        "-m",
        "snapshot",
        *session.posargs,
        coverage=False,
    )


@nox.session()
def doctest(session: nox.Session) -> None:
    """Run examples with xdoctest."""
    if session.posargs:
        args = [package, *session.posargs]
    else:
        args = [package]
        if "FORCE_COLOR" in os.environ:
            args.append("--xdoctest-colored=1")

    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=testing",
        env=_install_env(session),
    )
    session.run("pytest", "--xdoctest", *args)


@nox.session(name="docs")
def docs(session: nox.Session) -> None:
    """Build the documentation."""
    args = session.posargs or ["docs", "build", "-W"]
    if not session.posargs and "FORCE_COLOR" in os.environ:
        args.insert(0, "--color")

    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=docs",
        env=_install_env(session),
    )

    build_dir = Path("build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-build", *args)


@nox.session(name="docs-serve")
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
    session.install("sphinx-autobuild")
    session.run_install(
        *UV_SYNC_COMMAND,
        "--inexact",
        "--group=docs",
        env=_install_env(session),
    )

    build_dir = Path("build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-autobuild", *args)


@nox.parametrize("replay_file_path", COOKIECUTTER_REPLAY_FILES)
@nox.session()
def templates(session: nox.Session, replay_file_path: Path) -> None:
    """Uses the tap template to build an empty cookiecutter.

    Runs the lint task on the created test project.
    """
    cc_build_path = Path("./cookiecutter-output")
    folder_base_path = Path("./cookiecutter")
    replay_file = replay_file_path.resolve()

    if replay_file.name.startswith("tap"):
        folder = "tap-template"
    elif replay_file.name.startswith("target"):
        folder = "target-template"
    else:
        folder = "mapper-template"
    template = folder_base_path.joinpath(folder)

    if not template.exists():
        return

    if not replay_file.is_file():
        return

    sdk_dir = template.parent.parent.resolve()
    cc_output_dir = replay_file.name.replace(".json", "")
    cc_test_output = cc_build_path.joinpath(cc_output_dir)

    if cc_test_output.exists():
        session.run("rm", "-fr", str(cc_test_output), external=True)

    session.run(
        "uvx",
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

    # Use the local singer-sdk
    session.run("uv", "add", f"singer-sdk @ {sdk_dir}")

    # Check that the project can be installed for development
    session.run("uv", "lock")
    session.run("uv", "sync")

    # Check that the project can be built for distribution
    session.run("uv", "build")
    session.run("uvx", "twine", "check", "dist/*")

    session.run("git", "init", "-b", "main", external=True)
    session.run("git", "add", ".", external=True)
    session.run("uvx", "pre-commit", "run", "--all-files")
    session.run("uvx", "--with=tox-uv", "tox", "-e=typing")


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
        "check",
        "--no-color",
        "singer_sdk",
    ]

    if session.posargs:
        args.append(f"-a={session.posargs[0]}")

    if "GITHUB_ACTIONS" in os.environ:
        args.append("-f=github")

    session.run("uvx", "griffe", *args, external=True)
