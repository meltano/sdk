"""Check for public API breakages, resolving aliases across the `singer` package.

`griffecli check` hardcodes `resolve_external=None` for its `check` subcommand, so
it never loads the `singer` package (`packages/meltano-singer-python`) to resolve
base classes like `GenericSingerReader`/`GenericSingerWriter` that `singer_sdk`
classes inherit from. That causes every inherited member (e.g. on
`singer_sdk.contrib.msgspec.MsgSpecReader`) to be reported as "removed", even
though nothing about the public API changed.

This script calls the same `griffe` primitives directly with
`resolve_external=True` to avoid those false positives.
"""  # ruff: ignore[implicit-namespace-package]

from __future__ import annotations

import argparse
import os
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys

import griffe

SEARCH_PATHS = [".", "packages/meltano-singer-python"]


def _latest_tag() -> str:
    return subprocess.check_output(
        ["git", "describe", "--tags", "--abbrev=0"],  # noqa: S607
        text=True,
    ).strip()


def main() -> int:
    """Check for API changes and print a report.

    Returns:
        `0` if no breaking changes were found, `1` otherwise.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--against", default=None)
    args = parser.parse_args()

    against = args.against or _latest_tag()

    old_package = griffe.load_git(
        "singer_sdk",
        ref=against,
        search_paths=SEARCH_PATHS,
        resolve_aliases=True,
        resolve_external=True,
    )
    new_package = griffe.load(
        "singer_sdk",
        search_paths=SEARCH_PATHS,
        resolve_aliases=True,
        resolve_external=True,
    )

    style = (
        griffe.ExplanationStyle.GITHUB
        if "GITHUB_ACTIONS" in os.environ
        else griffe.ExplanationStyle.ONE_LINE
    )
    breakages = list(griffe.find_breaking_changes(old_package, new_package))
    for breakage in breakages:
        print(breakage.explain(style=style), file=sys.stderr)  # noqa: T201

    return 1 if breakages else 0


if __name__ == "__main__":
    sys.exit(main())
