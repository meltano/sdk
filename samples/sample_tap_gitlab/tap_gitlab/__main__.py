"""Entry point for the tap."""

from __future__ import annotations

from tap_gitlab.tap import TapGitlab

TapGitlab.cli()
