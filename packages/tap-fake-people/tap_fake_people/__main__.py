"""Entry point for the tap."""

from __future__ import annotations

from tap_fake_people.tap import TapFakePeople

TapFakePeople.cli()
