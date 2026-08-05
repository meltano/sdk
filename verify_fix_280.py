"""Reproduction script for issue #280: taps do not continue past partition errors."""

from __future__ import annotations

import typing as t

import singer_sdk.typing as th
from singer_sdk import Stream, Tap
from singer_sdk.exceptions import EndOfStreamError


class BrokenStream(Stream):
    """A stream that simulates a persistent error on one partition."""

    name = "broken_stream"
    schema = th.PropertiesList(th.Property("id", th.IntegerType)).to_dict()
    primary_keys: t.ClassVar[list[str]] = ["id"]

    @property
    def partitions(self) -> list[dict]:  # type: ignore[override]
        """Return partitions simulating a multi-repo tap."""  # ruff: ignore[property-docstring-starts-with-verb]
        return [
            {"repo": "good-repo"},
            {"repo": "broken-repo"},
            {"repo": "another-good-repo"},
        ]

    def get_records(  # ruff: ignore[no-self-use]
        self,
        context: dict | None,
    ) -> t.Generator[dict, None, None]:
        """Yield records, raising on the broken partition."""  # noqa: DOC501
        if context and context["repo"] == "broken-repo":
            msg = f"Simulated persistent error for repo: {context['repo']}"
            raise EndOfStreamError(msg)
        yield {"id": 1}


class BrokenTap(Tap):
    """A tap that surfaces the missing error-handling behavior."""

    name = "tap-broken"

    def discover_streams(self) -> list[Stream]:
        """Return the list of streams."""
        return [BrokenStream(self)]


if __name__ == "__main__":
    BrokenTap.cli()
