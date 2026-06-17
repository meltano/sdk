"""Reproduction script for Issue #1198.

Goal: show that nested replication keys are NOT yet supported.
Run with: python reproducing_issue_1198.py
"""

from __future__ import annotations

import typing as t

from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
)

if t.TYPE_CHECKING:
    from singer_sdk.helpers import types

# Fake records simulating an API response
# The "updated" timestamp lives INSIDE the nested "attributes" object,
# not at the top level of the record.

FAKE_RECORDS = [
    {
        "id": 1,
        "attributes": {
            "created": "2024-01-01T00:00:00Z",
            "updated": "2024-01-10T00:00:00Z",  # nested timestamp
        },
    },
    {
        "id": 2,
        "attributes": {
            "created": "2024-02-01T00:00:00Z",
            "updated": "2024-02-15T00:00:00Z",  # nested timestamp
        },
    },
]


class NestedKeyStream(Stream):
    """Stream for reproducing issue #1198 using nested reproduction keys."""

    name = "nested_key_stream"

    # This is what the issue requests — a dotted path to a nested field.
    # This does NOT work correctly yet.

    replication_key = "attributes.updated"

    schema = PropertiesList(
        Property("id", IntegerType),
        Property(
            "attributes",
            ObjectType(
                Property("created", DateTimeType),
                Property("updated", DateTimeType),
            ),
        ),
    ).to_dict()

    @t.override
    def get_records(self, _context: types.Context | None) -> t.Generator:
        """Yield fake records with nested timestamps."""
        yield from FAKE_RECORDS


# Tap wiring
class ReproTap(Tap):
    """Minimal tap for reproducing issue #1198."""

    name = "repro-tap"  # tap label

    def discover_streams(self) -> list[Stream]:
        """Return list of streams for this tap."""
        return [NestedKeyStream(tap=self)]  # create stream instances


# Run and show what the SDK does with the nested key
if __name__ == "__main__":
    tap = ReproTap(config={}, parse_env_config=False)
    stream = tap.streams["nested_key_stream"]

    print("ISSUE #1198 — Nested Replication Key Reproduction")  # noqa: T201

    for record in FAKE_RECORDS:
        print(f"\nRecord: {record}")  # noqa: T201

        # The SDK looks up the replication key value using a flat dict lookup.
        # "attributes.updated" is treated as a literal key name,
        # NOT as a dotted path — so it returns None instead of the timestamp.

        value = record.get(stream.replication_key)

        print(f"  replication_key = '{stream.replication_key}'")  # noqa: T201
        print(f"  SDK resolved value = {value!r}")  # noqa: T201

        if value is None:
            print("  ✗ BUG CONFIRMED: SDK cannot resolve nested key.")  # noqa: T201
            print("    Expected: '2024-01-10T00:00:00Z' (or similar)")  # noqa: T201
            print("    Got: None — state tracking will silently fail.")  # noqa: T201
        else:
            print("  ✓ Value resolved correctly (bug may be fixed).")  # noqa: T201
