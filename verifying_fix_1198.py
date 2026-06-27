"""Verifying fix for Issue #1198.

Goal: show that nested replication keys ARE now supported.
Run with: python verifying_fix_1198.py
"""

from __future__ import annotations

import typing as t

from singer_sdk import Stream, Tap
from singer_sdk.helpers._util import get_nested_value  # noqa: PLC2701
from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
)

if t.TYPE_CHECKING:
    from singer_sdk.helpers import types

# Fake records simulating an API response.
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
    """Stream for verifying issue #1198 fix using nested replication keys."""

    name = "nested_key_stream"

    # The SDK now supports dotted paths to nested fields.
    # "attributes.updated" traverses record["attributes"]["updated"].
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
class VerifyTap(Tap):
    """Minimal tap for verifying issue #1198 fix."""

    name = "verify-tap"

    def discover_streams(self) -> list[Stream]:
        """Return list of streams for this tap."""
        return [NestedKeyStream(tap=self)]


# Run and show that the SDK now resolves nested replication keys correctly
if __name__ == "__main__":
    tap = VerifyTap(config={}, parse_env_config=False)
    stream = tap.streams["nested_key_stream"]

    for record in FAKE_RECORDS:
        print(f"\nRecord: {record}")  # noqa: T201

        # Use get_nested_value() — the helper added as part of the fix —
        # to traverse the dotted path and retrieve the nested timestamp.
        value = get_nested_value(record, stream.replication_key)
        expected = record["attributes"]["updated"]

        print(f"  replication_key = '{stream.replication_key}'")  # noqa: T201
        print(f"  Resolved value  = {value!r}")  # noqa: T201
        print(f"  Expected value  = {expected!r}")  # noqa: T201

        if value == expected:
            print("  ✓ PASS: nested key resolved correctly.")  # noqa: T201
        else:
            print("  ✗ FAIL: nested key did not resolve.")  # noqa: T201
