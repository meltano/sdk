from __future__ import annotations

import sys
import typing as t
from datetime import datetime, timedelta, timezone

from singer_sdk import Stream, Tap

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override


if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


class _BaseStream(Stream):
    max_records = 5
    fail_after = max_records + 1

    schema: t.ClassVar = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
        },
    }

    @override
    def get_records(self, context: Context | None):
        for i in range(1, self.max_records + 1):
            yield {"id": i, "name": f"All Good ({i=})"}

            if i >= self.fail_after:
                msg = "Something went wrong!"
                raise RuntimeError(msg)


class _IncrementalBaseStream(_BaseStream):
    replication_key = "updated_at"
    schema: t.ClassVar = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "updated_at": {"type": "string", "format": "date-time"},
        },
    }

    @override
    def get_records(self, context: Context | None):
        first_datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)
        start_date = self.get_starting_timestamp(context)
        for i in range(1, self.max_records + 1):
            rk = first_datetime + timedelta(minutes=i)
            if start_date and rk <= start_date:
                continue

            yield {
                "id": i,
                "name": f"All Good ({i=})",
                "updated_at": rk.isoformat(),
            }

            if i >= self.fail_after:
                msg = "Something went wrong!"
                raise RuntimeError(msg)


class StreamAllGood(_BaseStream):
    """Stream that always succeeds."""

    name = "all_good"


class StreamWithErrors(StreamAllGood):
    """Stream that raises errors."""

    name = "with_errors"
    fail_after = 3


class IncrementalAllGood(_IncrementalBaseStream):
    """Incremental stream that raises errors."""

    name = "incremental_all_good"


class IncrementalWithError(_IncrementalBaseStream):
    """Incremental stream that raises errors."""

    name = "incremental_with_errors"
    fail_after = 3


class IncrementalResumable(_IncrementalBaseStream):
    """Incremental stream that raises errors."""

    name = "incremental_resumable"
    fail_after = 3
    is_sorted = True


class ParentStream(StreamAllGood):
    """Parent stream that depends on the other streams."""

    name = "parent"

    @override
    def generate_child_contexts(self, record: Record, context: Context | None):
        yield {"parent_id": record["id"]}


class ChildStreamWithErrors(StreamWithErrors):
    """Child stream that raises errors."""

    name = "child_with_errors"
    schema: t.ClassVar = {
        "type": "object",
        "properties": {
            "parent_id": {"type": "integer"},
            "id": {"type": "integer"},
            "name": {"type": "string"},
        },
    }
    parent_stream_type = ParentStream


class ContinueOnErrorsTap(Tap):
    name = "tap"

    @override
    def discover_streams(self) -> list[Stream]:
        return [
            StreamAllGood(self),
            StreamWithErrors(self),
            IncrementalAllGood(self),
            IncrementalResumable(self),
            IncrementalWithError(self),
            ParentStream(self),
            ChildStreamWithErrors(self),
        ]


if __name__ == "__main__":
    ContinueOnErrorsTap.cli()
