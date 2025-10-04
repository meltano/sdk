"""Tap for fake people."""

from __future__ import annotations

import sys
import typing as t

from faker import Faker

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.singerlib import SingerMessageType

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.singerlib import Message


class FakePeopleStream(Stream):
    """Stream for fake people."""

    name = "people"
    primary_keys = ("id",)
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("email", th.StringType, required=True),
        th.Property("phone", th.StringType, required=True),
        th.Property("address", th.StringType, required=True),
    ).to_dict()

    @override
    def get_records(self, context: Context | None) -> Iterable[Record]:
        faker = Faker()
        faker.seed_instance(42)
        for i in range(100):
            yield {
                "id": i,
                "name": faker.name(),
                "email": faker.email(),
                "phone": faker.phone_number(),
                "address": faker.address(),
            }


class TapFakePeople(Tap):
    """Tap for fake people."""

    name = "tap-fake-people"

    @override  # type: ignore[misc]
    def write_message(self, message: Message) -> None:
        if message.type == SingerMessageType.STATE:
            return
        super().write_message(message)

    @override
    def discover_streams(self) -> Sequence[Stream]:
        return [FakePeopleStream(self)]
