from __future__ import annotations

import typing as t

from faker import Faker

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.singerlib import Message, SingerMessageType

if t.TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from singer_sdk.helpers.types import Record


class FakePeopleStream(Stream):
    name = "people"
    primary_keys = ("id",)
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("email", th.StringType, required=True),
        th.Property("phone", th.StringType, required=True),
        th.Property("address", th.StringType, required=True),
    ).to_dict()

    def get_records(self, context: dict) -> Iterable[Record]:  # noqa: ARG002, PLR6301
        faker = Faker()
        for i in range(100):
            yield {
                "id": i,
                "name": faker.name(),
                "email": faker.email(),
                "phone": faker.phone_number(),
                "address": faker.address(),
            }


class SampleTapFakePeople(Tap):
    name = "sample-tap-fake-people"

    def write_message(self, message: Message) -> None:
        if message.type == SingerMessageType.STATE:
            return
        super().write_message(message)

    def discover_streams(self) -> Sequence[Stream]:
        return [FakePeopleStream(self)]
