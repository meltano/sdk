from __future__ import annotations

import random
import string
from typing import Iterable

from singer_sdk import typing as th
from singer_sdk.streams import Stream


class HostilePropertyNamesStream(Stream):
    """
    A stream with property names that are not compatible as unescaped identifiers
    in common DBMS systems.
    """

    name = "hostile_property_names_stream"
    schema = th.PropertiesList(
        th.Property("name with spaces", th.StringType),
        th.Property("NameIsCamelCase", th.StringType),
        th.Property("name-with-dashes", th.StringType),
        th.Property("Name-with-Dashes-and-Mixed-cases", th.StringType),
        th.Property("5name_starts_with_number", th.StringType),
        th.Property("name_with_emoji_😈", th.StringType),
    ).to_dict()

    @staticmethod
    def get_random_lowercase_string():
        return "".join(random.choice(string.ascii_lowercase) for _ in range(10))

    def get_records(self, context: dict | None) -> Iterable[dict | tuple[dict, dict]]:
        return (
            {
                "name with spaces": self.get_random_lowercase_string(),
                "NameIsCamelCase": self.get_random_lowercase_string(),
                "name-with-dashes": self.get_random_lowercase_string(),
                "Name-with-Dashes-and-Mixed-cases": self.get_random_lowercase_string(),
                "5name_starts_with_number": self.get_random_lowercase_string(),
                "name_with_emoji_😈": self.get_random_lowercase_string(),
            }
            for _ in range(10)
        )
