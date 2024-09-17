from __future__ import annotations

import csv
import typing as t

from singer_sdk import Stream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.tap_base import Tap

SDC_META_FILEPATH = "_sdc_file"


class CSVStream(Stream):
    def __init__(
        self,
        tap: Tap,
        name: str | None = None,
        *,
        partitions: list[str] | None = None,
    ) -> None:
        # TODO(edgarmondragon): Build schema from CSV file.
        schema = {
            "type": ["object"],
            "properties": {
                SDC_META_FILEPATH: {"type": "string"},
            },
            "required": [],
            "additionalProperties": {"type": "string"},
        }
        super().__init__(tap, schema, name)
        self._partitions = partitions or []

    @property
    def partitions(self) -> list[Context]:
        return self._partitions

    def get_records(  # noqa: PLR6301
        self,
        context: Context | None,
    ) -> t.Iterable[Record | tuple[Record, Context | None]]:
        path: str = context[SDC_META_FILEPATH]

        # Make these configurable.
        delimiter = ","
        quotechar = '"'
        escapechar = None
        doublequote = True
        lineterminator = "\r\n"

        # TODO: Use filesytem-specific file open method.
        with open(path, encoding="utf-8") as file:  # noqa: PTH123
            reader = csv.DictReader(
                file,
                delimiter=delimiter,
                quotechar=quotechar,
                escapechar=escapechar,
                doublequote=doublequote,
                lineterminator=lineterminator,
            )
            yield from reader
