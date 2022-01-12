"""Defines the flattening function including the Flattener class."""

from typing import List, Optional
from singer_sdk.mapper import StreamMap

from singer_sdk.helpers._flattening import flatten_schema, flatten_record


class Flattener(StreamMap):
    """A stream map which performs the flattening role."""

    def __init__(
        self,
        stream_alias: str,
        raw_schema: dict,
        key_properties: Optional[List[str]],
        max_level: int,
        separator: str = "__",
    ) -> None:
        """Initialize the Flattener.

        Args:
            stream_alias: [description]
            raw_schema: [description]
            key_properties: [description]
            max_level: [description]
            separator: [description]. Defaults to "__".
        """
        super().__init__(stream_alias, raw_schema, key_properties)

        self.separator = separator
        self.max_level = max_level
        self.transformed_schema = self.flatten_schema(raw_schema)

    def flatten_schema(self, raw_schema: dict) -> dict:
        """Flatten the provided schema.

        Args:
            raw_schema: The raw schema to flatten.

        Returns:
            The flattened version of the schema.
        """
        return flatten_schema(
            raw_schema, separator=self.separator, max_level=self.max_level
        )

    def transform(self, record: dict) -> dict:
        """Return a transformed record.

        Args:
            record: An individual record dictionary in a stream.

        Returns:
            The transformed record.
        """
        return flatten_record(
            record,
            self.transformed_schema,
            max_level=self.max_level,
            separator=self.separator,
        )

    def get_filter_result(self, record: dict) -> bool:
        """No records are excluded by flatteners.

        Args:
            record: The record dict.

        Returns:
            True to include the record. False to filter it out.
        """
        return True
