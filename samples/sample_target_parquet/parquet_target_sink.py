"""Sample Parquet target stream class, which handles writing streams."""

from __future__ import annotations

import typing as t

from singer_sdk.sinks import BatchSink

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pass


def json_schema_to_arrow(schema: dict[str, t.Any]) -> pa.Schema:
    """Convert a JSON Schema to an Arrow schema.

    Args:
        schema: The JSON Schema definition.

    Returns:
        An Arrow schema.
    """
    fields = _json_schema_to_arrow_fields(schema)
    return pa.schema(fields)


def _json_schema_to_arrow_fields(schema: dict[str, t.Any]) -> pa.StructType:
    """Convert a JSON Schema to an Arrow struct.

    Args:
        schema: The JSON Schema definition.

    Returns:
        An Arrow struct.
    """
    fields = []
    for name, property_schema in schema.get("properties", {}).items():
        field = pa.field(name, _json_type_to_arrow_field(property_schema))
        fields.append(field)

    return fields if fields else [pa.field("dummy", pa.string())]


def _json_type_to_arrow_field(  # noqa: PLR0911
    schema_type: dict[str, t.Any],
) -> pa.DataType:
    """Convert a JSON Schema to an Arrow struct.

    Args:
        schema: The JSON Schema definition.

    Returns:
        An Arrow struct.
    """
    property_type = schema_type.get("type")

    if isinstance(property_type, list):
        try:
            main_type = property_type[0]
        except IndexError:
            main_type = "null"
    else:
        main_type = property_type

    if main_type == "array":
        items = schema_type.get("items", {})
        return pa.list_(_json_type_to_arrow_field(items))

    if main_type == "object":
        return pa.struct(_json_schema_to_arrow_fields(schema_type))

    if main_type == "string":
        return pa.string()

    if main_type == "integer":
        return pa.int64()

    if main_type == "number":
        return pa.float64()

    if main_type == "boolean":
        return pa.bool_()

    if main_type == "null":
        return pa.null()

    return pa.null()


class SampleParquetTargetSink(BatchSink):
    """Parquery target sample class."""

    max_size = 100000  # Max records to write in any batch

    def process_batch(self, context: dict) -> None:
        """Write any prepped records out and return only once fully written."""
        records_to_drain = context["records"]
        schema = json_schema_to_arrow(self.schema)
        writer = pq.ParquetWriter(self.config["filepath"], schema)

        table = pa.Table.from_pylist(records_to_drain, schema=schema)
        writer.write_table(table)
        writer.close()
