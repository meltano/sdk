"""Typing tests."""

from __future__ import annotations

import pytest
import sqlalchemy.types
from sqlalchemy import types

from singer_sdk import typing as th


@pytest.mark.parametrize(
    "jsonschema_type,sql_type",
    [
        (th.StringType().to_dict(), sqlalchemy.types.VARCHAR()),
        (th.IntegerType().to_dict(), sqlalchemy.types.INTEGER()),
        (th.BooleanType().to_dict(), sqlalchemy.types.BOOLEAN()),
        (th.DecimalType().to_dict(), sqlalchemy.types.DECIMAL()),
        (th.ObjectType().to_dict(), sqlalchemy.types.VARCHAR()),
        (th.DateTimeType().to_dict(), sqlalchemy.types.DATETIME()),
        (th.DateType().to_dict(), sqlalchemy.types.DATE()),
        # Unhandled types end up as 'varchar':
        (
            th.CustomType({"type": "array", "items": "something"}).to_dict(),
            sqlalchemy.types.VARCHAR(),
        ),
        (
            th.CustomType({"cannot": "compute"}).to_dict(),
            sqlalchemy.types.VARCHAR(),
        ),
        (
            th.CustomType({"type": "string", "maxLength": 10}).to_dict(),
            sqlalchemy.types.VARCHAR(10),
        ),
    ],
    ids=[
        "string -> varchar",
        "integer -> integer",
        "boolean -> boolean",
        "number -> decimal",
        "object -> varchar",
        "datetime -> datetime",
        "date -> date",
        "unknown -> varchar",
        "array -> varchar",
        "string(maxLength: 10) -> varchar(10)",
    ],
)
def test_convert_jsonschema_type_to_sql_type(
    jsonschema_type: dict,
    sql_type: sqlalchemy.types.TypeEngine,
):
    with pytest.warns(DeprecationWarning, match="Use `JSONSchemaToSQL` instead"):
        result = th.to_sql_type(jsonschema_type)
    assert isinstance(result, sql_type.__class__)
    assert str(result) == str(sql_type)


@pytest.mark.parametrize(
    "sql_type,is_of_jsonschema_type",
    [
        (sqlalchemy.types.VARCHAR, th.StringType().to_dict()),
        (sqlalchemy.types.INTEGER, th.IntegerType().to_dict()),
        (sqlalchemy.types.BOOLEAN, th.BooleanType().to_dict()),
        (sqlalchemy.types.DATETIME, th.DateTimeType().to_dict()),
        (sqlalchemy.types.DATE, th.DateType().to_dict()),
        # Unhandled types end up as 'string':
        (sqlalchemy.types.CLOB, th.StringType().to_dict()),
    ],
)
def test_convert_sql_type_to_jsonschema_type(
    sql_type: sqlalchemy.types.TypeEngine,
    is_of_jsonschema_type: dict,
):
    with pytest.warns(DeprecationWarning, match="Use `SQLToJSONSchema` instead"):
        result = th.to_jsonschema_type(sql_type)

    assert result == is_of_jsonschema_type


@pytest.mark.filterwarnings("ignore:Use `JSONSchemaToSQL` instead.:DeprecationWarning")
@pytest.mark.parametrize(
    "jsonschema_type,expected",
    [
        ({"type": ["string", "null"]}, types.VARCHAR),
        ({"type": ["integer", "null"]}, types.INTEGER),
        ({"type": ["number", "null"]}, types.DECIMAL),
        ({"type": ["boolean", "null"]}, types.BOOLEAN),
        ({"type": "object", "properties": {}}, types.VARCHAR),
        ({"type": "array"}, types.VARCHAR),
        ({"format": "date", "type": ["string", "null"]}, types.DATE),
        ({"format": "time", "type": ["string", "null"]}, types.TIME),
        ({"format": "date-time", "type": ["string", "null"]}, types.DATETIME),
        (
            {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}]},
            types.DATETIME,
        ),
        ({"anyOf": [{"type": "integer"}, {"type": "null"}]}, types.INTEGER),
        ({"type": ["array", "object", "boolean", "null"]}, types.VARCHAR),
    ],
)
def test_to_sql_type(jsonschema_type, expected):
    assert isinstance(th.to_sql_type(jsonschema_type), expected)
