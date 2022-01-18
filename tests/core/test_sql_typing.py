"""Typing tests."""

import pytest
import sqlalchemy

from singer_sdk import typing as th


@pytest.mark.parametrize(
    "jsonschema_type,is_of_sql_type",
    [
        (th.StringType().to_dict(), sqlalchemy.types.VARCHAR),
        (th.IntegerType().to_dict(), sqlalchemy.types.INTEGER),
        (th.BooleanType().to_dict(), sqlalchemy.types.BOOLEAN),
        (th.NumberType().to_dict(), sqlalchemy.types.DECIMAL),
        (th.ObjectType().to_dict(), sqlalchemy.types.VARCHAR),
        (th.DateTimeType().to_dict(), sqlalchemy.types.DATETIME),
        (th.DateType().to_dict(), sqlalchemy.types.DATE),
        # Unhandled types end up as 'varchar':
        (
            th.CustomType({"type": "array", "items": "something"}).to_dict(),
            sqlalchemy.types.VARCHAR,
        ),
        (
            th.CustomType({"cannot": "compute"}).to_dict(),
            sqlalchemy.types.VARCHAR,
        ),
    ],
)
def test_convert_jsonschema_type_to_sql_type(
    jsonschema_type: dict, is_of_sql_type: sqlalchemy.types.TypeEngine
):
    result = th.to_sql_type(jsonschema_type)
    assert type(result) is is_of_sql_type


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
    sql_type: sqlalchemy.types.TypeEngine, is_of_jsonschema_type: dict
):
    result = th.to_jsonschema_type(sql_type)
    assert result == is_of_jsonschema_type
