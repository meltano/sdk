"""Typing tests."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from singer_sdk import typing as th


@pytest.mark.parametrize(
    "jsonschema_type,sql_type",
    [
        (th.StringType().to_dict(), sa.types.VARCHAR()),
        (th.IntegerType().to_dict(), sa.types.INTEGER()),
        (th.BooleanType().to_dict(), sa.types.BOOLEAN()),
        (th.NumberType().to_dict(), sa.types.DECIMAL()),
        (th.ObjectType().to_dict(), sa.types.VARCHAR()),
        (th.DateTimeType().to_dict(), sa.types.DATETIME()),
        (th.DateType().to_dict(), sa.types.DATE()),
        # Unhandled types end up as 'varchar':
        (
            th.CustomType({"type": "array", "items": "something"}).to_dict(),
            sa.types.VARCHAR(),
        ),
        (
            th.CustomType({"cannot": "compute"}).to_dict(),
            sa.types.VARCHAR(),
        ),
        (
            th.CustomType({"type": "string", "maxLength": 10}).to_dict(),
            sa.types.VARCHAR(10),
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
    sql_type: sa.types.TypeEngine,
):
    with pytest.warns(DeprecationWarning, match="Use `JSONSchemaToSQL` instead"):
        result = th.to_sql_type(jsonschema_type)
    assert isinstance(result, sql_type.__class__)
    assert str(result) == str(sql_type)


@pytest.mark.parametrize(
    "sql_type,is_of_jsonschema_type",
    [
        (sa.types.VARCHAR, th.StringType().to_dict()),
        (sa.types.INTEGER, th.IntegerType().to_dict()),
        (sa.types.BOOLEAN, th.BooleanType().to_dict()),
        (sa.types.DATETIME, th.DateTimeType().to_dict()),
        (sa.types.DATE, th.DateType().to_dict()),
        # Unhandled types end up as 'string':
        (sa.types.CLOB, th.StringType().to_dict()),
    ],
)
def test_convert_sql_type_to_jsonschema_type(
    sql_type: sa.types.TypeEngine,
    is_of_jsonschema_type: dict,
):
    with pytest.warns(DeprecationWarning, match="Use `SQLToJSONSchema` instead"):
        result = th.to_jsonschema_type(sql_type)

    assert result == is_of_jsonschema_type
