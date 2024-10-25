# Building SQL targets

## Mapping JSON Schema to SQL types

Starting with version `0.42.0`, the Meltano Singer SDK provides a clean way to map JSON Schema to SQL types. This is useful when the SQL dialect needs to do special handling for certain JSON Schema types.

### Custom JSON Schema mapping

If the default [`JSONSchemaToSQL`](connectors.sql.JSONSchemaToSQL) instance doesn't cover all the types supported by the SQLAlchemy dialect in your target, you can override the {attr}`SQLConnector.jsonschema_to_sql <singer_sdk.SQLConnector.jsonschema_to_sql>` property and register a new type handler for the type you need to support:

```python
import functools

import sqlalchemy as sa
from singer_sdk import typing as th
from singer_sdk.connectors import JSONSchemaToSQL, SQLConnector

from my_sqlalchemy_dialect import VectorType


def custom_array_to_sql(jsonschema: dict) -> VectorType | sa.types.ARRAY:
    """Custom mapping for arrays of numbers."""
    if items := jsonschema.get("items"):
        if items.get("type") == "number":
            return VectorType()

    return sa.types.VARCHAR()


class MyConnector(SQLConnector):
    @functools.cached_property
    def jsonschema_to_sql(self):
        to_sql = JSONSchemaToSQL()
        to_sql.register_type_handler("array", custom_array_to_sql)
        return to_sql
```

### Custom string format mapping

You can also register a new format handler for custom string formats:

```python
from my_sqlalchemy_dialect import URI


class MyConnector(SQLConnector):
    @functools.cached_property
    def jsonschema_to_sql(self):
        to_sql = JSONSchemaToSQL()
        to_sql.register_format_handler("uri", URI)
        return to_sql
```
