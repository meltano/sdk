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


def custom_array_to_sql(jsonschema: dict) -> VectorType | sa.types.VARCHAR:
    """Custom mapping for arrays of numbers."""
    if items := jsonschema.get("items"):
        if items.get("type") == "number":
            return VectorType()

    return sa.types.VARCHAR()


class MyConnector(SQLConnector):
    @functools.cached_property
    def jsonschema_to_sql(self):
        to_sql = JSONSchemaToSQL.from_config(
            self.config,
            max_varchar_length=self.max_varchar_length,
        )
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
        to_sql = JSONSchemaToSQL.from_config(
            self.config,
            max_varchar_length=self.max_varchar_length,
        )
        to_sql.register_format_handler("uri", URI)
        return to_sql
```

### Use the `x-sql-datatype` JSON Schema extension

You can register new type handlers for the `x-sql-datatype` extension:

```python
from my_sqlalchemy_dialect import URI


class MyConnector(SQLConnector):
    @functools.cached_property
    def jsonschema_to_sql(self):
        to_sql = JSONSchemaToSQL()
        to_sql.register_sql_datatype_handler("smallint", sa.types.SMALLINT)
        return to_sql
```

Then you can annotate the tap' catalog to specify the SQL type:

````{tab} meltano.yml
```yaml
# https://docs.meltano.com/concepts/plugins/#schema-extra
plugins:
  extractors:
  - name: tap-example
    schema:
      addresses:
        number:
          x-sql-datatype: smallint
```
````

````{tab} JSON catalog
```json
{
  "streams": [
    {
      "stream": "addresses",
      "tap_stream_id": "addresses",
      "schema": {
        "type": "object",
        "properties": {
          "number": {
            "type": "integer",
            "x-sql-datatype": "smallint"
          }
        }
      }
    }
  ]
}
```
````

### SQL target support for Singer Decimal string format

Starting from version `0.45.0`, the Meltano Singer SDK supports the `x-singer.decimal` format for strings. If the source tap is configured to use this format, the SDK will automatically convert the string to a `DECIMAL` type in the target database.

Read more about target support for `x-singer.decimal` in the [SQL tap guide](./sql-tap.md#sql-tap-support-for-singer-decimal-string-format).
