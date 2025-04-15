# Building SQL taps

```{warning}
Starting with version `0.43.0`, SQL taps require SQLAlchemy 2.0 or newer.
```

## Mapping SQL types to JSON Schema

Starting with version `0.41.0`, the Meltano Singer SDK provides a clean way to map SQL types to JSON Schema. This is useful when the SQL dialect you are using has custom types that need to be mapped accordingly to JSON Schema.

### Default type mapping

The Singer SDK automatically handles the most common SQLAlchemy column types, using [`functools.singledispatchmethod`](inv:python:py:class:#functools.singledispatchmethod) to process each type. See the [`SQLToJSONSchema`](connectors.sql.SQLToJSONSchema) reference documentation for details.

### Custom type mapping

If the class above doesn't cover all the types supported by the SQLAlchemy dialect in your tap, you can subclass it and override or extend with a new method for the type you need to support:

```python
import functools

from sqlalchemy import Numeric
from singer_sdk import typing as th
from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import SQLToJSONSchema

from my_sqlalchemy_dialect import VectorType


class CustomSQLToJSONSchema(SQLToJSONSchema):
    @functools.singledispatchmethod
    def to_jsonschema(self, column_type):
        return super().to_jsonschema(column_type)

    @to_jsonschema.register
    def custom_number_to_jsonschema(self, column_type: Numeric):
        """Override the default mapping for NUMERIC columns.

        For example, a scale of 4 translates to a multipleOf 0.0001.
        """
        return {"type": ["number"], "multipleOf": 10**-column_type.scale}

    @to_jsonschema.register(VectorType)
    def vector_to_json_schema(self, column_type):
        """Custom vector to JSON schema."""
        return th.ArrayType(th.NumberType()).to_dict()
```

````{tip}
You can also use a type annotation to specify the type of the column when registering a new method:

```python
@to_jsonschema.register
def vector_to_json_schema(self, column_type: VectorType):
    return th.ArrayType(th.NumberType()).to_dict()
```
````

Then, you need to use your custom type mapping in your connector:

```python
class MyConnector(SQLConnector):
    sql_to_jsonschema_converter = CustomSQLToJSONSchema
```

### Adapting the type mapping based on user configuration


If your type mapping depends on some user-defined configuration, you can also override the `from_config` method to pass the configuration to your custom type mapping:

```python
class ConfiguredSQLToJSONSchema(SQLToJSONSchema):
    def __init__(self, *, my_custom_setting: str, **kwargs):
        super().__init__(**kwargs)
        self.my_custom_setting = my_custom_setting

    @classmethod
    def from_config(cls, config: dict):
        return cls(my_custom_setting=config.get("my_custom_setting", "default_value"))
```

Then, you can use your custom type mapping in your connector as in the previous example.


### SQL tap support for Singer Decimal string format

Starting from version `0.45.0`, the Meltano Singer SDK supports the `x-singer.decimal` format for strings. You can configure the tap to use it with the `use_singer_decimal` setting. SQL Targets that support the `x-singer.decimal` format will create an appropriate numeric column in the target database.

Read more about target support for `x-singer.decimal` in the [SQL target guide](./sql-target.md#sql-target-support-for-singer-decimal-string-format).
