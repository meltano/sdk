# Building SQL taps

## Default type mapping

The Singer SDK automatically handles the most common SQLAlchemy column types, using [`functools.singledispatchmethod`](inv:python:py:class:#functools.singledispatchmethod) to process each type. See the [`SQLToJSONSchema`](connectors.sql.SQLToJSONSchema) reference documentation for details.

## Custom type mapping

If the class above doesn't cover all the types supported by the SQLAlchemy dialect in your tap, you can subclass it and override or extend with a new method for the type you need to support:

```python
import functools

from sqlalchemy import Numeric
from singer_sdk import typing as th
from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import SQLToJSONSchema

from my_sqlalchemy_dialect import VectorType


class CustomSQLToJSONSchema(SQLToJSONSchema):
    @SQLToJSONSchema.to_jsonschema.register
    def custom_number_to_jsonschema(self, column_type: Numeric):
        """Override the default mapping for NUMERIC columns.

        For example, a scale of 4 translates to a multipleOf 0.0001.
        """
        return {"type": ["number"], "multipleOf": 10**-column_type.scale}

    @SQLToJSONSchema.to_jsonschema.register(VectorType)
    def vector_to_json_schema(self, column_type):
        """Custom vector to JSON schema."""
        return th.ArrayType(th.NumberType()).to_dict()
```

````{tip}
You can also use a type annotation to specify the type of the column when registering a new method:

```python
@SQLToJSONSchema.to_jsonschema.register
def vector_to_json_schema(self, column_type: VectorType):
    return th.ArrayType(th.NumberType()).to_dict()
```
````

Then, you need to use your custom type mapping in your connector:

```python
class MyConnector(SQLConnector):
    @functools.cached_property
    def sql_to_jsonschema(self):
        return CustomSQLToJSONSchema()
```
