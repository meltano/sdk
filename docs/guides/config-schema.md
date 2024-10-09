# Defining a configuration schema

The Singer SDK provides a way to define a configuration schema for your tap or target. This schema is used to validate the configuration provided by the user and to provide a default configuration when the user does not provide one.

The configuration schema is defined as a JSON object that describes the configuration options that your tap or target supports. We recommend using the [JSON Schema helpers](../typing.rst) provided by the SDK to define the schema.

Here is an example of a configuration schema for a tap:

```python
from singer_sdk import Tap
from singer_sdk import typing as th


class MyTap(Tap):
    name = "my-tap"

    config_jsonschema = th.PropertiesList(
        th.Property("api_key", th.StringType, required=True, title="API Key"),
        th.Property("base_url", th.StringType, default="https://api.example.com", title="Base URL"),
        th.Property("start_date", th.DateTimeType, title="Start Date"),
    ).to_dict()
```

Explanation of the configuration schema defined above:

- The `config_jsonschema` attribute is a JSON object that describes the configuration options that the tap supports.
- The `th.PropertiesList` helper is used to define a list of properties.
- The `th.Property` helper is used to define a property with a name, type, and other attributes.
- The `th.StringType`, `th.DateTimeType`, etc. helpers are used to define the type of the property.
- The `required` attribute is used to mark a property as required. The tap will throw an error if the user does not provide a value for a required property.
- The `default` attribute is used to provide a default value for a property. The tap will use this if the user does not provide a value, so this can be accessed in the tap or streams with square bracket syntax, i.e. `self.config["base_url"]`.
- The `title` attribute is used to provide a human-readable title for the property.
- The `to_dict()` method is used to convert the JSON object to a Python dictionary.

See the full reference for the [typing module](../typing.rst) for more information on how to define a configuration schema.
