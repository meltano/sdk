# Future Deprecations

The following features and APIs are deprecated and will be removed in a future release. Please update your code to use the recommended alternatives.

## Passing a schema of type string or `pathlib.Path` to the `Stream` constructor

The `schema` argument of the `Stream`, `RESTStream` and `GraphQLStream` constructors will not accept a `pathlib.Path` object in a future release. If you want to use a JSON schema file that is distributed with your tap, you can use `singer_sdk.helpers.util.get_package_files` to load it:

```python
from singer_sdk.helpers.util import get_package_files
from tap_countries import schemas

SCHEMAS_DIR = get_package_files(schemas)

class MyTap(Tap):
    def discover_streams(self) -> List[Stream]:
        return [
            MyStream(
                self,
                name=name,
                schema=SCHEMAS_DIR.joinpath(f"{name}.json"),
            )
            for name in ["users", "orders"]
        ]
```


```{tip}
To convert your `schemas/` directory to an importable Python package as shown above, simply add an empty `__init__.py` file to it.
```

The [`Stream.schema_filepath`](singer_sdk.Stream.schema_filepath) property will continue to work with `pathlib.Path` objects, but it's recommended to use `singer_sdk.helpers.util.get_package_files` as well.
