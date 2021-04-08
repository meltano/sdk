# Code Samples

Below you will find a collection of code samples which can be used for inspiration.


## Project Samples

Below are full project samples, contributed by members in the community. Use these for inspiration
or to get more information on what an SDK-based tap will look like.

- [tap-bamboohr by Auto IDM](https://gitlab.com/autoidm/tap-bamboohr)
- [tap-confluence by @edgarrmondragon](https://github.com/edgarrmondragon/tap-confluence)
- [tap-investing by @DouweM](https://gitlab.com/DouweM/tap-investing)
- [tap-parquet by AJ](https://github.com/dataops-tk/tap-parquet)
- [tap-powerbi-metadata by Slalom](https://github.com/dataops-tk/tap-powerbi-metadata)

To add your project to this list, please 
[submit an issue](https://gitlab.com/meltano/meltano/-/issues/new?issue%5Bassignee_id%5D=&issue%5Bmilestone_id%5D=).

## Reusable Code Snippets

These are code samples taken from other projects. Use these as a reference if you get stuck.

### A simple Tap class definition with two streams

```python
class TapCountries(Tap):
    """Sample tap for Countries GraphQL API. This tap has no
    config options and does not require authentication.
    """
    name = "tap-countries"
    config_jsonschema = PropertiesList([]).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list containing the two stream types."""
        return [
            CountriesStream(tap=self),
            ContinentsStream(tap=self),
        ]
```

### Define a simple GraphQL-based stream with schema defined in a file 

```python
class ContinentsStream(GraphQLStream):
    """Continents stream from the Countries API."""

    name = "continents"
    primary_keys = ["code"]
    replication_key = None  # Incremental bookmarks not needed

    # Read JSON Schema definition from a text file:
    schema_filepath = SCHEMAS_DIR / "continents.json"

    # GraphQL API endpoint and query text:
    url_base = "https://countries.trevorblades.com/"
    query = """
        continents {
            code
            name
        }
        """
```

### Dynamically discovering `schema` for a stream

```python
class ParquetStream(Stream):
    """Stream class for Parquet streams."""

    #...

    @property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: List[Property] = []
        # Get a schema object using the parquet and pyarrow libraries
        parquet_schema = pq.ParquetFile(self.filepath).schema_arrow

        # Loop through each column in the schema object
        for i in range(len(parquet_schema.names)):
            # Get the column name
            name = parquet_schema.names[i]
            # Translate from the Parquet type to a JSON Schema type
            dtype = get_jsonschema_type(str(parquet_schema.types[i]))

            # Add the new property to our list
            properties.append(Property(name, dtype))

        # Return the list as a JSON Schema dictionay object
        return PropertiesList(*properties).to_dict()
```

### Initialize a collection of tap streams with differing types

```python
class TapCountries(Tap):
    # ...
    def discover_streams(self) -> List[Stream]:
        """Return a list containing one each of the two stream types."""
        return [
            CountriesStream(tap=self),
            ContinentsStream(tap=self),
        ]
```

Or equivalently:

```python

# Declare list of types here at the top of the file
STREAM_TYPES = [
    CountriesStream,
    ContinentsStream,
]

class TapCountries(Tap):
    # ...
    def discover_streams(self) -> List[Stream]:
        """Return a list with one each of all defined stream types."""
        return [
            stream_type(tap=self)
            for stream_type in STREAM_TYPES
        ]
```

### Run the standard built-in tap tests

```python
# Import the tests
from singer_sdk.testing import get_standard_tap_tests

# Import our tap class
from tap_parquet.tap import TapParquet

SAMPLE_CONFIG = {
    # ...
}

def test_sdk_standard_tap_tests():
    """Run the built-in tap tests from the SDK."""
    tests = get_standard_tap_tests(TapParquet, config=SAMPLE_CONFIG)
    for test in tests:
        test()
```
