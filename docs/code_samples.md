# Code Samples

Below you will find a collection of code samples which can be used for inspiration.

## Project Samples

Below are full project samples, contributed by members in the community. Use these for inspiration
or to get more information on what an SDK-based tap or target will look like.

- [tap-bamboohr by Auto IDM](https://gitlab.com/autoidm/tap-bamboohr)
- [tap-confluence by @edgarrmondragon](https://github.com/edgarrmondragon/tap-confluence)
- [tap-investing by @DouweM](https://gitlab.com/DouweM/tap-investing)
- [tap-parquet by AJ](https://github.com/dataops-tk/tap-parquet)
- [tap-powerbi-metadata by Slalom](https://github.com/dataops-tk/tap-powerbi-metadata)
- [target-athena, Community Project led by Andrew Stewart](https://github.com/dataops-tk/target-athena)

To add your project to this list, please
[submit an issue](https://gitlab.com/meltano/meltano/-/issues/new?issue%5Bassignee_id%5D=&issue%5Bmilestone_id%5D=).

## Reusable Code Snippets

These are code samples taken from other projects. Use these as a reference if you get stuck.

- [A simple Tap class definition with two streams](./code_samples.md#a-simple-tap-class-definition-with-two-streams)
- [Define a simple GraphQL-based stream with schema defined in a file](./code_samples.md#define-a-simple-graphql-based-stream-with-schema-defined-in-a-file)
- [Define a REST-based stream with a JSONPath expression](./code_samples.md#define-a-rest-based-stream-with-a-jsonpath-expression)
- [Use a JSONPath expression to extract the next page URL from a HATEOAS response](./code_samples.md#use-a-jsonpath-expression-to-extract-the-next-page-url-from-a-hateoas-response)
- [Dynamically discovering `schema` for a stream](./code_samples.md#dynamically-discovering-schema-for-a-stream)
- [Initialize a collection of tap streams with differing types](./code_samples.md#initialize-a-collection-of-tap-streams-with-differing-types)
- [Run the standard built-in tap tests](./code_samples.md#run-the-standard-built-in-tap-tests)
- [Make all streams reuse the same authenticator instance](./code_samples.md#make-all-streams-reuse-the-same-authenticator-instance)
- [Make a stream reuse the same authenticator instance for all requests](./code_samples.md#make-a-stream-reuse-the-same-authenticator-instance-for-all-requests)
- [Custom response validation](./code_samples.md#custom-response-validation)
- [Custom Backoff](./code_samples.md#custom-backoff)

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

### Define a REST-based stream with a JSONPath expression

```python
class LOTRCharactersStream(RESTStream):
    """Characters stream from the Lord of the Rings 'The One' API."""

    # Base REST API configuration
    url_base = "https://the-one-api.dev/v2"
    primary_keys = ["_id"]

    # Endpoint configuration
    path = "/character"
    name = "characters"
    records_jsonpath = "$.docs[*]"

    @property
    def authenticator(self):
        return SimpleAuthenticator(
            stream=self,
            auth_headers={
                "Authorization": f"Bearer {self.config.get('api_key')}",
            },
        )
```

### Use a JSONPath expression to extract the next page URL from a [HATEOAS](https://en.wikipedia.org/wiki/HATEOAS) response

```python
class MyStream(RESTStream):
    """A custom stream."""

    # Gets the href property from the links item where rel="next"
    next_page_token_jsonpath = "$.links[?(@.rel=='next')].href"
```

### Dynamically discovering `schema` for a stream

Here is an example which parses schema from a CSV file:

```python
FAKECSV = """
Header1,Header2,Header3
val1,val2,val3
val1,val2,val3
val1,val2,val3
"""

@property
class ParquetStream(Stream):
    def schema(self):
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: List[Property] = []
        for header in FAKECSV.split("\n")[0].split(",")
            # Assume string type for all fields
            properties.add(header, StringType())
        return PropertiesList(*properties).to_dict()
```

Here is another example from the Parquet tap. This sample uses a
custom `get_jsonschema_type()` function to return the data type.

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

        # Return the list as a JSON Schema dictionary object
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

### Make all streams reuse the same authenticator instance

```python
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.streams import RESTStream

class SingletonAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """A singleton authenticator."""

class SingletonAuthStream(RESTStream):
    """A stream with singleton authenticator."""

    @property
    def authenticator(self) -> SingletonAuthenticator:
        """Stream authenticator."""
        return SingletonAuthenticator(stream=self)
```

### Make a stream reuse the same authenticator instance for all requests

```python
from memoization import cached

from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import RESTStream

class CachedAuthStream(RESTStream):
    """A stream with singleton authenticator."""

    @property
    @cached
    def authenticator(self) -> APIAuthenticatorBase:
        """Stream authenticator."""
        return APIAuthenticatorBase(stream=self)
```

### Custom response validation

Some APIs deviate from HTTP status codes to report failures. For those cases,
you can override [`RESTStream.validate_response()`](./classes/singer_sdk.RESTStream.html#singer_sdk.RESTStream.validate_response)
and raise [`FatalAPIError`](./classes/singer_sdk.exceptions.FatalAPIError)
if an unrecoverable error is detected. If the API also has transient errors, either client-side
like rate limits, or server-side like temporary 5xx, you can raise
[`RetriableAPIError`](./classes/singer_sdk.exceptions.RetriableAPIError)
and the request will be retried with back-off:

```python
from enum import Enum
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams.rest import RESTStream

class CustomResponseValidationStream(RESTStream):
    """Stream with non-conventional error response."""

    url_base = "https://badapi.test"
    name = "non_conventional"
    schema = {"type": "object", "properties": {}}
    path = "/dummy

    class StatusMessage(str, Enum):
        """Possible status messages."""

        OK = "OK"
        ERROR = "ERROR"
        UNAVAILABLE = "UNAVAILABLE"

    def validate_response(self, response):
        # Still catch error status codes
        super().validate_response(response)

        data = response.json()
        if data["status"] == self.StatusMessage.ERROR:
            raise FatalAPIError("Error message found :(")
        if data["status"] == self.StatusMessage.UNAVAILABLE:
            raise RetriableAPIError("API is unavailable")
```

### Custom Backoff
Custom backoff and retry behaviour can be added by overriding the methods:
[`backoff_wait_generator`](./classes/singer_sdk.RESTStream.html#singer_sdk.RESTStream.backoff_wait_generator)
[`backoff_max_tries`](./classes/singer_sdk.RESTStream.html#singer_sdk.RESTStream.backoff_max_tries)
[`backoff_handler`](./classes/singer_sdk.RESTStream.html#singer_sdk.RESTStream.backoff_handler)

For example, to use a constant retry:
```
def backoff_wait_generator() -> Callable[..., Generator[int, Any, None]]:
    return backoff.constant(interval=10)
```

To utilise a response header as a wait value you can use [`backoff_runtime`](./classes/singer_sdk.RESTStream.html#singer_sdk.RESTStream.backoff_runtime), and pass a method that returns a wait value:
```
def backoff_wait_generator() -> Callable[..., Generator[int, Any, None]]:
    def _backoff_from_headers(retriable_api_error):
        response_headers = retriable_api_error.response.headers
        return int(response_headers.get("Retry-After", 0))

    return self.backoff_runtime(value=_backoff_from_headers)

```


## Additional Resources

More links, resources, and example solutions are available from community
members in the [`#singer-tap-development`](https://meltano.slack.com/archives/C01PKLU5D1R)
and [`#singer-target-development`](https://meltano.slack.com/archives/C01RKUVUG4S)
channels on [Meltano Slack](https://meltano.com/slack).
