# SDK Code Samples

Below you will find a collection of code samples which can be used for inspiration.

## Project Samples

The following are full project samples, contributed by members of the community:

- A REST Stream: [MeltanoLabs/tap-pulumi-cloud](https://github.com/MeltanoLabs/tap-pulumi-cloud)
- A SQL Target: [MeltanoLabs/target-postgres](https://github.com/MeltanoLabs/target-postgres)
- A SQL Tap: [MeltanoLabs/tap-postgres](https://github.com/MeltanoLabs/tap-postgres)
- A Cloud Service: [MeltanoLabs/tap-cloudwatch](https://github.com/MeltanoLabs/tap-cloudwatch)
- A REST Stream with complex and varied auth options: [MeltanoLabs/tap-github](https://github.com/MeltanoLabs/tap-github)

There are many more examples available: go to [Meltano Hub](https://hub.meltano.com) and type `sdk` in the searchbar to see a list of taps and targets created with the Singer SDK.

To add your project to Meltano Hub, please
[submit an issue](https://github.com/meltano/hub/issues/new?assignees=edgarrmondragon%2Cpnadolny13&labels=valuestream%2FHub&projects=&template=new_plugin.yml&title=Add+Plugin%3A+%3Cinsert+plugin+name%3E).

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
    config_jsonschema = th.PropertiesList([]).to_dict()

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

class ParquetStream(Stream):

    @property
    def schema(self):
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: List[th.Property] = []
        for header in FAKECSV.split("\n")[0].split(","):
            # Assume string type for all fields
            properties.append(th.Property(header, th.StringType()))
        return th.PropertiesList(*properties).to_dict()
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
        properties: List[th.Property] = []
        # Get a schema object using the parquet and pyarrow libraries
        parquet_schema = pq.ParquetFile(self.filepath).schema_arrow

        # Loop through each column in the schema object
        for i in range(len(parquet_schema.names)):
            # Get the column name
            name = parquet_schema.names[i]
            # Translate from the Parquet type to a JSON Schema type
            dtype = get_jsonschema_type(str(parquet_schema.types[i]))

            # Add the new property to our list
            properties.append(th.Property(name, dtype))

        # Return the list as a JSON Schema dictionary object
        return th.PropertiesList(*properties).to_dict()
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
from functools import cached_property

from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import RESTStream

class CachedAuthStream(RESTStream):
    """A stream with singleton authenticator."""

    @cached_property
    def authenticator(self) -> APIAuthenticatorBase:
        """Stream authenticator."""
        return APIAuthenticatorBase(stream=self)
```

### Use one of `requests`'s built-in authenticators

```python
from requests.auth import HTTPDigestAuth
from singer_sdk.streams import RESTStream

class DigestAuthStream(RESTStream):
    """A stream with digest authentication."""

    @property
    def authenticator(self) -> HTTPDigestAuth:
        """Stream authenticator."""
        return HTTPDigestAuth(
            username=self.config["username"],
            password=self.config["password"],
        )
```

[`HTTPBasicAuth`](https://requests.readthedocs.io/en/latest/api/#requests.auth.HTTPBasicAuth) and
[`HTTPProxyAuth`](https://requests.readthedocs.io/en/latest/api/#requests.auth.HTTPProxyAuth)
are also available in `requests.auth`. In addition to `requests.auth` classes, the community
has published a few packages with custom authenticator classes, which are compatible with the SDK.
For example:

- [`requests-aws4auth`](https://github.com/tedder/requests-aws4auth): AWS v4 authentication
- [`requests_auth`](https://github.com/Colin-b/requests_auth): A collection of authenticators
  for various services and protocols including Azure, Okta and NTLM.

### Custom response validation

Some APIs deviate from HTTP status codes to report failures. For those cases,
you can override [`RESTStream.validate_response()`](singer_sdk.RESTStream.validate_response)
and raise [`FatalAPIError`](FatalAPIError)
if an unrecoverable error is detected. If the API also has transient errors, either client-side
like rate limits, or server-side like temporary 5xx, you can raise
[`RetriableAPIError`](RetriableAPIError)
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

- [`backoff_wait_generator`](singer_sdk.RESTStream.backoff_wait_generator)
- [`backoff_max_tries`](singer_sdk.RESTStream.backoff_max_tries)
- [`backoff_handler`](singer_sdk.RESTStream.backoff_handler)
- [`backoff_jitter`](singer_sdk.RESTStream.backoff_jitter)

For example, to use a constant retry:
```
def backoff_wait_generator() -> Callable[..., Generator[int, Any, None]]:
    return backoff.constant(interval=10)
```

To utilise a response header as a wait value you can use [`backoff_runtime`](singer_sdk.RESTStream.backoff_runtime), and pass a method that returns a wait value:

**Note**: By default jitter makes this function wait a bit longer than the value provided.
To disable jitter override [`backoff_jitter`](singer_sdk.RESTStream.backoff_jitter).
In sdk versions <=0.21.0 the default jitter function will make the function below not work as you would expect without disabling jitter,
([here](https://github.com/meltano/sdk/issues/1477) for more information) to disable jitter override the `request_decorator` and pass `jitter=None` to the `backoff.on_exception` function.

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
