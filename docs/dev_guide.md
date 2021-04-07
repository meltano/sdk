# Singer SDK Development Docs

## SDK Overview

Create taps with `singer-sdk` requires overriding just two or three classes:

1. The `Tap` class. This class governs configuration, validation,
   and stream discovery.
2. The stream class. You have different options for your base class depending on the type
   of data source you are working with:
    - `Stream` - The **generic** base class for streams.
    - `RESTStream` - The base class for **REST**-type streams.
    - `GraphQLStream` - The base class for **GraphQL**-type streams. This class inherits
      from `RESTStream`, since GraphQL is built upon REST.
3. An optional authenticator class. You can omit this class entirely if you do not require authentication or if you prefer to write custom authentication logic. The supported authenticator classes are:
    - `SimpleAuthenticator` - This class is functionally equivalent to overriding
      `http_headers` property in the stream class.
    - `OAuthAuthenticator` - This class performs an OAuth 2.0 authentication flow.
    - `OAuthJWTAuthenticator` - This class performs an JWT (Java Web Token) authentication
       flow.

## Building a New Tap

The best way to get started is by building a new project from the
[cookiecutter tap template](../cookiecutter/tap-template).

## Adding Dynamic Schema Discovery

Many class properties required by the SDK can be overriden dynamically as well as statically. 
For instance, `Stream.schema`, `Stream.primary_keys`, `Stream.replication_key` can all be
declared either _statically_ if known ahead of time or _dynamically_ (if they will vary from
one environment to another).

For instance, here's a simple static Stream schema definition from the 
[cookiecutter template](../cookiecutter/tap-template/):

```python
class UsersStream({{ cookiecutter.source_name }}Stream):
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        # ...
        ),
    ).to_dict()
    primary_keys = ["id"]
    replication_key = None
```

And here is that same example converted to dynamic syntax:

```python
class UsersStream({{ cookiecutter.source_name }}Stream):
    @property
    def schema(self):
        return PropertiesList(
            Property("name", StringType),
            Property("id", StringType),
            # ...
            ),
        ).to_dict()
    
    @property
    def primary_keys(self):
        return ["id"]
    
    @property
    def replication_key(self):
        return None
```

Note that while both examples are functionally identical, the first static example is more concise
while the second example is more extensible. Use the static syntax whenever you are dealing with
stream properties that won't change and use the synamic syntax whenever you need to calculate 
the stream properties or discover them dynamically.

## Detailed Class Reference

For a detailed reference, please see the [SDK Reference Guide](./reference.md)

## CLI Samples

For a list of sample CLI commands you can run, [click here](./cli_commands.md).

## Singer SDK Implementation Details

For more detailed information about the Singer SDK implementation, please see the 
[Singer SDK Implementation Details](./implementation/README.md) section.
